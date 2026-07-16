import json
import os
import platform
import sys

import pyarrow
from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

OUT_PATH = os.environ.get("OUT_PATH", "/out/results.json")


def rows(df):
    return [json.loads(r) for r in df.toJSON().collect()]


def seed(spark):
    spark.sql("DROP TABLE IF EXISTS orders")
    spark.sql("CREATE TABLE orders (region STRING, amount DOUBLE, customer STRING) USING parquet")
    spark.sql(
        """INSERT INTO orders VALUES
        ('us', 120.0, 'ana'), ('us', 80.0, 'bob'), ('us', 40.0, 'ana'),
        ('eu', 200.0, 'ana'), ('eu', 60.0, 'cleo'),
        ('apac', 90.0, 'dan'), ('apac', 30.0, 'cleo')"""
    )

    spark.sql("DROP TABLE IF EXISTS products")
    spark.sql("CREATE TABLE products (id INT, name STRING, embedding ARRAY<FLOAT>) USING parquet")
    spark.sql(
        """INSERT INTO products VALUES
        (1, 'running shoes',   array(0.95f, 0.10f, 0.05f)),
        (2, 'trail sneakers',  array(0.88f, 0.22f, 0.10f)),
        (3, 'espresso maker',  array(0.05f, 0.92f, 0.15f)),
        (4, 'coffee grinder',  array(0.10f, 0.85f, 0.20f)),
        (5, 'yoga mat',        array(0.60f, 0.08f, 0.70f))"""
    )

    spark.sql("DROP TABLE IF EXISTS clicks")
    spark.sql("CREATE TABLE clicks (product STRING) USING parquet")
    spark.sql(
        """INSERT INTO clicks
        SELECT 'running shoes'  FROM range(50) UNION ALL
        SELECT 'coffee grinder' FROM range(30) UNION ALL
        SELECT 'yoga mat'       FROM range(12) UNION ALL
        SELECT 'espresso maker' FROM range(5)  UNION ALL
        SELECT 'trail sneakers' FROM range(3)"""
    )


def runtime(spark):
    return {
        "sparkVersion": spark.version,
        "pythonVersion": platform.python_version(),
        "pyarrowVersion": pyarrow.__version__,
        "javaVersion": spark._jvm.System.getProperty("java.version"),
        "builtinFunctions": spark.sql("SHOW FUNCTIONS").count(),
        "arrowPythonUdfDefault": spark.conf.get("spark.sql.execution.pythonUDF.arrow.enabled"),
    }


def metric_views(spark):
    """One governed definition; the engine keeps non-additive measures correct at every grain."""
    yaml = """version: 0.1
source: orders
dimensions:
  - name: region
    expr: region
measures:
  - name: revenue
    expr: SUM(amount)
  - name: customers
    expr: COUNT(DISTINCT customer)"""
    spark.sql("CREATE OR REPLACE VIEW revenue_mv WITH METRICS LANGUAGE YAML AS $$\n" + yaml + "\n$$")

    by_region = spark.sql(
        "SELECT region, MEASURE(revenue) AS revenue, MEASURE(customers) AS customers "
        "FROM revenue_mv GROUP BY region ORDER BY revenue DESC"
    )
    governed = spark.sql("SELECT MEASURE(customers) FROM revenue_mv").collect()[0][0]
    naive = spark.sql(
        "SELECT SUM(customers) FROM (SELECT region, MEASURE(customers) AS customers "
        "FROM revenue_mv GROUP BY region)"
    ).collect()[0][0]
    return {
        "yaml": yaml,
        "byRegion": rows(by_region),
        "governedTotalCustomers": governed,
        "naiveSummedCustomers": naive,
    }


def vector_search(spark):
    """Vector distance functions plus the NEAREST BY top-K ranking join."""
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW query AS "
        "SELECT 'athletic footwear' AS q, array(0.92f, 0.15f, 0.08f) AS qv"
    )
    nearest = spark.sql(
        """SELECT p.name,
                  vector_l2_distance(q.qv, p.embedding) AS distance,
                  vector_cosine_similarity(q.qv, p.embedding) AS similarity
           FROM query q JOIN products p
           EXACT NEAREST 3 BY DISTANCE vector_l2_distance(q.qv, p.embedding)"""
    )
    norms = spark.sql(
        "SELECT name, vector_norm(embedding) AS norm, vector_normalize(embedding) AS unit "
        "FROM products ORDER BY id"
    )
    return {
        "queryText": "athletic footwear",
        "queryVector": [0.92, 0.15, 0.08],
        "nearest": rows(nearest),
        "norms": rows(norms),
    }


def sketches(spark):
    topk = spark.sql("SELECT explode(approx_top_k(product, 3)) AS t FROM clicks").selectExpr(
        "t.item AS item", "t.count AS count"
    )
    exact = spark.sql("SELECT COUNT(DISTINCT product) FROM clicks").collect()[0][0]
    theta = spark.sql(
        "SELECT theta_sketch_estimate(theta_sketch_agg(product)) FROM clicks"
    ).collect()[0][0]
    return {"topK": rows(topk), "exactDistinct": exact, "thetaSketchDistinct": theta}


def geospatial(spark):
    wkb = "X'0101000000000000000000F03F000000000000F03F'"
    df = spark.sql(
        f"""SELECT 'store-lisbon' AS site,
                   st_srid(st_setsrid(st_geomfromwkb({wkb}), 4326)) AS srid,
                   typeof(st_setsrid(st_geomfromwkb({wkb}), 4326)) AS geometryType,
                   typeof(CAST(NULL AS GEOGRAPHY(4326))) AS geographyType"""
    )
    st_fns = [r[0] for r in spark.sql("SHOW FUNCTIONS").collect() if r[0].startswith("st_")]
    return {"sample": rows(df), "availableStFunctions": st_fns}


def arrow_udf(spark):
    """Arrow-optimized Python UDFs are the default execution path in 4.2."""

    @udf(returnType=IntegerType())
    def boost(clicks):
        return int(clicks) * 3

    df = spark.sql("SELECT product AS name, COUNT(*) AS clicks FROM clicks GROUP BY product")
    boosted = df.select(col("name"), boost(col("clicks")).alias("boosted")).orderBy(
        col("boosted").desc()
    )
    collected = rows(boosted)
    return {
        "rows": len(collected),
        "arrowEnabledByDefault": spark.conf.get("spark.sql.execution.pythonUDF.arrow.enabled"),
        "sample": collected,
    }


class InventoryReader(DataSourceReader):
    def __init__(self, schema):
        self.schema = schema

    def read(self, partition):
        for i, label in enumerate(["lisbon-a1", "porto-b2", "madrid-c3", "paris-d4"]):
            yield (i + 1, label)


class InventoryDataSource(DataSource):
    """A batch reader written in pure Python, used through spark.read.format(...)."""

    @classmethod
    def name(cls):
        return "inventory"

    def schema(self):
        return StructType(
            [StructField("id", IntegerType()), StructField("label", StringType())]
        )

    def reader(self, schema):
        return InventoryReader(schema)


def data_source(spark):
    spark.dataSource.register(InventoryDataSource)
    return {"rows": rows(spark.read.format("inventory").load())}


def main():
    spark = (
        SparkSession.builder.appName("spark-4.2-python3-fun")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.warehouse.dir", os.environ.get("WAREHOUSE_DIR", "/tmp/warehouse"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        results = {}
        seed(spark)
        results["runtime"] = runtime(spark)
        results["metricView"] = metric_views(spark)
        results["vector"] = vector_search(spark)
        results["sketches"] = sketches(spark)
        results["geo"] = geospatial(spark)
        results["arrowUdf"] = arrow_udf(spark)
        results["dataSource"] = data_source(spark)
        os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
        with open(OUT_PATH, "w") as f:
            json.dump(results, f)
        print(f"wrote {OUT_PATH}", file=sys.stderr)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
