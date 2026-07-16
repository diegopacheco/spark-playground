import org.apache.spark.sql.{DataFrame, SparkSession}
import java.nio.file.{Files, Paths}

object Main:

  private val OutPath = sys.env.getOrElse("OUT_PATH", "/out/results.json")

  def main(args: Array[String]): Unit =
    val spark = SparkSession
      .builder()
      .appName("spark-4.2-scala3-fun")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.warehouse.dir", sys.env.getOrElse("WAREHOUSE_DIR", "/tmp/warehouse"))
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try
      seed(spark)
      val sections = Seq(
        "runtime"     -> runtime(spark),
        "metricView"  -> metricViews(spark),
        "vector"      -> vectorSearch(spark),
        "sketches"    -> sketches(spark),
        "geo"         -> geospatial(spark)
      )
      val json = sections.map((k, v) => "\"" + k + "\":" + v).mkString("{", ",", "}")
      val out = Paths.get(OutPath)
      Option(out.getParent).foreach(Files.createDirectories(_))
      Files.writeString(out, json)
      println(s"wrote $OutPath")
    finally spark.stop()

  private def rows(df: DataFrame): String =
    df.toJSON.collect().mkString("[", ",", "]")

  private def seed(spark: SparkSession): Unit =
    spark.sql("DROP TABLE IF EXISTS orders")
    spark.sql("CREATE TABLE orders (region STRING, amount DOUBLE, customer STRING) USING parquet")
    spark.sql("""INSERT INTO orders VALUES
      ('us', 120.0, 'ana'), ('us', 80.0, 'bob'), ('us', 40.0, 'ana'),
      ('eu', 200.0, 'ana'), ('eu', 60.0, 'cleo'),
      ('apac', 90.0, 'dan'), ('apac', 30.0, 'cleo')""")

    spark.sql("DROP TABLE IF EXISTS products")
    spark.sql("CREATE TABLE products (id INT, name STRING, embedding ARRAY<FLOAT>) USING parquet")
    spark.sql("""INSERT INTO products VALUES
      (1, 'running shoes',   array(0.95f, 0.10f, 0.05f)),
      (2, 'trail sneakers',  array(0.88f, 0.22f, 0.10f)),
      (3, 'espresso maker',  array(0.05f, 0.92f, 0.15f)),
      (4, 'coffee grinder',  array(0.10f, 0.85f, 0.20f)),
      (5, 'yoga mat',        array(0.60f, 0.08f, 0.70f))""")

    // Built with SQL range() rather than createDataFrame(Seq): Dataset encoders need
    // Scala 2 TypeTags, which Scala 3 does not emit.
    spark.sql("DROP TABLE IF EXISTS clicks")
    spark.sql("CREATE TABLE clicks (product STRING) USING parquet")
    spark.sql("""INSERT INTO clicks
      SELECT 'running shoes'  FROM range(50) UNION ALL
      SELECT 'coffee grinder' FROM range(30) UNION ALL
      SELECT 'yoga mat'       FROM range(12) UNION ALL
      SELECT 'espresso maker' FROM range(5)  UNION ALL
      SELECT 'trail sneakers' FROM range(3)""")

  private def runtime(spark: SparkSession): String =
    val arrowUdf = spark.conf.get("spark.sql.execution.pythonUDF.arrow.enabled")
    val fnCount = spark.sql("SHOW FUNCTIONS").count()
    val payload =
      s"""{"sparkVersion":"${spark.version}",
          |"scalaCompiler":"3.7.4",
          |"scalaRuntimeStdlib":"${scala.util.Properties.versionNumberString}",
          |"javaVersion":"${System.getProperty("java.version")}",
          |"builtinFunctions":$fnCount,
          |"arrowPythonUdfDefault":"$arrowUdf"}""".stripMargin.replaceAll("\n", "")
    payload

  /** Metric views: one governed definition, correct at every grain.
    * COUNT(DISTINCT customer) is non-additive - summing per-region counts double counts
    * customers who bought in more than one region.
    */
  private def metricViews(spark: SparkSession): String =
    val yaml =
      """version: 0.1
        |source: orders
        |dimensions:
        |  - name: region
        |    expr: region
        |measures:
        |  - name: revenue
        |    expr: SUM(amount)
        |  - name: customers
        |    expr: COUNT(DISTINCT customer)""".stripMargin
    spark.sql("CREATE OR REPLACE VIEW revenue_mv WITH METRICS LANGUAGE YAML AS $$\n" + yaml + "\n$$")

    val byRegion = spark.sql(
      "SELECT region, MEASURE(revenue) AS revenue, MEASURE(customers) AS customers " +
        "FROM revenue_mv GROUP BY region ORDER BY revenue DESC")
    val governed = spark.sql("SELECT MEASURE(customers) AS customers FROM revenue_mv")
      .collect().head.getLong(0)
    val naive = spark.sql(
      "SELECT SUM(customers) FROM (SELECT region, MEASURE(customers) AS customers " +
        "FROM revenue_mv GROUP BY region)").collect().head.getLong(0)

    s"""{"yaml":${quote(yaml)},"byRegion":${rows(byRegion)},
       |"governedTotalCustomers":$governed,"naiveSummedCustomers":$naive}""".stripMargin.replaceAll("\n", "")

  /** Vector similarity + NEAREST BY top-K ranking join. */
  private def vectorSearch(spark: SparkSession): String =
    spark.sql("CREATE OR REPLACE TEMP VIEW query AS SELECT 'athletic footwear' AS q, array(0.92f, 0.15f, 0.08f) AS qv")
    val nearest = spark.sql(
      """SELECT p.name, vector_l2_distance(q.qv, p.embedding) AS distance,
        |       vector_cosine_similarity(q.qv, p.embedding) AS similarity
        |FROM query q JOIN products p EXACT NEAREST 3 BY DISTANCE vector_l2_distance(q.qv, p.embedding)""".stripMargin)
    val norms = spark.sql(
      "SELECT name, vector_norm(embedding) AS norm, vector_normalize(embedding) AS unit FROM products ORDER BY id")
    s"""{"queryText":"athletic footwear","queryVector":[0.92,0.15,0.08],
       |"nearest":${rows(nearest)},"norms":${rows(norms)}}""".stripMargin.replaceAll("\n", "")

  /** Probabilistic sketches: approx_top_k and theta sketch cardinality. */
  private def sketches(spark: SparkSession): String =
    val topk = spark.sql("SELECT explode(approx_top_k(product, 3)) AS t FROM clicks")
      .selectExpr("t.item AS item", "t.count AS count")
    val exact = spark.sql("SELECT COUNT(DISTINCT product) AS c FROM clicks").collect().head.getLong(0)
    val theta = spark.sql("SELECT theta_sketch_estimate(theta_sketch_agg(product)) AS c FROM clicks")
      .collect().head.getLong(0)
    s"""{"topK":${rows(topk)},"exactDistinct":$exact,"thetaSketchDistinct":$theta}"""

  /** Native GEOMETRY / GEOGRAPHY types with SRID preservation. */
  private def geospatial(spark: SparkSession): String =
    val df = spark.sql(
      """SELECT 'store-lisbon' AS site,
        |       st_srid(st_setsrid(st_geomfromwkb(X'0101000000000000000000F03F000000000000F03F'), 4326)) AS srid,
        |       typeof(st_setsrid(st_geomfromwkb(X'0101000000000000000000F03F000000000000F03F'), 4326)) AS geometryType,
        |       typeof(CAST(NULL AS GEOGRAPHY(4326))) AS geographyType""".stripMargin)
    val fns = spark.sql("SHOW FUNCTIONS").collect().map(_.getString(0)).filter(_.startsWith("st_"))
    s"""{"sample":${rows(df)},"availableStFunctions":${fns.map(quote).mkString("[", ",", "]")}}"""

  private def quote(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\""
