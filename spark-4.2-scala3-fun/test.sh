#!/bin/bash
set -e
cd "$(dirname "$0")"

mkdir -p out
rm -f out/results.json

podman-compose build spark-job
podman-compose run --rm spark-job

if [ ! -f out/results.json ]; then
  echo "FAIL: out/results.json was not produced"
  exit 1
fi

python3 - <<'PY'
import json, sys

d = json.load(open("out/results.json"))
fails = []

def check(name, actual, expected):
    ok = actual == expected
    print(f"{'PASS' if ok else 'FAIL'}  {name}: {actual!r}" + ("" if ok else f" (expected {expected!r})"))
    if not ok:
        fails.append(name)

def show(name, value):
    print(f"      {name}: {value}")

r = d["runtime"]
check("spark version", r["sparkVersion"], "4.2.0")
check("scala compiler", r["scalaCompiler"], "3.7.4")
show("scala runtime stdlib", r["scalaRuntimeStdlib"])
show("java", r["javaVersion"])
show("builtin functions", r["builtinFunctions"])

m = d["metricView"]
check("metric view governed distinct customers", m["governedTotalCustomers"], 4)
check("naive summed customers (double counts)", m["naiveSummedCustomers"], 6)
check("metric view regions", len(m["byRegion"]), 3)

v = d["vector"]
check("NEAREST BY returned top-3", len(v["nearest"]), 3)
check("nearest product to 'athletic footwear'", v["nearest"][0]["name"], "running shoes")
if not v["nearest"][0]["distance"] < v["nearest"][2]["distance"]:
    fails.append("distance ordering")
    print("FAIL  distance ordering")
else:
    print("PASS  distance ordering: ranked ascending by L2")

s = d["sketches"]
check("approx_top_k heavy hitter", s["topK"][0]["item"], "running shoes")
check("approx_top_k count", s["topK"][0]["count"], 50)
check("theta sketch matches exact distinct", s["thetaSketchDistinct"], s["exactDistinct"])

g = d["geo"]
check("geometry SRID preserved", g["sample"][0]["srid"], 4326)
check("GEOMETRY type", g["sample"][0]["geometryType"], "geometry(4326)")
show("ST functions in 4.2", ", ".join(g["availableStFunctions"]))

print()
if fails:
    print(f"{len(fails)} check(s) FAILED: {fails}")
    sys.exit(1)
print("All checks passed.")
PY
