"""
Docstring for src.rts24-pyspark-schema.cli

Description:
    Command-line interface for extracting fields from the RTS 24 Table 2 PDF and generating a PySpark schema. 

"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from .extract import DEFAULT_PDF_URL, run


def main() -> None:
    p = argparse.ArgumentParser(description="Extract RTS 24 Table 2 fields and generate PySpark schema.")
    p.add_argument("--url", default=DEFAULT_PDF_URL, help="PDF URL to download and parse")
    p.add_argument("--out", default="outputs", help="Output directory")
    p.add_argument(
        "--spark-metadata",
        action="store_true",
        help="Also create a Spark DataFrame for field metadata and print a preview",
    )
    args = p.parse_args()

    out_dir = Path(args.out)
    result = run(url=args.url, out_dir=out_dir)

    print(f"Extracted fields: {len(result.fields_meta)}")
    print("Schema (simpleString):")
    print(result.schema.simpleString())
    print(f"Wrote: {out_dir / 'fields_meta.csv'}")
    print(f"Wrote: {out_dir / 'schema_simpleString.txt'}")

    if args.spark_metadata:
        spark = SparkSession.builder.getOrCreate()
        sdf = spark.createDataFrame(result.fields_meta.astype(str))
        sdf.show(truncate=False)

        empty_df = spark.createDataFrame([], schema=result.schema)
        empty_df.printSchema()
