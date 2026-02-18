"""
Docstring for src.rts24-pyspark-schema.extract

Description:
    Core logic for extracting fields from the RTS 24 Table 2 PDF and generating a PySpark schema.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import pdfplumber
import requests
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


DEFAULT_PDF_URL = "https://ec.europa.eu/finance/securities/docs/isd/mifid/rts/160624-rts-24-annex_en.pdf"


def download_pdf(url: str, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    out_path.write_bytes(r.content)
    return out_path


def to_col_name(field_name: str) -> str:
    x = field_name.lower()
    x = re.sub(r"[^a-z0-9]+", "_", x).strip("_")
    if re.match(r"^\d", x):
        x = "f_" + x
    return x


def suggest_spark_type(standards_formats: str):
    s = (standards_formats or "").lower()

    # boolean if explicitly true/false
    if "'true'" in s and "'false'" in s:
        return BooleanType()

    # timestamp/date cues
    if "{date_time" in s or "yyyy-mm-ddt" in s:
        return TimestampType()
    if "{dateformat" in s:
        return DateType()

    # decimals like {DECIMAL-18/5}
    decs = re.findall(r"\{decimal-(\d+)\/(\d+)\}", s)
    if decs:
        prec = max(int(p) for p, _ in decs)
        scale = max(int(sc) for _, sc in decs)
        return DecimalType(prec, scale)

    # default
    return StringType()


@dataclass(frozen=True)
class ExtractResult:
    fields_meta: pd.DataFrame
    schema: StructType


def extract_table2_fields(pdf_path: Path) -> pd.DataFrame:
    """
    Extract Table 2 rows (N=1..51) from the RTS 24 Annex PDF.
    Returns a DataFrame with:
      n, section, field, content, standards_formats, page, col_name, spark_type
    """
    rows = []
    current_section: Optional[str] = None

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            for tbl in (page.extract_tables() or []):
                for row in tbl:
                    if not row or all((c is None or str(c).strip() == "") for c in row):
                        continue

                    first = str(row[0]).strip() if row[0] is not None else ""

                    # Section header (e.g., "Section A - ...")
                    if first.lower().startswith("section"):
                        current_section = first.replace("\n", " ").strip()
                        continue

                    # Field row starts with an integer N
                    if re.fullmatch(r"\d+", first):
                        # Expected columns: [N, Field, Content, Standards/Formats]
                        field = (row[1] or "")
                        content = (row[2] or "")
                        std = (row[3] or "")

                        rows.append(
                            {
                                "n": int(first),
                                "section": current_section,
                                "field": str(field).replace("\n", " ").strip(),
                                "content": str(content).replace("\n", " ").strip(),
                                "standards_formats": str(std).replace("\n", " ").strip(),
                                "page": page_idx + 1,
                            }
                        )

    df = pd.DataFrame(rows).sort_values("n").reset_index(drop=True)

    # Derived fields for schema + nicer usage
    df["col_name"] = df["field"].apply(to_col_name)
    df["spark_type"] = df["standards_formats"].apply(lambda s: suggest_spark_type(s).simpleString())
    return df


def build_schema(fields_meta: pd.DataFrame) -> StructType:
    struct_fields = []
    for _, r in fields_meta.sort_values("n").iterrows():
        spark_t = suggest_spark_type(r["standards_formats"])
        struct_fields.append(StructField(str(r["col_name"]), spark_t, True))
    return StructType(struct_fields)


def run(url: str = DEFAULT_PDF_URL, out_dir: Path = Path("outputs")) -> ExtractResult:
    out_dir.mkdir(parents=True, exist_ok=True)

    pdf_path = out_dir / "rts24_annex_en.pdf"
    download_pdf(url, pdf_path)

    fields_meta = extract_table2_fields(pdf_path)
    schema = build_schema(fields_meta)

    # Write artifacts
    fields_meta.to_csv(out_dir / "fields_meta.csv", index=False)
    (out_dir / "schema_simpleString.txt").write_text(schema.simpleString() + "\n", encoding="utf-8")

    return ExtractResult(fields_meta=fields_meta, schema=schema)
