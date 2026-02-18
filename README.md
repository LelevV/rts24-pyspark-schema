# rts24-pyspark-schema
Parse RTS 24 Annex Table 2 into a PySpark schema + field metadata from the EU PDF.

## What is RTS24 orderdata?
RTS 24 (MiFID II / MiFIR) defines the order record keeping data that trading venues and firms must collect, store, and (where required) report for orders in financial instruments, so competent authorities can monitor market integrity and trading behaviour. This repo extracts the Annex (Table 2) field set from the official RTS 24 PDF and turns it into a PySpark schema plus a per-field metadata catalog.

Extracts all fields from **RTS 24 Annex (Table 2)** and generates:
- a **PySpark `StructType` schema** (derived from Standards/Formats when possible)
- a **field-metadata dataset** (one row per field) with section/description/standards/type hints

Source url: https://ec.europa.eu/finance/securities/docs/isd/mifid/rts/160624-rts-24-annex_en.pdf

## Requirements
- Python 3.10+
- Java (required by PySpark). On macOS: `brew install openjdk`.
- `uv` installed: https://docs.astral.sh/uv/

## Setup (uv)
```bash
uv venv
uv sync
