# PySpark ETL Demo

Minimal PySpark ETL example that reads customer and transaction CSVs, aggregates total spend per customer, and writes results to Parquet and CSV.

Usage
- Install Python deps (optional when using Spark's PySpark):

```bash
pip install -r requirements.txt
```

- Run locally (will generate sample data if no input provided):

```bash
python demo.py
```

- Provide input/output paths or generate sample input into a folder:

```bash
python demo.py --generate-sample --input ./input --output ./out
# or with spark-submit
spark-submit demo.py --input ./input --output ./out
```

Files
- `demo.py`: ETL script
- `requirements.txt`: minimal Python dependency list

Notes
- Requires a working Spark installation for `spark-submit`.
- Running with plain `python` uses the PySpark package and a local Spark runtime.