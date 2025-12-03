#!/usr/bin/env python3
import argparse
import csv
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("This script requires pandas. Install with: pip install pandas", file=sys.stderr)
    sys.exit(1)

SQL_TYPE_MAP = {
    "int": "BIGINT",
    "float": "FLOAT",
    "bool": "BIT",
    "datetime": "DATETIME2",
    "date": "DATE",
    "time": "TIME",
    "string": "NVARCHAR(MAX)",
}

def sniff_delimiter(csv_path):
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        sample = f.read(2048)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return ","

def bracket_ident(name: str) -> str:
    # Quote identifiers with [ ] to be safe with spaces/reserved words
    name = name.replace("]", "]]")
    return f"[{name}]"

def quote_value(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "NULL"
    if pd.isna(val):
        return "NULL"
    # Keep ints and floats numeric
    if isinstance(val, (int, float)):
        # Handle NaN already done
        return str(int(val)) if isinstance(val, (int,)) else str(val)
    s = str(val)
    s = s.replace("'", "''")
    # Use Unicode literal for SQL Server
    return f"N'{s}'"

def detect_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return SQL_TYPE_MAP["int"]
    if pd.api.types.is_float_dtype(series):
        return SQL_TYPE_MAP["float"]
    if pd.api.types.is_bool_dtype(series):
        return SQL_TYPE_MAP["bool"]
    if pd.api.types.is_datetime64_any_dtype(series):
        return SQL_TYPE_MAP["datetime"]
    # Heuristics for date/time in object columns
    if pd.api.types.is_object_dtype(series):
        sample_non_null = series.dropna().astype(str).head(100)
        def looks_like_date(s):
            # Simple patterns check
            return any(sep in s for sep in ["-", "/"]) and any(ch.isdigit() for ch in s)
        def looks_like_time(s):
            return s.count(":") >= 1 and any(ch.isdigit() for ch in s)
        if len(sample_non_null) and sample_non_null.map(looks_like_date).mean() > 0.9:
            return SQL_TYPE_MAP["date"]
        if len(sample_non_null) and sample_non_null.map(looks_like_time).mean() > 0.9:
            return SQL_TYPE_MAP["time"]
    return SQL_TYPE_MAP["string"]

def build_create_table(df: pd.DataFrame, table_name: str) -> str:
    cols_sql = []
    for col in df.columns:
        sql_type = detect_type(df[col])
        cols_sql.append(f"{bracket_ident(col)} {sql_type}")
    cols_joined = ",\n    ".join(cols_sql)
    return f"IF OBJECT_ID(N'{table_name}', N'U') IS NOT NULL DROP TABLE {table_name};\nGO\nCREATE TABLE {table_name} (\n    {cols_joined}\n);\nGO"

def build_inserts(df: pd.DataFrame, table_name: str, batch_size: int = 1000) -> str:
    col_idents = ", ".join(bracket_ident(c) for c in df.columns)
    statements = []
    batch = []
    for _, row in df.iterrows():
        values = [quote_value(row[c]) for c in df.columns]
        batch.append("(" + ", ".join(values) + ")")
        if len(batch) >= batch_size:
            statements.append(f"INSERT INTO {table_name} ({col_idents}) VALUES\n" + ",\n".join(batch) + ";\nGO")
            batch = []
    if batch:
        statements.append(f"INSERT INTO {table_name} ({col_idents}) VALUES\n" + ",\n".join(batch) + ";\nGO")
    return "\n".join(statements)

def main():
    ap = argparse.ArgumentParser(description="CSV -> SQL Server .sql (CREATE TABLE + INSERT).")
    ap.add_argument("--csv", required=True, help="Path to CSV file (UTF-8 by default).")
    ap.add_argument("--table", required=True, help="Target table name, e.g., dbo.MyTable")
    ap.add_argument("--out", default=None, help="Output .sql path (default: <csv_basename>_mssql.sql)")
    ap.add_argument("--encoding", default="utf-8", help="CSV encoding, default utf-8")
    ap.add_argument("--delimiter", default=None, help="CSV delimiter (auto-detected if omitted)")
    ap.add_argument("--na", nargs="*", default=["", "NA", "NaN", "null", "NULL"], help="Values treated as NULL")
    ap.add_argument("--limit", type=int, default=None, help="Optional row limit")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)
    delim = args.delimiter or sniff_delimiter(csv_path)
    try:
        df = pd.read_csv(csv_path, encoding=args.encoding, delimiter=delim, na_values=args.na, low_memory=False)
    except Exception as e:
        print(f"Failed to read CSV: {e}", file=sys.stderr)
        sys.exit(1)
    if args.limit:
        df = df.head(args.limit)

    out_path = Path(args.out) if args.out else csv_path.with_name(csv_path.stem + "_mssql.sql")

    create_sql = build_create_table(df, args.table)
    inserts_sql = build_inserts(df, args.table)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("-- Generated for SQL Server (SSMS)\n")
        f.write(f"-- Source: {csv_path.name}\n\n")
        f.write(create_sql)
        f.write("\n\n")
        if len(df) == 0:
            f.write("-- No rows to insert.\n")
        else:
            f.write(inserts_sql)

    print(f"Wrote SQL to: {out_path}")

if __name__ == "__main__":
    main()
