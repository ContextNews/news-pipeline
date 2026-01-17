"""Print a readable database schema from the RDS connection."""

from __future__ import annotations

from dotenv import load_dotenv
from sqlalchemy import inspect

load_dotenv()

from rds_postgres.connection import engine


def _format_default(value: object) -> str:
    if value is None:
        return "None"
    return str(value)


def print_schema() -> None:
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    for table in table_names:
        print(f"Table: {table}")

        columns = inspector.get_columns(table)
        for col in columns:
            name = col["name"]
            col_type = str(col["type"])
            nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
            default = _format_default(col.get("default"))
            primary_key = "PK" if col.get("primary_key") else ""
            print(f"  Column: {name} | {col_type} | {nullable} | default={default} {primary_key}".rstrip())

        indexes = inspector.get_indexes(table)
        if indexes:
            print("  Indexes:")
            for idx in indexes:
                cols = ", ".join(idx.get("column_names", []))
                unique = " UNIQUE" if idx.get("unique") else ""
                print(f"    - {idx.get('name')} ({cols}){unique}")

        fks = inspector.get_foreign_keys(table)
        if fks:
            print("  Foreign keys:")
            for fk in fks:
                cols = ", ".join(fk.get("constrained_columns", []))
                ref_table = fk.get("referred_table")
                ref_cols = ", ".join(fk.get("referred_columns", []))
                print(f"    - ({cols}) -> {ref_table}({ref_cols})")

        print("")


def main() -> None:
    print_schema()


if __name__ == "__main__":
    main()
