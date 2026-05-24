from pathlib import Path

from src.db import get_conn


def main():
    schema_sql = Path("sql/schema.sql").read_text(encoding="utf-8")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)

        conn.commit()

    print("Schema applied successfully.")


if __name__ == "__main__":
    main()