from __future__ import annotations
import glob
from app.infra.db import Postgres

def main():
    db = Postgres()
    for f in sorted(glob.glob("db/migrations/*.sql")):
        sql = open(f, "r", encoding="utf-8").read()
        if sql.strip():
            db.execute(sql)
            print(f"Applied {f}")
    print("Done.")

if __name__ == "__main__":
    main()
