import pandas as pd
import mysql.connector

DB = {
    "host": "localhost",
    "user": "[ID]",
    "password": "[PASSWORD]",
    "database": "anime_project",
}

def main():
    df = pd.read_csv("data/raw/entities.csv")

    # 문자열 정리
    df["name"] = df["name"].astype(str).str.strip()
    df["entity_type"] = df["entity_type"].astype(str).str.strip()

    # "nan" 문자열로 들어온 케이스도 제거
    df.loc[df["name"].isin(["", "nan", "None", "NaN"]), "name"] = None
    df.loc[df["entity_type"].isin(["", "nan", "None", "NaN"]), "entity_type"] = None

    # name/entity_type 없는 행 제거 (DB NOT NULL 대응)
    before = len(df)
    df = df.dropna(subset = ["entity_id", "entity_type", "name"]).drop_duplicates()
    after = len(df)

    # NaN -> None (MySQL NULL)
    df = df.where(pd.notnull(df), None)

    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()

    sql = """
    INSERT INTO entities (entity_id, entity_type, name, image_url)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      entity_type = VALUES(entity_type),
      name = VALUES(name),
      image_url = VALUES(image_url);
    """

    cur.executemany(sql, [tuple(r) for r in df.itertuples(index = False, name = None)])
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM entities;")
    print("entities rows:", cur.fetchone()[0])
    print("dropped rows:", before - after)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()