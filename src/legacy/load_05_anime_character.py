import pandas as pd
import mysql.connector

DB = {
    "host": "localhost",
    "user": "[ID]",
    "password": "[PASSWORD]",
    "database": "anime_project",
}

def main():
    df = pd.read_csv("data/raw/anime_characters.csv")

    # 결측 제거 + 중복 제거
    df = df.dropna(subset=["anime_id", "character_id"]).drop_duplicates()

    # 타입 정리
    df["anime_id"] = df["anime_id"].astype(int)
    df["character_id"] = df["character_id"].astype(int)

    # role 컬럼이 없을 수도 있으니 안전 처리
    if "role" not in df.columns:
        df["role"] = None

    df = df[["anime_id", "character_id", "role"]]
    df = df.where(pd.notnull(df), None)

    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()

    sql = """
    INSERT IGNORE INTO anime_character (anime_id, character_id, role)
    VALUES (%s, %s, %s);
    """

    cur.executemany(sql, [tuple(r) for r in df.itertuples(index = False, name = None)])
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM anime_character;")
    print("anime_character rows:", cur.fetchone()[0])

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()