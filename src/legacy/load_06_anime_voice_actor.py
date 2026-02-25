import pandas as pd
import mysql.connector

DB = {
    "host": "localhost",
    "user": "[ID]",
    "password": "[PASSWORD]",
    "database": "anime_project",
}

def main():
    df = pd.read_csv("data/raw/anime_voice_actors.csv")

    df = df.dropna(subset=["character_id", "person_id"]).drop_duplicates()
    df["character_id"] = df["character_id"].astype(int)
    df["person_id"] = df["person_id"].astype(int)

    if "language" not in df.columns:
        df["language"] = None

    df = df[["character_id", "person_id", "language"]]
    df = df.where(pd.notnull(df), None)

    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()

    # anime_id 없이 임시로 NULL 넣는 게 불가(스키마에서 NOT NULL로 바꿨기 때문에)
    # character_id -> anime_id를 먼저 조회해서 merge한 뒤 insert

    # character_id -> anime_id 매핑 가져오기
    cur.execute("SELECT character_id, anime_id FROM anime_character;")
    char_to_anime = cur.fetchall()
    map_df = pd.DataFrame(char_to_anime, columns = ["character_id", "anime_id"])

    merged = df.merge(map_df, on = "character_id", how = "inner")
    merged = merged[["anime_id", "character_id", "person_id", "language"]].drop_duplicates()
    merged["anime_id"] = merged["anime_id"].astype(int)

    sql = """
    INSERT IGNORE INTO anime_voice_actor (anime_id, character_id, person_id, language)
    VALUES (%s, %s, %s, %s);
    """

    cur.executemany(sql, [tuple(r) for r in merged.itertuples(index = False, name = None)])
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM anime_voice_actor;")
    print("anime_voice_actor rows:", cur.fetchone()[0])

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()