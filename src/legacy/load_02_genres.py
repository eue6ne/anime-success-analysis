import pandas as pd
import mysql.connector
import re

DB = {
    "host": "localhost",
    "user": "[ID]",
    "password": "[PASSWORD]",
    "database": "anime_project",
}

def clean_genre(g: str) -> str:
    if g is None:
        return None
    g = str(g).strip()
    if not g:
        return None
    # "Action Action" 같이 같은 단어 반복이면 하나로
    parts = re.split(r"\s+", g)
    if len(parts) == 2 and parts[0].lower() == parts[1].lower():
        return parts[0]
    return g

def main():
    df = pd.read_csv("data/raw/anime_genres.csv")
    df["genre"] = df["genre"].map(clean_genre)
    df = df.dropna(subset=["genre"]).drop_duplicates()

    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()

    # 1) genre_dim 채우기
    genres = sorted(df["genre"].unique().tolist())
    cur.executemany(
        "INSERT IGNORE INTO genre_dim (genre_name) VALUES (%s);",
        [(g,) for g in genres],
    )
    conn.commit()

    # 2) 매핑 넣기 (anime_id + genre_id)
    cur.execute("SELECT genre_id, genre_name FROM genre_dim;")
    genre_map = {name: gid for (gid, name) in cur.fetchall()}

    rows = []
    for anime_id, genre in df[["anime_id", "genre"]].itertuples(index = False, name = None):
        gid = genre_map.get(genre)
        if gid is not None:
            rows.append((int(anime_id), int(gid)))

    cur.executemany(
        "INSERT IGNORE INTO anime_genre_map (anime_id, genre_id) VALUES (%s, %s);",
        rows,
    )
    conn.commit()

    # 3) 확인 출력
    cur.execute("SELECT COUNT(*) FROM genre_dim;")
    print("genre_dim rows:", cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM anime_genre_map;")
    print("anime_genre_map rows:", cur.fetchone()[0])

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()