import pandas as pd
import mysql.connector

DB = {
    "host": "localhost",
    "user": "[ID]",
    "password": "[PASSWORD]",
    "database": "anime_project",
}

df = pd.read_csv("data/raw/anime.csv")

# 컬럼명 정리
df.rename(columns = {"rank": "mal_rank"}, inplace = True)

# 날짜 변환 (실패하면 NaT)
df["start_date"] = pd.to_datetime(df["start_date"], errors = "coerce").dt.date
df["end_date"] = pd.to_datetime(df["end_date"], errors = "coerce").dt.date

# episodes 정수화 (결측은 0)
df["episodes"] = pd.to_numeric(df["episodes"], errors = "coerce").fillna(0).astype(int)

cols = [
    "anime_id", "title", "score", "mal_rank",
    "popularity", "members", "synopsis",
    "start_date", "end_date", "type",
    "episodes", "image_url"
]
df = df[cols]

# ✅ NaN/NaT를 MySQL NULL로 안전 변환
df = df.where(pd.notnull(df), None)

conn = mysql.connector.connect(**DB)
cursor = conn.cursor()

sql = """
INSERT INTO anime_dim
(anime_id, title, score, mal_rank, popularity, members,
 synopsis, start_date, end_date, type, episodes, image_url)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
title = VALUES(title),
score = VALUES(score),
mal_rank = VALUES(mal_rank),
popularity = VALUES(popularity),
members = VALUES(members),
synopsis = VALUES(synopsis),
start_date = VALUES(start_date),
end_date = VALUES(end_date),
type = VALUES(type),
episodes = VALUES(episodes),
image_url = VALUES(image_url);
"""

data = [tuple(row) for row in df.itertuples(index = False, name = None)]
cursor.executemany(sql, data)
conn.commit()

cursor.execute("SELECT COUNT(*) FROM anime_dim;")
print("anime_dim rows:", cursor.fetchone()[0])

cursor.close()
conn.close()