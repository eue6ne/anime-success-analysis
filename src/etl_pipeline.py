from __future__ import annotations

import warnings
warnings.filterwarnings('ignore')

import os
import re
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import mysql.connector
from dotenv import load_dotenv

load_dotenv()


# -----------------------------
# Project Paths
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"


# -----------------------------
# Database Config (Environment Variables Only)
# -----------------------------
DB = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

CHUNK_SIZE = int(os.getenv("ETL_CHUNK_SIZE", "5000"))


# -----------------------------
# Helpers
# -----------------------------
def validate_db_config():
    missing = [k for k, v in DB.items() if v is None and k != "port"]
    if missing:
        raise ValueError(
            f"Missing DB environment variables: {missing}\n"
            "Set DB_HOST, DB_USER, DB_PASSWORD, DB_NAME before running."
        )


def connect_db():
    validate_db_config()
    return mysql.connector.connect(**DB)


def df_to_tuples(df: pd.DataFrame) -> List[Tuple]:
    df = df.where(pd.notnull(df), None)
    return [tuple(r) for r in df.itertuples(index = False, name = None)]


def executemany_in_chunks(cur, sql: str, rows: List[Tuple], chunk_size: int = CHUNK_SIZE) -> int:
    if not rows:
        return 0
    total = 0
    for i in range(0, len(rows), chunk_size):
        batch = rows[i : i + chunk_size]
        cur.executemany(sql, batch)
        total += len(batch)
    return total


def clean_genre(g: str) -> str | None:
    if g is None:
        return None
    g = str(g).strip()
    if not g:
        return None
    parts = re.split(r"\s+", g)
    if len(parts) == 2 and parts[0].lower() == parts[1].lower():
        return parts[0]
    return g


# -----------------------------
# Load Steps
# -----------------------------
def load_anime_dim(cur) -> int:
    df = pd.read_csv(RAW_DIR / "anime.csv")

    # align schema
    df = df.rename(columns={"rank": "mal_rank"})
    df["start_date"] = pd.to_datetime(df["start_date"], errors = "coerce").dt.date
    df["end_date"] = pd.to_datetime(df["end_date"], errors = "coerce").dt.date
    df["episodes"] = pd.to_numeric(df["episodes"], errors = "coerce").fillna(0).astype(int)

    cols = [
        "anime_id", "title", "score", "mal_rank",
        "popularity", "members", "synopsis",
        "start_date", "end_date", "type",
        "episodes", "image_url",
    ]
    df = df[cols].drop_duplicates(subset =["anime_id"]).copy()

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
    """.strip()

    rows = df_to_tuples(df)
    return executemany_in_chunks(cur, sql, rows, chunk_size = 2000)


def load_genres(cur) -> Tuple[int, int]:
    df = pd.read_csv(RAW_DIR / "anime_genres.csv")
    df["genre"] = df["genre"].map(clean_genre)
    df = df.dropna(subset =["genre"]).drop_duplicates()

    # genre_dim
    genres = sorted(df["genre"].unique().tolist())
    sql_genre_dim = "INSERT IGNORE INTO genre_dim (genre_name) VALUES (%s);"
    genre_rows = [(g,) for g in genres]
    genre_dim_cnt = executemany_in_chunks(cur, sql_genre_dim, genre_rows, chunk_size = 2000)

    # map genre_name -> genre_id
    cur.execute("SELECT genre_id, genre_name FROM genre_dim;")
    genre_map = {name: gid for (gid, name) in cur.fetchall()}

    # anime_genre_map
    rows = []
    for anime_id, genre in df[["anime_id", "genre"]].itertuples(index = False, name = None):
        gid = genre_map.get(genre)
        if gid is not None:
            rows.append((int(anime_id), int(gid)))

    sql_map = "INSERT IGNORE INTO anime_genre_map (anime_id, genre_id) VALUES (%s, %s);"
    map_cnt = executemany_in_chunks(cur, sql_map, rows, chunk_size = 5000)

    return genre_dim_cnt, map_cnt


def load_entities(cur) -> int:
    df = pd.read_csv(RAW_DIR / "entities.csv")

    df["name"] = df["name"].astype(str).str.strip()
    df["entity_type"] = df["entity_type"].astype(str).str.strip()

    df.loc[df["name"].isin(["", "nan", "None", "NaN"]), "name"] = None
    df.loc[df["entity_type"].isin(["", "nan", "None", "NaN"]), "entity_type"] = None

    df = df.dropna(subset = ["entity_id", "entity_type", "name"]).drop_duplicates()
    df = df[["entity_id", "entity_type", "name", "image_url"]].copy()

    sql = """
    INSERT INTO entities (entity_id, entity_type, name, image_url)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      entity_type = VALUES(entity_type),
      name = VALUES(name),
      image_url = VALUES(image_url);
    """.strip()

    rows = df_to_tuples(df)
    return executemany_in_chunks(cur, sql, rows, chunk_size = 5000)


def load_companies(cur) -> Tuple[int, int]:
    entities = pd.read_csv(RAW_DIR / "entities.csv")
    comp = pd.read_csv(RAW_DIR / "anime_companies.csv")

    company_entities = entities[entities["entity_type"].isin(["studio", "producer"])].copy()
    company_entities = company_entities[["entity_id", "entity_type", "name", "image_url"]].drop_duplicates()

    sql_company = """
    INSERT INTO company (company_id, entity_type, name, image_url)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      entity_type = VALUES(entity_type),
      name = VALUES(name),
      image_url = VALUES(image_url);
    """.strip()

    company_rows = df_to_tuples(company_entities)
    company_cnt = executemany_in_chunks(cur, sql_company, company_rows, chunk_size = 5000)

    comp = comp.dropna(subset = ["anime_id", "company_id", "role"]).drop_duplicates().copy()
    comp = comp.astype({"anime_id": int, "company_id": int})

    sql_map = """
    INSERT IGNORE INTO anime_company (anime_id, company_id, role)
    VALUES (%s, %s, %s);
    """.strip()

    map_rows = df_to_tuples(comp[["anime_id", "company_id", "role"]])
    map_cnt = executemany_in_chunks(cur, sql_map, map_rows, chunk_size = 5000)

    return company_cnt, map_cnt


def load_anime_character(cur) -> int:
    path = RAW_DIR / "anime_characters.csv"
    if not path.exists():
        return 0

    df = pd.read_csv(path)
    df = df.dropna(subset = ["anime_id", "character_id"]).drop_duplicates().copy()
    df["anime_id"] = df["anime_id"].astype(int)
    df["character_id"] = df["character_id"].astype(int)

    if "role" not in df.columns:
        df["role"] = None

    df = df[["anime_id", "character_id", "role"]]

    sql = """
    INSERT IGNORE INTO anime_character (anime_id, character_id, role)
    VALUES (%s, %s, %s);
    """.strip()

    rows = df_to_tuples(df)
    return executemany_in_chunks(cur, sql, rows, chunk_size = 5000)


def load_anime_voice_actor(cur) -> int:
    df = pd.read_csv(RAW_DIR / "anime_voice_actors.csv")
    df = df.dropna(subset =["character_id", "person_id"]).drop_duplicates().copy()
    df["character_id"] = df["character_id"].astype(int)
    df["person_id"] = df["person_id"].astype(int)

    if "language" not in df.columns:
        df["language"] = None

    df = df[["character_id", "person_id", "language"]]
    df = df.where(pd.notnull(df), None)

    # character_id -> anime_id mapping from anime_character
    cur.execute("SELECT character_id, anime_id FROM anime_character;")
    map_df = pd.DataFrame(cur.fetchall(), columns=["character_id", "anime_id"])
    if map_df.empty:
        return 0

    merged = df.merge(map_df, on = "character_id", how = "inner")
    merged = merged[["anime_id", "character_id", "person_id", "language"]].drop_duplicates().copy()
    merged["anime_id"] = merged["anime_id"].astype(int)

    sql = """
    INSERT IGNORE INTO anime_voice_actor (anime_id, character_id, person_id, language)
    VALUES (%s, %s, %s, %s);
    """.strip()

    rows = df_to_tuples(merged)
    return executemany_in_chunks(cur, sql, rows, chunk_size = 5000)


# -----------------------------
# Main
# -----------------------------
def main():
    conn = connect_db()
    cur = conn.cursor()

    try:
        a = load_anime_dim(cur)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM anime_dim;")
        print(f"anime_dim rows: {cur.fetchone()[0]} (loaded/updated {a})")

        g_dim, g_map = load_genres(cur)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM genre_dim;")
        print(f"genre_dim rows: {cur.fetchone()[0]} (insert attempted {g_dim})")
        cur.execute("SELECT COUNT(*) FROM anime_genre_map;")
        print(f"anime_genre_map rows: {cur.fetchone()[0]} (insert attempted {g_map})")

        e = load_entities(cur)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM entities;")
        print(f"entities rows: {cur.fetchone()[0]} (loaded/updated {e})")

        c_cnt, ac_cnt = load_companies(cur)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM company;")
        print(f"company rows: {cur.fetchone()[0]} (loaded/updated {c_cnt})")
        cur.execute("SELECT COUNT(*) FROM anime_company;")
        print(f"anime_company rows: {cur.fetchone()[0]} (insert attempted {ac_cnt})")

        ch = load_anime_character(cur)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM anime_character;")
        print(f"anime_character rows: {cur.fetchone()[0]} (insert attempted {ch})")

        va = load_anime_voice_actor(cur)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM anime_voice_actor;")
        print(f"anime_voice_actor rows: {cur.fetchone()[0]} (insert attempted {va})")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()