import pandas as pd
import mysql.connector

DB = {
    "host": "localhost",
    "user": "[ID]",
    "password": "[PASSWORD]",
    "database": "anime_project",
}

def main():
    entities = pd.read_csv("data/raw/entities.csv")
    comp = pd.read_csv("data/raw/anime_companies.csv")

    # entities에서 회사만 추출 (studio/producer 등)
    company_entities = entities[entities["entity_type"].isin(["studio", "producer"])].copy()

    # company 테이블 적재
    company_entities = company_entities[["entity_id", "entity_type", "name", "image_url"]].drop_duplicates()
    company_entities = company_entities.where(pd.notnull(company_entities), None)

    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()

    cur.executemany(
        """
        INSERT INTO company (company_id, entity_type, name, image_url)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          entity_type = VALUES(entity_type),
          name = VALUES(name),
          image_url = VALUES(image_url);
        """,
        [tuple(r) for r in company_entities.itertuples(index = False, name = None)]
    )
    conn.commit()

    # anime_company 매핑 적재
    comp = comp.dropna(subset = ["anime_id", "company_id", "role"]).drop_duplicates()
    comp = comp.astype({"anime_id": int, "company_id": int})
    comp = comp.where(pd.notnull(comp), None)

    cur.executemany(
        """
        INSERT IGNORE INTO anime_company (anime_id, company_id, role)
        VALUES (%s, %s, %s);
        """,
        [tuple(r) for r in comp[["anime_id", "company_id", "role"]].itertuples(index = False, name = None)]
    )
    conn.commit()

    # 확인 출력
    cur.execute("SELECT COUNT(*) FROM company;")
    print("company rows:", cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM anime_company;")
    print("anime_company rows:", cur.fetchone()[0])

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()