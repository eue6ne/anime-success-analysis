USE anime_project;

DROP VIEW IF EXISTS feature_anime;

CREATE VIEW feature_anime AS
SELECT
    a.anime_id,
    a.title,
    a.score,
    a.members,
    a.type,
    a.episodes,
    a.start_date,
    a.popularity,
    COUNT(DISTINCT ag.genre_id) AS genre_count,
    COUNT(DISTINCT ac.company_id) AS company_count,
    COUNT(DISTINCT CASE WHEN ac.role = 'Studio' THEN ac.company_id END) AS studio_count,
    COUNT(DISTINCT CASE WHEN ac.role = 'Producer' THEN ac.company_id END) AS producer_count,
    COUNT(DISTINCT ava.person_id) AS voice_actor_count,
    COUNT(DISTINCT CASE WHEN ava.language = 'Japanese' THEN ava.person_id END) AS japanese_va_count
FROM anime_dim a
LEFT JOIN anime_genre_map ag ON a.anime_id = ag.anime_id
LEFT JOIN anime_company ac ON a.anime_id = ac.anime_id
LEFT JOIN anime_voice_actor ava ON a.anime_id = ava.anime_id
GROUP BY a.anime_id, a.title, a.score, a.members, a.type, a.episodes, a.start_date, a.popularity;