USE anime_project;

-- 1) 타입별 평균 평점/인기
SELECT type, COUNT(*) cnt, AVG(score) avg_score, AVG(members) avg_members
FROM anime_dim
GROUP BY type
ORDER BY cnt DESC;

-- 2) 장르 수 대비 평점
SELECT genre_count, COUNT(*) cnt, AVG(score) avg_score, AVG(members) avg_members
FROM feature_anime
WHERE score IS NOT NULL
GROUP BY genre_count
ORDER BY genre_count;

-- 3) 제작사(Studio) 작품 수 TOP 10
SELECT c.name, COUNT(*) cnt
FROM anime_company ac
JOIN company c ON ac.company_id = c.company_id
WHERE ac.role = 'Studio'
GROUP BY c.name
ORDER BY cnt DESC
LIMIT 10;

-- 4) 제작사(Studio) 평균 평점 TOP (작품 수 10개 이상)
SELECT c.name, COUNT(*) cnt, AVG(a.score) avg_score
FROM anime_company ac
JOIN company c ON ac.company_id = c.company_id
JOIN anime_dim a ON ac.anime_id = a.anime_id
WHERE ac.role='Studio' AND a.score IS NOT NULL
GROUP BY c.name
HAVING cnt >= 10
ORDER BY avg_score DESC
LIMIT 15;

-- 5) 성우 수와 평점의 관계(간단 집계)
SELECT
  CASE
    WHEN voice_actor_count < 5 THEN '<5'
    WHEN voice_actor_count BETWEEN 5 AND 9 THEN '5-9'
    WHEN voice_actor_count BETWEEN 10 AND 19 THEN '10-19'
    ELSE '20+'
  END AS va_bucket,
  COUNT(*) cnt,
  AVG(score) avg_score,
  AVG(members) avg_members
FROM feature_anime
WHERE score IS NOT NULL
GROUP BY va_bucket
ORDER BY cnt DESC;