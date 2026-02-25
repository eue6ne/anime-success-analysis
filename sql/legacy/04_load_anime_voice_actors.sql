USE anime_project;

LOAD DATA LOCAL INFILE 'data/raw/anime_voice_actors.csv'
INTO TABLE anime_voice_actor
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(character_id, person_id, language);

UPDATE anime_voice_actor ava
JOIN anime_character ac
  ON ava.character_id = ac.character_id
SET ava.anime_id = ac.anime_id
WHERE ava.anime_id IS NULL;