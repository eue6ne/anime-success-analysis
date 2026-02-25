USE anime_project;

LOAD DATA LOCAL INFILE 'data/raw/anime_characters.csv'
INTO TABLE anime_character
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(anime_id, character_id, role);