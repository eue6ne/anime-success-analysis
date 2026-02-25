USE anime_project;

DROP TABLE IF EXISTS anime_voice_actor;
DROP TABLE IF EXISTS anime_character;
DROP TABLE IF EXISTS anime_company;
DROP TABLE IF EXISTS anime_genre_map;

DROP TABLE IF EXISTS voice_actor;
DROP TABLE IF EXISTS character_entity;
DROP TABLE IF EXISTS company;
DROP TABLE IF EXISTS genre_dim;
DROP TABLE IF EXISTS anime_dim;

CREATE TABLE anime_dim (
  anime_id      INT PRIMARY KEY,
  title         VARCHAR(255) NOT NULL,
  score         DECIMAL(4,2),
  mal_rank      INT,
  popularity    INT,
  members       INT,
  synopsis      TEXT,
  start_date    DATE,
  end_date      DATE,
  type          VARCHAR(50),
  episodes      INT,
  image_url     VARCHAR(500)
) ENGINE=InnoDB;

CREATE INDEX idx_anime_score ON anime_dim(score);
CREATE INDEX idx_anime_members ON anime_dim(members);

CREATE TABLE genre_dim (
  genre_id   INT AUTO_INCREMENT PRIMARY KEY,
  genre_name VARCHAR(100) NOT NULL UNIQUE
) ENGINE=InnoDB;

CREATE TABLE anime_genre_map (
  anime_id INT NOT NULL,
  genre_id INT NOT NULL,
  PRIMARY KEY (anime_id, genre_id),
  FOREIGN KEY (anime_id) REFERENCES anime_dim(anime_id),
  FOREIGN KEY (genre_id) REFERENCES genre_dim(genre_id)
) ENGINE=InnoDB;

CREATE TABLE company (
  company_id   INT PRIMARY KEY,
  entity_type  VARCHAR(30),
  name         VARCHAR(255),
  image_url    VARCHAR(500)
) ENGINE=InnoDB;

CREATE TABLE anime_company (
  anime_id    INT NOT NULL,
  company_id  INT NOT NULL,
  role        VARCHAR(50),
  PRIMARY KEY (anime_id, company_id, role),
  FOREIGN KEY (anime_id) REFERENCES anime_dim(anime_id),
  FOREIGN KEY (company_id) REFERENCES company(company_id)
) ENGINE=InnoDB;

CREATE TABLE character_entity (
  character_id INT PRIMARY KEY,
  name         VARCHAR(255),
  image_url    VARCHAR(500)
) ENGINE=InnoDB;

CREATE TABLE anime_character (
  anime_id     INT NOT NULL,
  character_id INT NOT NULL,
  role         VARCHAR(50) NULL,
  PRIMARY KEY (anime_id, character_id),
  FOREIGN KEY (anime_id) REFERENCES anime_dim(anime_id),
  FOREIGN KEY (character_id) REFERENCES character_entity(character_id)
) ENGINE=InnoDB;

CREATE TABLE voice_actor (
  person_id    INT PRIMARY KEY,
  name         VARCHAR(255),
  image_url    VARCHAR(500)
) ENGINE=InnoDB;

CREATE TABLE anime_voice_actor (
  anime_id     INT NOT NULL,
  character_id INT NOT NULL,
  person_id    INT NOT NULL,
  language     VARCHAR(50),
  PRIMARY KEY (anime_id, character_id, person_id, language),
  FOREIGN KEY (anime_id) REFERENCES anime_dim(anime_id),
  FOREIGN KEY (character_id) REFERENCES character_entity(character_id),
  FOREIGN KEY (person_id) REFERENCES voice_actor(person_id)
) ENGINE=InnoDB;