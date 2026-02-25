USE anime_project;

DROP TABLE IF EXISTS entities;

CREATE TABLE entities (
  entity_id    INT PRIMARY KEY,
  entity_type  VARCHAR(50) NOT NULL,
  name         VARCHAR(255) NOT NULL,
  image_url    VARCHAR(500)
) ENGINE=InnoDB;