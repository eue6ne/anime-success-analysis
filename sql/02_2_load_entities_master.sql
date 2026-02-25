USE anime_project;

INSERT INTO character_entity (character_id, name, image_url)
SELECT entity_id, name, image_url
FROM entities
WHERE entity_type = 'character'
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  image_url = VALUES(image_url);

INSERT INTO voice_actor (person_id, name, image_url)
SELECT entity_id, name, image_url
FROM entities
WHERE entity_type = 'voice_actor'
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  image_url = VALUES(image_url);

INSERT INTO company (company_id, entity_type, name, image_url)
SELECT entity_id, entity_type, name, image_url
FROM entities
WHERE entity_type IN ('studio','producer')
ON DUPLICATE KEY UPDATE
  entity_type = VALUES(entity_type),
  name = VALUES(name),
  image_url = VALUES(image_url);