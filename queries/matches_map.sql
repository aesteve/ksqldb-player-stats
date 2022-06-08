CREATE STREAM MATCHES_MAP (
    match_id INT KEY,
    home_team_id STRING,
    away_team_id STRING,
    score STRING,
    players MAP<STRING, STRUCT<goals INT, shots INT>>
)
WITH (
    KAFKA_TOPIC = 'matches_map',
    VALUE_FORMAT = 'JSON'
);


-- WIP, we don't seem to have an `EXPLODE` equivalent