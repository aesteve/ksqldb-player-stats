-- SAMPLE DATA: 1234;{"match_id": 1234, "home_team_id": "FCB", "away_team_id": "Cel", "score": "3-2", "players": [{"player_id": "Aubameyang", "goals": 2, "shots": 6}, {"player_id": "Depay", "goals": 1, "shots": 4}]}

CREATE STREAM MATCHES_ARRAY (
    match_id INT KEY,
    home_team_id STRING,
    away_team_id STRING,
    score STRING,
    players ARRAY<STRUCT<player_id STRING, goals INT, shots INT>>
)
WITH (
    KAFKA_TOPIC = 'matches_array',
    VALUE_FORMAT = 'JSON'
);

CREATE STREAM PLAYERS_EXPLODED AS
    SELECT
        match_id,
        EXPLODE(players)->player_id     as player_id,
        EXPLODE(players)->goals         as goals,
        EXPLODE(players)->shots         as shots
    FROM MATCHES_ARRAY
    EMIT CHANGES;

CREATE TABLE PLAYERS_STATS AS
    SELECT
        player_id,
        SUM(goals) as scored,
        SUM(shots) as attempts
    FROM
        PLAYERS_EXPLODED
    GROUP BY player_id
    ;

SELECT * FROM PLAYERS_STATS;