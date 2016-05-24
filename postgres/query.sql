CREATE TABLE players (player_id INT, last_name varchar, jersey int, first_name VARCHAR, position VARCHAR, team_id int);
CREATE TABLE locations (game_id int, quarter int, unix_time VARCHAR,

game_clock FLOAT, shot_clock FLOAT, team_id INT, player_id INT, x FLOAT, y FLOAT, z FLOAT);

COPY players FROM '~/players/all_players.csv' DELIMITER ',' CSV; COPY locations FROM '~/all_moments.csv' DELIMITER ',' CSV;
SELECT * FROM (
SELECT game_id, LHS.player_id, TEXTCAT(TEXTCAT(first_name, ' '), last_name) AS name, position, total_distance FROM (SELECT game_id, player_id, SUM(distance) total_distance FROM
(SELECT player_id, game_id, unix_time, SQRT(POW(x_distance, 2) + POW(y_distance, 2))*0.000189394 distance FROM
(SELECT player_id, game_id, unix_time, x, y, x - lag(x) OVER (PARTITION BY game_id, player_id ORDER BY unix_time) x_distance, y - lag(y) OVER (PARTITION BY game_id, player_id ORDER BY unix_time) y_distance FROM raw_locations) LHS) LHS2
GROUP BY player_id, game_id) LHS LEFT JOIN raw_players RHS ON LHS.player_id = RHS.player_id) AGG ORDER BY total_distance DESC LIMIT 25;
