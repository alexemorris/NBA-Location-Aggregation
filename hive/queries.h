CREATE EXTERNAL TABLE raw_players (player_id int, last_name STRING, jersey INT, first_name String, position String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION ‘s3://dcproject/all_games/players/';

CREATE EXTERNAL TABLE raw_locations (game_id int, quarter int, unix_time STRING, game_clock FLOAT, shot_clock FLOAT, team_id INT, player_id INT, x FLOAT, y FLOAT, z FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://dcproject/all_games/moments/';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table locations (quarter int, unix_time STRING, game_clock FLOAT, shot_clock FLOAT, team_id INT, player_id INT, x FLOAT, y FLOAT, z FLOAT) PARTITIONED BY (game_id int) row format delimited fields terminated by ",";

INSERT OVERWRITE TABLE locations PARTITION(game_id) SELECT game_id, quarter int, unix_time, game_clock, shot_clock, team_id, player_id, x, y, z FROM raw_locations;

SELECT * FROM (
SELECT game_id, LHS.player_id, CONCAT(first_name, " ", last_name), position, total_distance FROM (SELECT game_id, player_id, SUM(distance) total_distance FROM
(SELECT player_id, game_id, unix_time, SQRT(POW(x_distance, 2) + POW(y_distance, 2))*0.000189394 distance FROM
(SELECT player_id, game_id, unix_time, x, y, x - lag(x) OVER (PARTITION BY game_id, player_id ORDER BY unix_time) x_distance, y - lag(y) OVER (PARTITION BY game_id, player_id ORDER BY unix_time) y_distance FROM locations) LHS) LHS2
GROUP BY player_id, game_id) LHS LEFT JOIN raw_players RHS ON LHS.player_id = RHS.player_id) AGG WHERE player_id <> -1 ORDER BY total_distance DESC LIMIT 50;