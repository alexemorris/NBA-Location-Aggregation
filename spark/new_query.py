from pyspark.sql.types import *
players_raw = sc.textFile("s3://dcproject/2014-10-29/players/*.csv").repartition(100)

def parse_players(line):
	fields = line.split(",")
	player_id = int(fields[0])
	team = int(fields[5])
	player = fields[3] + " " + fields[1] position = str(fields[4])
	jersey = int(fields[2])
	return player_id,team,player,position,jersey

print players_raw.take(2)
players_info = players_raw.map(parse_players)
fields = [("player_id", IntegerType()), ("team_id", IntegerType()), ("player_name", StringType()), ("position",StringType()), ("jersey", IntegerType())]
player_schema = StructType([StructField(x[0], x[1], True) for x in fields])
schema_PlayersInfo = sqlc.createDataFrame(players_info, player_schema)
sqlc.registerDataFrameAsTable(schema_PlayersInfo, "raw_players")
locations_raw = sc.textFile("s3://dcproject/2014-10-29/moments/*.csv.gz").repartition(100)
def parse_locations(line): 
	fields = line.split(",")
	game_id = int(fields[0])
	quarter = int(fields[1])
	unix_time = int(fields[2]) 
	try:
		game_clock = float(fields[3]) 
	except:
		game_clock = 0.0
	try:
		shot_clock = float(fields[4]) 
	except:
		shot_clock = 0.0
	team_id = int(fields[5]) 
	player_id = int(fields[6])
	try:
		x = float(fields[7])
	except:
		x = 0.0
	try:
		y = float(fields[8])
	except:
		y = 0.0
	try:
		z = float(fields[9])
	except:
		z = 0.0
	return game_id, quarter, unix_time, game_clock, shot_clock, team_id, player_id, x, y, z
locations_raw = locations_raw.map(parse_locations)

types = [("game_id", IntegerType()), ("quarter", IntegerType()), ("unix_time",IntegerType()), ("game_clock", FloatType()), ("shot_clock", DoubleType()), ("team_id",IntegerType()), ("player_id", IntegerType()), ("x", DoubleType()), ("y", DoubleType()), ("z", DoubleType())]

location_schema = StructType([StructField(x[0], x[1], True) for x in types])
schema_LocationInfo = sqlc.createDataFrame(locations_raw, location_schema)
sqlc.registerDataFrameAsTable(schema_LocationInfo, "raw_locations")

y = sc.parallelize([float(x) for x in range(51)])
x = sc.parallelize([float(x) for x in range(95)])

fields = [StructField("x", DoubleType(), True), StructField("y", DoubleType(), True)] null_location_schema = sqlc.createDataFrame(x.cartesian(y), StructType(fields)) 

sqlc.registerDataFrameAsTable(null_location_schema, "court")

query = """
SELECT * FROM (
SELECT game_id, LHS.player_id, CONCAT(first_name, " ", last_name), position, total_distance FROM (SELECT game_id, player_id, SUM(distance) total_distance FROM
(SELECT player_id, game_id, unix_time, SQRT(POW(x_distance, 2) + POW(y_distance, 2))*0.000189394 distance FROM
(SELECT player_id, game_id, unix_time, x, y, x - lag(x) OVER (PARTITION BY game_id, player_id ORDER BY unix_time) x_distance, y - lag(y) OVER (PARTITION BY game_id, player_id ORDER BY unix_time) y_distance FROM raw_locations) LHS) LHS2
GROUP BY player_id, game_id) LHS LEFT JOIN raw_players RHS ON LHS.player_id = RHS.player_id) AGG ORDER BY total_distance DESC LIMIT 25
"""
print sqlc.sql(query).take(25)