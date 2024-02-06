# Analysis imports
import re
from collections import defaultdict

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, MapType, IntegerType
import pyspark.sql.functions as F

# Create a Spark session
spark = SparkSession.builder.appName("GetCaptures").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Start positions
default = {"a2": "P", "b2": "P", "c2": "P", "d2": "P", "e2": "P", "f2": "P", "g2": "P", "h2": "P",
           "a1": "R", "b1": "N", "c1": "B", "d1": "Q", "e1": "K", "f1": "B", "g1": "N", "h1": "R",
           "a7": "P", "b7": "P", "c7": "P", "d7": "P", "e7": "P", "f7": "P", "g7": "P", "h7": "P",
           "a8": "R", "b8": "N", "c8": "B", "d8": "Q", "e8": "K", "f8": "B", "g8": "N", "h8": "R",
           }


# Call this function with a game to get analytics
def run(game):
    try:
        # Remove game analytics (time and eval)
        moves = re.sub(r'{[^}]*}', "", game)

        # Split the game into a list of moves
        moves = moves.split()
        moves = [move for move in moves if not move[0].isdigit()]

        res = defaultdict(int)

        # Castling (replace castling with the two executed moves)
        new_moves = []
        for i in range(len(moves)):
            move = moves[i]
            if move == "O-O":
                res['king castle'] += 1
                if i % 2 == 0:
                    new_moves.extend(['Kg1', 'Nhf1'])
                else:
                    new_moves.extend(['Kg8', 'Nhf8'])
            elif move == "O-O-O":
                res['queen castle'] += 1
                if i % 2 == 0:
                    new_moves.extend(['Kc1', 'Nad1'])
                else:
                    new_moves.extend(['Kc8', 'Nad8'])
            else:
                new_moves.append(move)
        moves = new_moves

        # Main loop
        for i, move in enumerate(moves):
            # Piece taken
            if 'x' in move:
                res['capture'] += 1
                killer, location = move.split('x')

                # Remove potential distinction between two equal pieces
                if len(killer) > 1:
                    killer = killer[0]

                # Name all pawns P
                if killer in "abcdefgh":
                    killer = "P"

                # En passant
                if killer == "P" and (location[1] == '3' or location[1] == '6'):
                    if moves[i-1][0] not in "KQRBN":
                        res["en passant"] += 1
                        res["PxP"] += 1
                        continue

                # Go backwards to find last piece moved to this location
                for j in range(i - 1, -1, -1):
                    if location[:2] in moves[j]:
                        killed = moves[j][0] if moves[j][0] in "KQRBN" else 'P'
                        break
                else:
                    # if no piece moved to this location, use the starting position
                    killed = default[location[:2]]

                res[f"{killer}x{killed}"] += 1
        res['games'] += 1
        return dict(res)
    except Exception as e:
        raise NameError(f"gaat kaput {e}, {game}")


df_parsed = spark.read.parquet("/user/s2570408/project/parsed")
get_captured_pieces_udf = F.udf(run, MapType(StringType(), IntegerType()))
df_captures = df_parsed.withColumn("captures", get_captured_pieces_udf(F.col("parsedGame")["Moves"]))

data = df_captures.select(F.col("captures")).rdd

# Aggregate captures over all games
rdd_aggregated = data\
    .flatMap(lambda x: x[0].items())\
    .reduceByKey(lambda a, b: a + b)
print(rdd_aggregated.collect())

df_aggregated = rdd_aggregated.toDF()
df_aggregated.show()

df_aggregated.write.parquet("/user/s2009501/project/result_1", mode="overwrite")
