import pymysql
import argparse
import json
import datetime


parser = argparse.ArgumentParser()
parser.add_argument("--file", help="SQL File", nargs='?', type=str)
parser.add_argument("--password", help="password", nargs='?', type=str)
parser.add_argument("host", help="host name", nargs='?', type=str, default="denuri-db.cw5cwsrwvj2x.ap-northeast-2.rds.amazonaws.com")
parser.add_argument("db", help="database name", nargs='?', type=str, default="denuri_db")
parser.add_argument("user", help="user name", nargs='?', type=str, default="admin")
parser.add_argument("charset", help="charset", nargs='?', type=str, default="utf8mb4")
parser.add_argument("output", help="output filename", nargs='?', type=str, default="output.jsl")
args = parser.parse_args()

def json_encoder(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

# Connect to the database
connection = pymysql.connect(host=args.host,
                             user=args.user,
                             password=args.password,
                             db=args.db,
                             charset=args.charset,
                             cursorclass=pymysql.cursors.DictCursor)

with open(args.file) as f:
    sql = f.read()
try:
    with connection.cursor() as cursor:
        cursor.execute(sql)

        with open(args.output, "wt") as outf:
            i = 0
            while True:
                result = cursor.fetchone()
                if not result:
                    print(f"Fetched {i} rows")
                    break

                i = i + 1
                if i % 1000 == 0:
                    print(f"Fetching {i} rows...")

                outf.write(json.dumps(result, default=json_encoder))
                outf.write('\n')

finally:
    connection.close()