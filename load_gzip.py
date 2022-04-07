import gzip
import sys
import csv
from dateutil import parser
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="place",
    user="postgres",
    password="mysecretpassword"
)

filename = sys.argv[1]

users = set()
data = []
counter = 0
cur = conn.cursor()

with gzip.open(filename, mode='rt', compresslevel=9, encoding=None, errors=None, newline=None) as f:
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    first = True
    for row in spamreader:
        if first:
            first = False
            continue
        user = row[1]
        cur.execute("""
            INSERT INTO place_users(user_name)
            VALUES (%s) ON CONFLICT DO NOTHING;
        """, (user, ))
        counter += 1
        if counter % 1000 == 0:
            conn.commit()
            print(counter)

        if counter > 10000:
            conn.commit()
            break

counter = 0

with gzip.open(filename, mode='rt', compresslevel=9, encoding=None, errors=None, newline=None) as f:
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    first = True
    for row in spamreader:
        if first:
            first = False
            continue
        user = row[1]
        users.add(user)
        _data = {
            "date": parser.parse(row[0]),
            "user": user,
            "color": row[2],
            "x": int(row[3].split(',')[0]),
            "y": int(row[3].split(',')[1]),
        }
        data.append(_data)

        cur.execute("""
            INSERT INTO points (time, coord_x, coord_y, color, user_id)
            VALUES (%(date)s, %(x)s, %(y)s, %(color)s, (SELECT user_id FROM place_users WHERE user_name = %(user)s))
        """, _data)

        counter += 1
        if counter % 1000 == 0:
            conn.commit()
            print(counter)

        if counter > 10000:
            conn.commit()
            break


print("Users:", len(users))
print("Data:", len(data))