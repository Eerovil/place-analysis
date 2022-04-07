#!/usr/bin/env python3

import gzip
import sys
import csv

import psycopg2
import psycopg2.extras

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="place",
    user="postgres",
    password="mysecretpassword"
)

filename = sys.argv[1]
print("Loading data from {}".format(filename))
file_users = set()

data = []
users = []
counter = 0
cur = conn.cursor()
conn.autocommit = False


cur.execute("""
    SELECT user_name, user_id
    FROM place_users
""")
all_users = {}
for _users_row in cur.fetchall():
    all_users[_users_row[0]] = _users_row[1]

print("all_users: {}".format(len(all_users)))

def commit_users(_users):
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO place_users(user_name)
        VALUES (%s) ON CONFLICT DO NOTHING;
    """, _users)

with gzip.open(filename, mode='rt', compresslevel=9, encoding=None, errors=None, newline=None) as f:
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    first = True
    for row in spamreader:
        if first:
            first = False
            continue
        file_users.add(row[1])
        if row[1] in all_users:
            continue
        users.append((row[1], ))

        counter += 1
        if counter % 5000 == 0:
            commit_users(users)
            users = []
            print(counter)

        # if counter > 10000:
        #     conn.commit()
        #     break

commit_users(users)
print("done: counter: {}".format(counter))
users = []

if counter > 0:
    cur.execute("""
        SELECT user_name, user_id
        FROM place_users
    """)
    all_users = {}
    for _users_row in cur.fetchall():
        all_users[_users_row[0]] = _users_row[1]

print("all_users: {}".format(len(all_users)))
print("Fetching existing points...")

user_ids = tuple(all_users[_user] for _user in file_users)
user_timestamps = set()
cur.execute("""
    SELECT user_id, time
    FROM points
    WHERE user_id IN %s
""", (user_ids, ))
for _points_row in cur.fetchall():
    _date_str = str(_points_row[1])
    if '.' not in _date_str:
        _date_str += '.000000'
    if len(_date_str) < 26:
        _date_str += '0' * (26 - len(_date_str))
    user_timestamps.add("{}_{}".format(_points_row[0], _date_str))

print("done: user_timestamps: {}".format(len(user_timestamps)))

def commit_points(_points):
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO points (time, coord_x, coord_y, color, user_id)
        VALUES (%(date)s, %(x)s, %(y)s, %(color)s, %(user)s)
        ON CONFLICT DO NOTHING;
    """, _points)

counter = 0
skipped = 0
data = []
# import pprofile
# profiler = pprofile.Profile()
# with profiler:
with gzip.open(filename, mode='rt', compresslevel=9, encoding=None, errors=None, newline=None) as f:
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    first = True
    for row in spamreader:
        if first:
            first = False
            continue
        user = row[1]
        #format date, e.g. 2022-04-01 15:38:01.116 UTC
        #2022-04-03 17:43:14.599000
        _date_str = " ".join(row[0].split(' ')[:-1])
        if '.' not in _date_str:
            _date_str += '.000000'
        if len(_date_str) < 26:
            _date_str += '0' * (26 - len(_date_str))
        
        _user_timestamp = "{}_{}".format(all_users[user], _date_str)
        if _user_timestamp in user_timestamps:
            skipped += 1
            continue

        print("user_timestamp: {}".format(_user_timestamp))
        
        # date = datetime.datetime.strptime(_date_str, "%Y-%m-%d %H:%M:%S.%f")
        _data = {
            "date": _date_str,
            "user": all_users[user],
            "color": row[2],
            "x": int(row[3].split(',')[0]),
            "y": int(row[3].split(',')[1]),
        }
        data.append(_data)

        counter += 1
        if counter % 10000 == 0:
            commit_points(data)
            data = []
            print(counter)

commit_points(data)

# profiler.dump_stats('profiler_dump.txt')

conn.commit()

print("done: {}, skipped: {}".format(counter, skipped))
