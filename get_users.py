#!/usr/bin/env python3

import psycopg
import argparse
import datetime
from dateutil import parser as dateparser

# Star wars poster
# ./get_users.py -r573,701,668,841

# Original finnish flag
# ./get_users.py -r288,961,435,999 --correct_time 1648851707221

import db

conn = db.get_conn()
cur = conn.cursor()

parser = argparse.ArgumentParser(description="Print all uers who contributed to image at given points",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-p', action='append', nargs='+', help="Point (e.g. 420,420)")
parser.add_argument('-r', action='append', nargs='+', help="Rectangle (e.g. 69,69,420,420)")
parser.add_argument("--correct_time", help="Timestamp when image is correct (unix)", default=None)
args = parser.parse_args()
config = vars(args)
print(config)


points = {}
for _point_str in (config.get('p', [[]]) or [[]]):
    if not _point_str:
        continue
    points[(int(_point_str[0].split(',')[0]), int(_point_str[0].split(',')[1]))] = None

for _rect_str in (config.get('r', [[]]) or [[]]):
    if not _rect_str:
        continue
    top_left = (int(_rect_str[0].split(',')[0]), int(_rect_str[0].split(',')[1]))
    bottom_right = (int(_rect_str[0].split(',')[2]), int(_rect_str[0].split(',')[3]))
    for x in range(top_left[0], bottom_right[0]):
        for y in range(top_left[1], bottom_right[1]):
            points[(x, y)] = None

# Get correct color for each point

if not config.get('correct_time'):
    config['correct_time'] = '1649112287221'  # Start of white pixels only

first_date = datetime.datetime.fromtimestamp(1648817027.221)
selected_date = datetime.datetime.fromtimestamp(int(config['correct_time']) / 1000.0)

for point in points.keys():
    cur.execute("""
        SELECT color
        FROM points
        WHERE coord_x = %s AND coord_y = %s AND time <= %s
        ORDER BY time DESC
        LIMIT 1
    """, (point[0], point[1], selected_date))
    try:
        points[point] = cur.fetchone()[0]
    except TypeError:
        print("No color for point {}".format(point))
        points[point] = '#000000'

print(points)

# Get all users who set the right color for each point
all_users = set()
for point, color in points.items():
    cur.execute("""
        SELECT user_id
        FROM points
        WHERE coord_x = %s AND coord_y = %s AND color = %s
    """, (point[0], point[1], color))
    for user_id in cur.fetchall():
        all_users.add(user_id[0])


print(all_users)

import json
with open('users.json', 'w') as f:
    json.dump(list(all_users), f)
