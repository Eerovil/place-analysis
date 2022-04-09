#!/usr/bin/env python3


from PIL import Image, ImageDraw
import datetime
import os
import json
from dateutil import parser as dateparser
import argparse

import psycopg

conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="place",
    user="postgres",
    password="mysecretpassword"
)

parser = argparse.ArgumentParser(description="Build png on given hour:minute",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--correct_time", help="Timestamp when image is correct (unix ms)", default=None)
args = parser.parse_args()
config = vars(args)

users = set()
data = []
counter = 0
cur = conn.cursor()

user_filter = None
if os.path.exists('users.json'):
    with open('users.json', 'r') as f:
        user_filter = set(json.load(f))

if not config.get('correct_time'):
    config['correct_time'] = '1649112287221'  # Start of white pixels only

first_date = datetime.datetime.fromtimestamp(1648817027.221)
selected_date = datetime.datetime.fromtimestamp(int(config['correct_time']) / 1000.0)


print("Fetching max_x")
cur.execute("""
    SELECT coord_x
    FROM points
    WHERE time <= %s AND coord_x > 999
    LIMIT 1
""", (selected_date, ))
max_x = 2000 if cur.fetchone() else 1000

print("Fetching max_y")
cur.execute("""
    SELECT coord_y
    FROM points
    WHERE time <= %s AND coord_y > 999
    LIMIT 1
""", (selected_date, ))
max_y = 2000 if cur.fetchone() else 1000

# size of image
canvas = (max_x, max_y)

print("Starting draw")
# init canvas
im = Image.new('RGB', canvas, (255, 255, 255))
draw = ImageDraw.Draw(im)

used = set()

total_points = max_x * max_y

x = 0
y = 0

BLOCK_SIZE = 250

# Fetch latest points in blocks of 100x100
while x < max_x:
    while y < max_y:
        print("")
        print("Fetching ({}, {})...".format(x, y), end="")
        if not user_filter:
            cur.execute("""
            select distinct on (coord_x, coord_y) coord_x, coord_y, color
            from points
            WHERE coord_x >= %(min_x)s AND coord_x < %(max_x)s AND coord_y >= %(min_y)s AND coord_y < %(max_y)s AND time <= %(selected_date)s
            order by coord_x, coord_y, time desc;
            """, {
                "min_x": x,
                "max_x": x + BLOCK_SIZE,
                "min_y": y,
                "max_y": y + BLOCK_SIZE,
                "selected_date": selected_date,
            })
        else:
            cur.execute("""
            select distinct on (coord_x, coord_y) coord_x, coord_y, color
            from points
            WHERE coord_x >= %(min_x)s AND coord_x < %(max_x)s AND coord_y >= %(min_y)s AND coord_y < %(max_y)s AND time <= %(selected_date)s
            AND user_id in %(user_filter)s
            order by coord_x, coord_y, time desc;
            """, {
                "min_x": x,
                "max_x": x + BLOCK_SIZE,
                "min_y": y,
                "max_y": y + BLOCK_SIZE,
                "user_filter": tuple(user_filter),
                "selected_date": selected_date,
            })

        print("done.", end="")


        found = False
        counter = 0

        for row in cur:
            found = True
            point_x = row[0]
            point_y = row[1]
            used_len = len(used)
            counter += 1
            if counter % 10000 == 0:
                print(".", end="")

            if used_len >= total_points:
                print("1 used_len >= total_points")
                break
            if (point_x, point_y) in used:
                continue
            used.add((point_x, point_y))
            # color = int(rgb2short(row[2].lower())[0])
            color = row[2]
            draw.point((point_x, point_y), fill=color)
            if used_len % 100000 == 0:
                print("{}% points drawn".format(used_len / total_points * 100))

        y += BLOCK_SIZE

        im.save('im.png')

        if used_len >= total_points:
            print("2 used_len >= total_points")
            break

    x += BLOCK_SIZE
    y = 0

# save image
im.save('im.png')