#!/usr/bin/env python3


from PIL import Image, ImageDraw
import datetime
import re

import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="place",
    user="postgres",
    password="mysecretpassword"
)

users = set()
data = []
counter = 0
cur = conn.cursor()

on_date = datetime.datetime.now()

print("Fetching max_x")
cur.execute("""
    SELECT coord_x
    FROM points
    WHERE time <= %s AND coord_x > 999
    LIMIT 1
""", (on_date, ))
max_x = 2000 if cur.fetchone() else 1000

print("Fetching max_y")
cur.execute("""
    SELECT coord_y
    FROM points
    WHERE time <= %s AND coord_y > 999
    LIMIT 1
""", (on_date, ))
max_y = 2000 if cur.fetchone() else 1000

# size of image
canvas = (max_x, max_y)

print("Starting draw")
# init canvas
im = Image.new('RGB', canvas, (255, 255, 255))
draw = ImageDraw.Draw(im)

used = set()

curr_time = on_date

total_points = max_x * max_y

x = 0
y = 0

BLOCK_SIZE = 250

# Fetch latest points in blocks of 100x100
while x < max_x:
    while y < max_y:
        print("")
        print("Fetching ({}, {})...".format(x, y), end="")
        cur.execute("""
        select distinct on (coord_x, coord_y) coord_x, coord_y, color
        from points
        WHERE coord_x >= %(min_x)s AND coord_x < %(max_x)s AND coord_y >= %(min_y)s AND coord_y < %(max_y)s
        order by coord_x, coord_y, time desc;
        """, {
            "min_x": x,
            "max_x": x + BLOCK_SIZE,
            "min_y": y,
            "max_y": y + BLOCK_SIZE,
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