from PIL import Image, ImageDraw
import datetime

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
conn.autocommit = True

on_date = datetime.datetime.now()

cur.execute("""
    SELECT MAX(coord_x) as max_x
    FROM points
    WHERE time <= %s
""", (on_date, ))
max_x = cur.fetchone()[0]

cur.execute("""
    SELECT MAX(coord_y) as max_y
    FROM points
    WHERE time <= %s
""", (on_date, ))
max_y = cur.fetchone()[0]

if max_x <= 1000:
    max_x = 1000
else:
    max_x = 2000

if max_y <= 1000:
    max_y = 1000
else:
    max_y = 2000

# size of image
canvas = (max_x, max_y)

cur.execute("""
    SELECT coord_x, coord_y, color
    FROM points
    WHERE time <= %s
    ORDER BY time DESC
""", (on_date, ))
points = cur.fetchall()


# init canvas
im = Image.new('RGBA', canvas, (255, 255, 255, 255))
draw = ImageDraw.Draw(im)

used = set()

for row in points:
    x = row[0]
    y = row[1]
    if (x, y) in used:
        continue
    used.add((x, y))
    color = row[2]
    draw.point((x, y), fill=color)

# save image
im.save('im.png')