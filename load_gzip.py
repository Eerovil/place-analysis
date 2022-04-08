#!/usr/bin/env python3

import gzip
import csv
from io import TextIOWrapper

from urllib.request import urlopen

import psycopg2
import psycopg2.extras
from psycopg2.errors import NotNullViolation

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="place",
    user="postgres",
    password="mysecretpassword"
)

file_users = set()

data = []
users = []
counter = 0
cur = conn.cursor()
conn.autocommit = True

def _parse_row(row):
    _date_str = " ".join(row[0].split(' ')[:-1])
    if '.' not in _date_str:
        _date_str += '.000000'
    if len(_date_str) < 26:
        _date_str += '0' * (26 - len(_date_str))
    
    return {
        'date': _date_str,
        'user': row[1],
        'color': row[2],
        'x': int(row[3].split(',')[0]),
        'y': int(row[3].split(',')[1]),
    }


def _read_users(rows, force=False):
    if not force:
        first = rows[0][1]
        last = rows[-1][1]

        # Check if first and last exist already in database
        cur.execute("""
            SELECT user_name
            FROM place_users
            WHERE user_name = %s OR user_name = %s
        """, (first, last))
        existing = cur.fetchall()
        if len(existing) == 2:
            print("Users {} and {} already exist in database".format(first, last))
            return

    _users = [(row[1], ) for row in rows]

    print("Inserting {} users...".format(len(_users)))
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO place_users(user_name)
        VALUES (%s) ON CONFLICT DO NOTHING;
    """, _users)


def _read_points(rows, force=False):
    if not force:
        # Check if first and last exist already in database
        first = _parse_row(rows[0])
        last = _parse_row(rows[-1])

        cur.execute("""
            SELECT time, coord_x, coord_y
            FROM points
            WHERE time = %s AND coord_x = %s AND coord_y = %s
        """, (first['date'], first['x'], first['y']))
        existing = cur.fetchall()
        if len(existing) == 1:
            print("Point {} (first) already exists in database".format(first))
            # Check last point
            cur.execute("""
                SELECT time, coord_x, coord_y
                FROM points
                WHERE time = %s AND coord_x = %s AND coord_y = %s
            """, (last['date'], last['x'], last['y']))
            existing = cur.fetchall()
            if len(existing) == 1:
                print("Point {} (last) already exists in database".format(last))
                return

    _points = [_parse_row(row) for row in rows]

    print("Inserting {} points...".format(len(_points)))
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO points (time, coord_x, coord_y, color, user_id)
        VALUES (%(date)s, %(x)s, %(y)s, %(color)s, (SELECT user_id FROM place_users WHERE user_name = %(user)s))
        ON CONFLICT DO NOTHING;
    """, _points)


import sys
offset = int(sys.argv[1]) if len(sys.argv) > 1 else 0

for i in range(offset, 77):
    zeropadded = str(i).zfill(12)
    filename = "2022_place_canvas_history-{}.csv.gzip".format(zeropadded)
    url = "https://placedata.reddit.com/data/canvas-history/{}".format(filename)

    print("Streaming {}".format(filename))

    with urlopen(url) as gzip_file:
        fd = gzip.GzipFile(fileobj=gzip_file, mode="r")

        lines_batch = []
        first = True
        for line in TextIOWrapper(fd, newline=''):
            if first:
                first = False
                continue
            comma_split = line.split(',')
            # sample line
            # 2022-04-04 01:03:24.691 UTC,BIKKEGyB9ZLqD0zHF2wpM9wnHCor8mfV59jYWDqKgKjV1DDyDcR156082NAF7/lIAoYJQxbgQEelAAlbDA3evw==,#000000,"165,1507"
            parsed_line = (
                comma_split[0],
                comma_split[1],
                comma_split[2],
                ",".join(comma_split[3:]).replace('"', ''),
            )
            lines_batch.append(parsed_line)
            if len(lines_batch) == 100000:
                _read_users(lines_batch)
                try:
                    _read_points(lines_batch)
                except NotNullViolation:
                    print("Got a NotNullViolation, forcing user re-import")
                    conn.rollback()
                    _read_users(lines_batch, force=True)
                    _read_points(lines_batch, force=True)

                lines_batch = []
            