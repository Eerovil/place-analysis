#!/usr/bin/env python3

import gzip
import datetime
from io import TextIOWrapper
import re

import time

from urllib.request import urlopen


import db
conn = db.get_conn()

file_users = set()

data = []
users = []
counter = 0
cur = conn.cursor()
conn.autocommit = False

def _parse_rows(rows, user_ids=None):
    """
    date is in format:
        2022-04-04 01:03:24.691 UTC
    we want to get milliseconds after
        2022-04-01 12:43:47.221000 UTC
    and store that to db

    return (time, coord_x, coord_y, color, user_id)
    """
    _points = []
    pk_set = set()
    for _row in rows:
        _date_str = " ".join(_row[0].split(' ')[:-1])
        res = re.search(r'\d{4}-\d{2}-(\d{2}) (\d{2}):(\d{2}):(\d{2})(\.\d+)?.*', _date_str)
        date_parts = {
            'day': int(res.group(1)),
            'hour': int(res.group(2)),
            'minute': int(res.group(3)),
            'second': int(res.group(4)),
            'millisecond': int((res.group(5) or '0').replace('.', ''))
        }
        after_beginning = (
            (date_parts['day'] - 1) * 24 * 60 * 60 * 1000 +
            (date_parts['hour'] - 12) * 60 * 60 * 1000 +
            (date_parts['minute'] - 43) * 60 * 1000 +
            (date_parts['second'] - 47) * 1000 +
            (date_parts['millisecond'] - 221)
        )

        # Convert #FFFFFF string to integer
        color_int = int(_row[2][1:], 16)

        _values = (
            after_beginning,
            int(_row[3].split(',')[0]),
            int(_row[3].split(',')[1]),
            color_int,
            user_ids[_row[1]] if user_ids else None,
        )

        _pk_str = "{}_{}_{}_{}".format(_values[0], _values[1], _values[2], _values[4])
        if _pk_str in pk_set:
            # print("Duplicate: {}".format(_pk_str))
            continue
        pk_set.add(_pk_str)

        _points.append(_values)
    return _points

def _check_exists(rows):
    rows = _parse_rows([rows[0], rows[-1]])
    cur.execute("""
        SELECT COUNT(*)
        FROM points
        WHERE time = %s AND coord_x = %s AND coord_y = %s OR time = %s AND coord_x = %s AND coord_y = %s
    """, (rows[0][0], rows[0][1], rows[0][2], rows[1][0], rows[1][1], rows[1][2]))

    return cur.fetchone() == (2,)

def _read_users(rows):

    _users = []
    for row in rows:
        _users.append((row[1], ))

    print("Inserting {} users...".format(len(_users)))

    cur.execute("""
        CREATE TEMP TABLE tmp_table (
            user_name VARCHAR(90) NOT NULL
        ) ON COMMIT DROP;
    """)

    with cur.copy("""COPY tmp_table (user_name) FROM STDIN;""") as copy:
        for _user in _users:
            copy.write_row(_user)

    cur.execute("""
        INSERT INTO place_users (user_name)
        SELECT user_name
        FROM tmp_table
        ON CONFLICT DO NOTHING;
    """)

    # Select users from place_users if they exist in tmp_table
    cur.execute("""
        SELECT (user_name, user_id)
        FROM place_users
        WHERE user_name IN (SELECT user_name FROM tmp_table);
    """)
    user_ids = {row[0][0]: row[0][1] for row in cur.fetchall()}

    cur.execute("""
        DROP TABLE tmp_table;
    """)
    conn.commit()

    print("Inserted {} users".format(len(user_ids)))
    return user_ids


def _read_points(rows, user_ids):
    # Get related users

    _points = _parse_rows(rows, user_ids=user_ids)
    print("Inserting {} points...".format(len(_points)))
    start = time.time()
    try:
        with cur.copy("""COPY points (time, coord_x, coord_y, color, user_id) FROM STDIN;""") as copy:
            for _point in _points:
                copy.write_row(_point)

    except Exception as e:
        print("Failed to copy, using ON CONFLICT DO NOTHING")
        print(e)
        conn.rollback()
    

        cur.execute("""
            CREATE TEMP TABLE tmp_table 
            (LIKE points INCLUDING DEFAULTS)
            ON COMMIT DROP;
        """)

        with cur.copy("""COPY tmp_table (time, coord_x, coord_y, color, user_id) FROM STDIN;""") as copy:
            for _point in _points:
                copy.write_row(_point)
        
        cur.execute("""
            INSERT INTO points
            SELECT *
            FROM tmp_table
            ON CONFLICT DO NOTHING;
        """)

    end = time.time()
    print("Inserted {} points, {} per second".format(len(_points), len(_points) / (end - start)))

    # db.create_indexes(conn)
    conn.commit()


import sys
import os
offset = int(sys.argv[1]) if len(sys.argv) > 1 else 0

for i in range(offset, 77):
    zeropadded = str(i).zfill(12)
    filename = "2022_place_canvas_history-{}.csv.gzip".format(zeropadded)

    next_filename = "2022_place_canvas_history-{}.csv.gzip".format(str(i + 1).zfill(12))
    if i != 77 and not os.path.exists(next_filename):
        print("missing {}".format(next_filename))
        break

    print("Streaming {}".format(filename))

    with open(filename, 'rb') as gzip_file:
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
            if len(lines_batch) == 500000:
                print("Reading lines...")
                if _check_exists(lines_batch):
                    print("Skipping, exists")
                    lines_batch = []
                    continue
                user_ids = _read_users(lines_batch)
                _read_points(lines_batch, user_ids)
                lines_batch = []

        print("Reading lines...")
        if _check_exists(lines_batch):
            print("Skipping, exists")
            lines_batch = []
            continue
        user_ids = _read_users(lines_batch)
        _read_points(lines_batch, user_ids)

    print("Done")