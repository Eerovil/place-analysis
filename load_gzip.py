#!/usr/bin/env python3

import gzip
import datetime
from io import TextIOWrapper

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

def _parse_rows(rows):
    _points = []
    pk_set = set()
    for _row in rows:
        _date_str = " ".join(_row[0].split(' ')[:-1])
        if '.' not in _date_str:
            _date_str += '.000000'
        if len(_date_str) < 26:
            _date_str += '0' * (26 - len(_date_str))
        
        _values = (
            1000000, #_date_str,
            int(_row[3].split(',')[0]),
            int(_row[3].split(',')[1]),
            _row[2],
            _row[1],
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
        WHERE time = %s AND user_name = %s AND coord_x = %s AND coord_y = %s OR time = %s AND user_name = %s AND coord_x = %s AND coord_y = %s
    """, (rows[0][0], rows[0][4], rows[0][1], rows[0][2], rows[1][0], rows[1][4], rows[1][1], rows[1][2]))

    return cur.fetchone() == (2,)

def _read_points(rows, block_users={}):
    # Get related users

    _points = _parse_rows(rows)
    print("Inserting {} points...".format(len(_points)))
    start = time.time()
    try:
        with cur.copy("""COPY points (time, coord_x, coord_y, color, user_name) FROM STDIN;""") as copy:
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

        with cur.copy("""COPY tmp_table (time, coord_x, coord_y, color, user_name) FROM STDIN;""") as copy:
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
db.drop_indexes(conn)

for i in range(offset, 77):
    zeropadded = str(i).zfill(12)
    filename = "2022_place_canvas_history-{}.csv.gzip".format(zeropadded)

    next_filename = "2022_place_canvas_history-{}.csv.gzip".format(str(i + 1).zfill(12))
    if i != 77 and not os.path.exists(next_filename):
        print("missing {}".format(next_filename))
        break

    print("Opening {}".format(filename))

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
            if len(lines_batch) == 10000000:
                print("Reading lines...")
                if _check_exists(lines_batch):
                    print("Skipping, exists")
                    lines_batch = []
                    continue
                _read_points(lines_batch)
                lines_batch = []

        print("Reading lines...")
        if _check_exists(lines_batch):
            print("Skipping, exists")
            lines_batch = []
            continue
        _read_points(lines_batch)

