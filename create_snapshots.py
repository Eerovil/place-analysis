#!/usr/bin/env python3

"""
For a given timestamp, store all latest points.
"""

import argparse
import datetime

import snapshots
import db
conn = db.get_conn()
cur = conn.cursor()

parser = argparse.ArgumentParser(description="Create a snapshot for given timestamp",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-t", "--correct_time", help="ms", default=None)
args = parser.parse_args()
config = vars(args)
print(config)

if config['correct_time']:
    timestamp = int(config['correct_time']) / 1000.0
else:
    timestamp = None

assert timestamp

# get a possible starting point
snapshots.init_snapshots_table(conn)
snapshot = snapshots.get_points(conn, timestamp)


earliest_time = 0
points = {}
if snapshot:
    earliest_time = snapshot[0]
    for _point in snapshot[1]:
        x, y, color, user_name = _point
        points[(x, y)] = {
            'color': color,
            'user_name': user_name,
        }

# Start from timestamp and go back in time to "earliest_time"
# and store all points in "points"

cur.execute("""
    select distinct on (coord_x, coord_y) coord_x, coord_y, color
    from points
    WHERE time <= %(selected_date)s AND time >= %(earliest_time)s
    order by coord_x, coord_y, time desc;
    """, {
        "selected_date": timestamp,
        "earliest_time": earliest_time,
    })

