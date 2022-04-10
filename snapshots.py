

def init_snapshots_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            time integer,
            coord_x smallint,
            coord_y smallint,
            color integer,
            user_id integer
        )
    """)
    conn.commit()


def get_points(conn, timestamp):
    cur = conn.cursor()

    # latest time
    cur.execute("""
        SELECT time FROM snapshots
        WHERE time <= %s
        ORDER BY time DESC
        LIMIT 1
    """, (timestamp, ))

    result = list(cur.fetchone() or [])
    if not result:
        print("No snapshot for {}".format(timestamp))
        return None

    latest_time = result[0]

    # get points for this time in snapshots table

    cur.execute("""
        SELECT (coord_x, coord_y, color, user_id) FROM snapshots
        FROM snapshots
        WHERE time = %s
    """, (latest_time,))

    ret = list(cur.fetchall())

    if len(ret) == 0:
        print("No snapshot for {}".format(timestamp))
        return None

    assert len(ret) == 1000 * 1000 or len(ret) == 1000 * 2000 or len(ret) == 2000 * 2000

    return latest_time, ret
