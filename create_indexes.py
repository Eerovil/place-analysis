import db

conn = db.get_conn()

db.create_indexes(conn)
