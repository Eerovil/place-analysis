### /r/place analysis project

This is a project to read the whole datadump to a postgreSQL database.

I want to check out what specific users have been doing:
    * The users who contributed to the star wars image, what else did they create?
    * Other stuff like that.


After loading the data, you can truncate the place_users table. Result should be about 15GB


Steps to build database

1. docker-compose up -d
2. python download_place.py
3. python init_db.py
4. python load_gzip.py
5. ...
6. profit!