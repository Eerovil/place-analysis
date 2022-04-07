
import subprocess
import os


for i in range(0, 77):
    zeropadded = str(i).zfill(12)
    filename = "2022_place_canvas_history-{}.csv.gzip".format(zeropadded)
    if os.path.exists(filename):
        print("{} exists".format(filename))
        continue
    url = "https://placedata.reddit.com/data/canvas-history/{}".format(filename)
    subprocess.check_call(["wget", url]) 
