import sys
import time
import logging
import h5pyd
 
if len(sys.argv) < 2 or sys.argv[1] in ('-h', '--help'):
    print("usage: python get_station_ids <ghcn_file>")
    sys.exit(0)
filename = sys.argv[1]

logging.basicConfig(level=logging.INFO)
start_time = time.time()
logging.info(f"start_time: {start_time:.2f}")

f = h5pyd.File(filename, mode='r', use_cache=False)
dset = f['data']
station_year_map = {}
cursor = dset.create_cursor()
bad_count = 0
line_count = 0
year_count = 0
previous_year = 0
for row in cursor:
    station_id = row['station_id'].decode('ascii')
    ymd = row['ymd'].decode('ascii')
    line_count += 1
    if len(ymd) != 8:
        # print(f"unexpected ymd: {ymd}")
        bad_count += 1
        continue
    year = int(ymd[:4])  # format YYYYMMDD
    if year != previous_year:
        now = time.time()
        if previous_year:
            elapsed = now-start_time
            msg = f"year: {previous_year} processing time: {elapsed:6.2f} s "
            msg += f"for {year_count} lines - "
            msg += f"lines/sec: {int((year_count/elapsed))}"
            logging.info(msg)
        year_count = 0
        previous_year = year
    year_count += 1
    if year not in station_year_map:
        station_year_map[year] = set()
    station_ids = station_year_map[year]
    station_ids.add(station_id)
     
now = time.time()
logging.info(f"finish time +{(now-start_time):.2f}")
logging.info(f"year_count: {len(station_year_map)}")
logging.info(f"line count: {line_count}")
logging.info(f"bad lines: {bad_count}")

for year in station_year_map:
    station_ids = station_year_map[year]
    print(f"{year} - {len(station_ids)}") 
