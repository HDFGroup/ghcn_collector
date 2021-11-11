import h5pyd
f = h5pyd.File("/shared/ghcn/ghcn.h5k", use_cache=False)
dset = f['data']
station_year_map = {}
cursor = dset.create_cursor()
count = 0
for row in cursor:
    station_id = row['station_id'].decode('ascii')
    ymd = row['ymd'].decode('ascii')
    if len(ymd) != 8:
        # print(f"unexpected ymd: {ymd}")
        count += 1
        continue
    year = int(ymd[:4])  # format YYYYMMDD
    if year not in station_year_map:
        station_year_map[year] = set()
    station_ids = station_year_map[year]
    station_ids.add(station_id)
print("bad lines:", count)
for year in station_year_map:
    station_ids = station_year_map[year]
    print(f"{year} - {len(station_ids)}") 
