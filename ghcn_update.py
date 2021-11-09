#!/usr/bin/env python3

'''
GHCN_update:

Read GHCN csv files from S3, parse, and append to HDF GHCN table.
'''

import time
import logging
import sys
import boto3
from botocore.exceptions import ClientError
import numpy as np
import h5pyd
import h5py
import config

MIN_SHORT = -32768
MAX_SHORT = 32767

if __name__ == "__main__":
    from ghcn_dtype import dt_day
else:
    from .ghcn_dtype import dt_day
 

def h5File(path, mode='r'):
    """ open a HSDS domain or HDF5 file based on the path.
        if path starts with "hdf5://", use HSDS, otherwise
        use h5py on a regular file path """
    logging.debug(f"h5File: {path}")
    
    if path.startswith("hdf5://"):
        kwargs = {'use_cache': False}
        endpoint = config.get("hsds_endpoint")
        if endpoint:
            kwargs['endpoint'] = endpoint
        username = config.get("hsds_username")
        if username:
            kwargs['username'] = username
        password = config.get("hsds_password")
        if password:
            kwargs['password'] = password
        f = h5pyd.File(path, mode=mode, **kwargs)
    else:
        f = h5py.File(path, mode=mode)
    return f

def addRows(f, rows):
    """ Add rows to table """
    count = len(rows)
    if count == 0:
        logging.warn("addRows - no rows to add!")
        return
    
    arr = np.zeros((count,), dtype=dt_day)
    for i in range(count):
        row = rows[i]
        fields = row.split(',')
        if len(fields) != 8:
            logging.warning(f"Expected 8 fields, skipping row:{i}")
            continue
        e = arr[i]
        station_id = fields[0]
        if len(station_id) != 11:
            logging.warning(f"Unexpected length for station: {station_id}")
        e['station_id'] = station_id
        ymd = fields[1]
        if len(ymd) != 8:
            logging.warning(f"Unexpected length for ymd: {ymd}")
        e['ymd'] = ymd
        element = fields[2]
        if len(element) != 4:
            logging.warning(f"Unexpected length for elemenbt: {element}")
        e['element'] = element
        try:
            data_value = int(fields[3])
        except ValueError:
            logging.warning(f"Unable to convert data_value to int: {fields[3]}")
            data_value = -999
        if data_value < MIN_SHORT:
            logging.warning(f"Data value less than MIN_SHORT: {data_value}")
            data_value = -999
        if data_value > MAX_SHORT:
            logging.warning(f"Data value greater than MAX_SHORT: {data_value}")
            data_value = -999
        e['data_value'] = data_value
        m_flag = fields[4]
        if len(m_flag) > 1:
            logging.warn(f"Unexpected length of m_flag: {m_flag}")
            m_flag = m_flag[0]
        e['m_flag'] = m_flag
        q_flag = fields[5]
        if len(q_flag) > 1:
            logging.warn(f"Unexpected length of q_flag: {q_flag}")
            q_flag = q_flag[0]
        e['q_flag'] = q_flag
        s_flag = fields[6]
        if len(s_flag) > 1:
            logging.warn(f"Unexpected length of s_flag: {s_flag}")
            s_flag = s_flag[0]
        e['s_flag'] = s_flag
        obs_time = fields[7]
        if len(obs_time) > 4:
            logging.warn(f"Unexpected length of obs_time: {obs_time}")
            obs_time = obs_time[:4]
        e['obs_time'] = obs_time
        arr[i] = e

    dset  = f['data']
    next_row = dset.shape[0]
    logging.info(f"current shape: {dset.shape[0]}, adding: {count}")
    # Extend by num_rows
    dset.resize((next_row+count,))
    # Write array to extended area
    dset[next_row:next_row+count] = arr
    
    return count

def getRowMarker(f, year):
    """ Get the row marker for given year 
    (where the most recent update left off) and return. 
    Returns 0 if doesn't exist.  """
    dset = f['data']
    marker = 0
    if "_row_marker" in dset.attrs:
        row_marker = dset.attrs["_row_marker"]
        # returns [year, row]
        if row_marker[0] == year:
            marker = row_marker[1]
        
    return marker

def setRowMarker(f, year, row):
    """ Set the row marker year and row.  Will over-write
    any existing value. """
    dset = f['data']
    if "_row_marker" in dset.attrs:
        del dset.attrs["_row_marker"]
    dset.attrs["_row_marker"] = [year, row]


def addYearData(f, year):
    """Get data for given year and add to table"""
    logging.info(f"addYearData: {year}")
    return_rows = 0
    range_start = 0
    block_size = config.get("block_size")
    # expected lines:
    #  b'ASN00008050,18770101,PRCP,0,,,a,\n
    s3_bucket = config.get("ghcn_bucket")
    s3_path = config.get("ghcn_path")
    s3_key = f"{s3_path}{year}.csv"

    # get the last row processed for the given year
    row_marker = getRowMarker(f, year)
    rows_read = 0

    #s3 = boto3.ressource('s3')
    s3 = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='')
    s3._request_signer.sign = (lambda *args, **kwargs: None)
    try:
        # Do HEAD request to verify key exist and get size
        rsp = s3.head_object(Bucket=s3_bucket, Key=s3_key)
        content_length = rsp['ContentLength']
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'NoSuchKey':
            logging.warn(f"key: {s3_key} not found")
            return 0

    logging.debug(f"content length for {s3_key}: {content_length}")
    
    while True:
        range_end = range_start + block_size
        if range_end > content_length:
            range_end = content_length
        if range_end - range_start <= 0:
            logging.info("no more bytes to read")
            break

        s3_range = f"bytes={range_start}-{range_end}"
        logging.debug(f"s3_range: {s3_range}")
        ghcn_text = None
        try:
            rsp = s3.get_object(Bucket=s3_bucket, Key=s3_key, Range=s3_range)
            body = rsp['Body']
            ghcn_text = body.read()
        except ClientError as ce:
            error_code = ce.response['Error']['Code']
            if error_code == "InvalidRange":
                logging.info(f"exceeded range for s3_range: {s3_range}")
            else:
                logging.error(f"ClientError: {ce.response['Error']['Code']} for s3_range: {s3_range}")
        
        if not ghcn_text:
            logging.info("no bytes read")
            break
        ghcn_text = ghcn_text.decode('ascii')        
        num_bytes = len(ghcn_text)
        logging.debug(f"read {num_bytes} bytes")
        rows = ghcn_text.split('\n')
        last_row = rows[-1]
        fields = last_row.split(',')
        range_start += num_bytes
        if len(fields) < 8:
            # back up to the last \n so we get full text line.  
            range_start -= len(last_row)
            rows = rows[:-1]

        rows_read += len(rows)

        if len(rows) == 0:
            logging.debug("no rows read, breaking")
            break

        # If the current set of rows overlaps with rows we've
        # already read, just process the remaining rows.
        # Example:
        # row_marker = 101
        # rows_read = 110
        # len(rows) = 10
        # rows = rows[1:]
        # index = len(rows) + row_maker - rows_read
        # rows = rows[index:]
        logging.info(f"got row_marker: {row_marker}")
        if rows_read > row_marker:
            if rows_read - len(rows) < row_marker:
                # remove rows we've already processed
                index = len(rows) + row_marker - rows_read
                rows = rows[index:]
            logging.info("adding {len(rows)} rows")
            addRows(f, rows)

            return_rows += len(rows)    

            setRowMarker(f, year, rows_read)    
    
    logging.info(f"addYearData {year} - return_rows: {return_rows}")

    return return_rows


def getData(f):
    """ update data table with latest GHCN content """
    data_dset = f["data"]
    num_rows = data_dset.shape[0]
    if num_rows == 0:
        # empty, start at first year
        year = config.get("start_year")
        logging.info(f"no data, starting at year: {year}")
    else:
        last_row = data_dset[-1]
        ymd = last_row["ymd"]
        year = int(ymd[:4])
        logging.info(f"most recent year: {year}")

    total_added = 0
    last_year = -1
    while True:
        if year >= config.get("last_year"):
            # completed desired year range
            break
        this_year = addYearData(f, year)
        total_added += this_year
        if last_year == 0 and this_year == 0:
            # no data for this year or last, quit
            break
        last_year = this_year
        year += 1

    return total_added
 
#
# Main:
#


# Setup logging
log_level = config.get('log_level')
print('Set-up log_level:', log_level)
if log_level == 'DEBUG':
    level = logging.DEBUG
elif log_level == 'INFO':
    level = logging.INFO
elif log_level in ('WARN', 'WARNING'):
    level = logging.WARNING
elif log_level == 'ERROR':
    level = logging.ERROR
else:
    print(f'Unexpected log_level settings: {log_level}, defaulting to DEBUG')
    level = logging.DEBUG

# logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=level)
logging.basicConfig(level=level)

sleep_time = config.get("polling_interval")
logging.debug("sleep_time:", sleep_time)

filename = None
for i in range(1, len(sys.argv)):
    arg = sys.argv[1]
    if arg[0] != '-':
        # not an override option
        filename = arg

if not filename:
    filename = config.get("filename")

if not filename:
    logging.error("no filename provided!")
    sys.exit(1)

logging.info(f"Using filename: {filename}")

# Process yearly data files until we get two consective years with no update.
while True:
    nrows = 0
    try:
        with h5File(filename, mode='a') as f:
            nrows = getData(f)
            if nrows > 0:
                logging.info(f"added {nrows} rows") 
            else:
                logging.info("no rows found")
    except Exception as e:
        logging.error(f"Unexpected exception {e}")
    if config.get("run_forever"):
        logging.info(f"sleeping for {sleep_time} minutes")
        time.sleep(sleep_time*60)
    else:
        break

