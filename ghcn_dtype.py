'''
GHCN-dtype:
Describes the data types to be used by the HDF store
'''

import numpy as np


#
# Definition of data types (dt)
#

# Type station identification code
dt_station_id = np.dtype('S11')

# Type for calendar day
# 8 character date in YYYYMMDD format (e.g. 19860529 = May 29, 1986)
dt_ymd = np.dtype('S8')

# Eleement - 5 character element type
dt_element = np.dtype('S4')

# Type for data value
# provided as 5 character string, so short should be sufficient
dt_data_value = np.dtype('i2')

# Measurement Flag
dt_m_flag = np.dtype('S1')

# Quality Flag
dt_q_flag = np.dtype('S1')

# Source Flg
dt_s_flag = np.dtype('S1')

# Observation time
# 4-character time of observation in hour-minute format (i.e. 0700 =7:00 am)
dt_obs_time = np.dtype('S4')

# Datatype for 'day' table
dt_day = np.dtype([('station_id', dt_station_id),
                      ('ymd', dt_ymd),
                      ('element', dt_element),
                      ('data_value', dt_data_value),
                      ('m_flag', dt_m_flag),
                      ('q_flag', dt_q_flag),
                      ('s_flag', dt_s_flag),
                      ('obs_time', dt_obs_time)
                      ])


