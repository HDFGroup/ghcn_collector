#!/usr/bin/env python3

'''
ghcn_setup:
Creates or updates HDF file for GHCN data.
'''

import sys
import os
import logging
import h5pyd
import h5py

if __name__ == "__main__":
    from ghcn_dtype import dt_day
else:
    from .ghcn_dtype import dt_day

def usage():
    """ Usage message """
    print("Create or update HDF data file for GHCN data")
    print("Usage: ghcn_config.py [-h] [--loglevel debug|info|warning|error] <filepath>")
    print("   <filepath>: HSDS or hdf5 file path ('hdf5://' prefix for HSDS)")
    print("Options:")
    print("   --help: this message")
    print("   --loglevel debug|info|warning|error: change default log level")
    sys.exit(1)



def h5File(path, mode='r'):
    """ open a HSDS domain or HDF5 file based on the path.
        if path starts with "hdf5://", use HSDS, otherwise
        use h5py on a regular file path """
    if path.startswith("hdf5://"):
        f = h5pyd.File(path, mode=mode)
    else:
        f = h5py.File(path, mode=mode)
    return f

def create_table(grp, name, dt):
    """ Create extensible 1-D dataset of given type if object with that
        name doesn't already exist. """
    if name in grp:
        return  # Dataset already exists
    logging.info(f"Creating dataset: {name}")

    grp.create_dataset(name, (0,), maxshape=(None,), chunks=(91268,), dtype=dt)
#  
# Main
#
if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
    usage()

hdf_filepath = None

loglevel = logging.INFO
argn = 1
while argn < len(sys.argv):
    arg = sys.argv[argn]
    val = None
    if len(sys.argv) > argn + 1:
        val = sys.argv[argn+1]
    if arg[0] == '-':
        # process option
        if arg == "--loglevel":
            val = val.upper()
            if val == "DEBUG":
                loglevel = logging.DEBUG
            elif val == "INFO":
                loglevel = logging.INFO
            elif val in ("WARN", "WARNING"):
                loglevel = logging.WARNING
            elif val == "ERROR":
                loglevel = logging.ERROR
            else:
                usage()
            argn += 1
        elif arg in ("-h", "--help"):
            usage()
        else:
            # unknown option
            usage()
    else:
        if not hdf_filepath:
            hdf_filepath = arg
         
    argn += 1

if not hdf_filepath:
    logging.error("HDF filepath not provided")
    usage()

logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)

if os.path.isfile(hdf_filepath):
    logging.info(f"HDF file: {hdf_filepath} not found, will initialize new file")

with h5File(hdf_filepath, mode='a') as f:
    logging.debug(f"Got root id: {f.id.id}")
    # Create data table if not created already
    create_table(f, "data", dt_day)

    # TBD - create/update auxillary tables 
    logging.info("done")

