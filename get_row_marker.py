import sys
import h5py
import h5pyd

def h5File(path):
    """ open a HSDS domain or HDF5 file based on the path.
        if path starts with "hdf5://", use HSDS, otherwise
        use h5py on a regular file path """
    
    if path.startswith("hdf5://"):
        f = h5pyd.File(path, mode='r')
    else:
        f = h5py.File(path, mode='r')
    return f

#
# Main
#
if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
    print("Usage: python get_row_marker.py <filepath>")
    sys.exit(0)

filepath = sys.argv[1]
f = h5File(filepath)
dset = f["data"]
if "_row_marker" in dset.attrs:
    row_marker = dset.attrs["_row_marker"]
    print(f"year: {row_marker[0]} row: {row_marker[1]}")
else:
    print("not found")
