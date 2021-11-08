GHCN Data Loader
======================================================================
 
Introduction
------------

This project can be used to create an HDF5 file containing GHCN (Global Historical Climatology Network Daily) data.  NOAA updates GHCN records daily for thousands
of weather stations across the globe.  These records are available on AWS S3 as CSV
files stored by year (starting in 1876!).  The data loader will incremental pull data
from these files and construct an HDF5 dataset containing all the records.  Once created,
you can use the HDF5 library or HSDS to query and analyze information from the file.

Usage
-----

Update config.yml with values for filename, and any other desired values. To use HSDS 
rather than HDF5 library, add the "hdf5://" prefix to the filepath.
HSDS endpoint, username, and password can be specified here, or pulled from a .hscfg file,
or specified using environment variables (`export HSDS_ENDPOINT=http://myhsds.myorg.org`, etc.)

Run: `python ghcn_setup.py <filepath>` to initialize the HDF5 or HSDS domain file.

Run: `python ghcn_update.py` to start the collection of GHCN data.  If the `run_forever`
config is set, the script will periodically check for updates in the GHCN CSV bucket.  Otherwise, it will stop after the desired year range is collected.

Related Information
--------------------

See: <https://github.com/awslabs/open-data-docs/tree/main/docs/noaa/noaa-ghcn> for 
information on the data format.

The AWS Open Data repository for GHCN data is described here: <https://registry.opendata.aws/noaa-ghcn/>.

Information about HSDS can be found here: <https://github.com/HDFGroup/hsds>.
