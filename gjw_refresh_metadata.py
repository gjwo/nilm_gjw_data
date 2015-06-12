from __future__ import print_function, division
import os
from os.path import join
from nilmtk.utils import check_directory_exists
from nilm_metadata import convert_yaml_to_hdf5

home_dir='/Users/GJWood/nilm_gjw_data' # path to input data

def refresh_gjw_metadata(gjw_path, output_filename, format="HDF"):
    """
    Parameters
    ----------
    gjw_path : str
        The root path of the gjw dataset.
    output_filename : str
        The destination filename (including path and suffix), will default if not specified
    directory and file structure
    nilm_gjw_data
        building<1>
            elec
                4-POWER_REAL_FINE <date> Dump.csv
                5-POWER_REACTIVE_STANDARD <date> Dump.csv
                ...
        ...
        building<n>
        HDF5
            nilm_gjw_data.hdf5
        metadata
            building1.yaml
            dataset.yaml
            meter_devices.yaml
        other files    
    """
    if gjw_path is None: gjw_path = home_dir
    check_directory_exists(gjw_path)
    os.chdir(gjw_path)
    gjw_path = os.getcwd()  # sort out potential issue with slashes or backslashes
    if output_filename is None:
        output_filename =join(home_dir,'HDF5','nilm_gjw_data.hdf5')
    convert_yaml_to_hdf5(join(gjw_path, 'metadata'),output_filename)
    print("Done refreshing metadata")
    
def main():
    refresh_gjw_metadata('c:/Users/GJWood/nilm_gjw_data', None)

if __name__ == '__main__':
    main()