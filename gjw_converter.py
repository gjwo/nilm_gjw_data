from __future__ import print_function, division
import pandas as pd
import numpy as np
from copy import deepcopy
import os
from os.path import join, isdir, isfile
from os import listdir
import fnmatch
import re
from sys import stdout
from nilmtk.utils import get_datastore
from nilmtk.datastore import Key
from nilmtk.timeframe import TimeFrame
from nilmtk.measurement import LEVEL_NAMES
from nilmtk.utils import get_module_directory, check_directory_exists
from nilm_metadata import convert_yaml_to_hdf5, save_yaml_to_datastore

column_mapping = {
    'active': ('power', 'active'),
    'reactive': ('power', 'reactive')
    
}
# data for file name manipulation
filename_prefix_mapping = {
    'active' : ('4-POWER_REAL_FINE '),
    'reactive' : ('5-POWER_REACTIVE_STANDARD ')
}
filename_suffix_mapping = {
    'active' : (' Dump'),
    'reactive' : (' Dump')
}
# DataFrame column names
TIMESTAMP_COLUMN_NAME = "timestamp"
ACTIVE_COLUMN_NAME = "active"
REACTIVE_COLUMN_NAME = "reactive"

TIMEZONE = "Europe/London" # local time zone
home_dir='/Users/GJWood/nilm_gjw_data' # path to input data

#regular expression matching
bld_re = re.compile('building\d+') #used to pull building name from directory path
bld_nbr_re = re.compile ('\d+') # used to pull the building number from the name
iso_date_re = re.compile ('\d{4}-\d{2}-\d{2}') # used to pull the date from the file name

def convert_gjw(gjw_path, output_filename, format="HDF"):
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
    if gjw_path is None: gjwpath = home_dir
    check_directory_exists(gjw_path)
    os.chdir(gjw_path)
    gjw_path = os.getcwd()  # sort out potential issue with slashes or backslashes
    if output_filename is None:
        output_filename =join(home_dir,'HDF5','nilm_gjw_data.hdf5')
    elec_path = join(gjw_path, 'building1','elec')
    # Open data store
    print( 'opening datastore', output_filename)
    store = get_datastore(output_filename, format, mode='w')
    # walk the directory tree from the dataset home directory
    #clear dataframe & add column headers
     df = pd.DataFrame(columns=[TIMESTAMP_COLUMN_NAME,ACTIVE_COLUMN_NAME,REACTIVE_COLUMN_NAME])
    found = False
    for current_dir, dirs_in_current_dir, files in os.walk(gjw_path):
        if current_dir.find('.git')!=-1 or current_dir.find('.ipynb') != -1:
            print( 'Skipping ', current_dir)
            continue
        print( 'checking', current_dir)
        m = bld_re.search(current_dir)
        if m:
            building_name = m.group()
            building_number = int(bld_nbr_re.search(building_name).group())
            meter_nbr = 1
            key = Key(building=building_number, meter=meter_nbr)
       for items in fnmatch.filter(files, "4*.csv"):
            # process any .CSV files found
            found = True
            ds = iso_date_re.search(items).group()
            print( 'found files for date:', ds)
            # found files to process
            df1,df2 = _read_filename_pair(current_dir,ds) # read the csv files into dataframes
            df3 = pd.merge(df1,df2,on=TIMESTAMP_COLUMN_NAME) #merge the two column types into 1 frame 
            df = pd.concat([df,df3]) # concatenate the results into one long dataframe
        if found:
            found = False
            df = _tidy_data(df)
            csvout_fn ='building'+str(building_nbr)+'_meter'+str(meter_nbr)+'.data'
            csvout_ffn = join(current_dir,csvout_fn) # not called .csv to avoid clash
            #print( csvout_ffn)
            df.to_csv(csvout_ffn)
            store.put(str(key), df)
            #clear dataframe & add column headers
            #df = pd.DataFrame(columns=[TIMESTAMP_COLUMN_NAME,ACTIVE_COLUMN_NAME,REACTIVE_COLUMN_NAME])
            break # only 1 folder with .csv files at present
    store.close()
    convert_yaml_to_hdf5(join(gjw_path, 'metadata'),output_filename)
    print("Done converting gjw to HDF5!")

def _read_filename_pair(dir,ds):
    fn1 = filename_prefix_mapping['active']+ds+filename_suffix_mapping['active']+'.csv'
    fn2 = filename_prefix_mapping['reactive']+ds+filename_suffix_mapping['reactive']+'.csv'
    ffn1 = join(dir,fn1)
    ffn2 = join(dir,fn2)
    #print(fn1 +' <-> '+ fn2)
    return pd.read_csv(ffn1,names=[TIMESTAMP_COLUMN_NAME,ACTIVE_COLUMN_NAME]),pd.read_csv(ffn2,names=[TIMESTAMP_COLUMN_NAME,REACTIVE_COLUMN_NAME])

def _tidy_data(df):
    df.drop_duplicates(subset=["timestamp"], inplace=True) # remove duplicate rows with same timestamp
    df.index = pd.to_datetime(df.timestamp.values, unit='s', utc=True) # convert the index to time based
    df = df.tz_convert(TIMEZONE) #deal with summertime etc. for London timezone
    df = df.drop(TIMESTAMP_COLUMN_NAME, 1) # remove the now redundant timestamp column
    df.rename(columns=lambda x: column_mapping[x], inplace=True) #replace column lables with [P,T] pair
    df.columns.set_names(LEVEL_NAMES, inplace=True) # rename the columns with 2 levels of name
    df = df.convert_objects(convert_numeric=True) # make sure everything is numeric
    df = df.dropna() # drop rows with empty cells
    df = df.astype(np.float32) #make everything floating point
    df = df.sort_index()
    return df
    
def main():
    convert_gjw('c:/Users/GJWood/nilm_gjw_data', None)

if __name__ == '__main__':
    main()