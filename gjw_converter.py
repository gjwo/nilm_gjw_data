from __future__ import print_function, division
import pandas as pd
import numpy as np
import os
from os.path import join, isfile
import fnmatch
import re
from nilmtk.utils import get_datastore
from nilmtk.datastore import Key
from nilmtk.measurement import LEVEL_NAMES
from nilmtk.utils import check_directory_exists
from nilm_metadata import convert_yaml_to_hdf5, save_yaml_to_datastore

column_mapping = {
    'frequency': ('frequency', ""),
    'voltage': ('voltage', ""),
    'W': ('power', 'active'),
    'active': ('power', 'active'),
    'energy': ('energy', 'apparent'),
    'A': ('current', ''),
    'reactive_power': ('power', 'reactive'),
    'apparent_power': ('power', 'apparent'),
    'power_factor': ('pf', ''),
    'PF': ('pf', ''),
    'phase_angle': ('phi', ''),
    'VA': ('power', 'apparent'),
    'VAR': ('power', 'reactive'),
    'reactive': ('power', 'reactive'),
    'VLN': ('voltage', ""),
    'V': ('voltage', ""),
    'f': ('frequency', "")
    
}
# data for file name manipulation
TYPE_A = "active"
TYPE_R = "reactive"

filename_prefix_mapping = {
    TYPE_A : ('4-POWER_REAL_FINE '),
    TYPE_R : ('5-POWER_REACTIVE_STANDARD ')
}
filename_suffix_mapping = {
    TYPE_A : (' Dump'),
    TYPE_R : (' Dump')
}

# DataFrame column names
TIMESTAMP_COLUMN_NAME = "timestamp"
ACTIVE_COLUMN_NAME = "VA"
REACTIVE_COLUMN_NAME = "reactive"

type_column_mapping = {
    TYPE_A : (ACTIVE_COLUMN_NAME),
    TYPE_R : (REACTIVE_COLUMN_NAME) 
}


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
    if gjw_path is None: gjw_path = home_dir
    check_directory_exists(gjw_path)
    os.chdir(gjw_path)
    gjw_path = os.getcwd()  # sort out potential issue with slashes or backslashes
    if output_filename is None:
        output_filename =join(home_dir,'HDF5','nilm_gjw_data.hdf5')
    # Open data store
    print( 'opening datastore', output_filename)
    store = get_datastore(output_filename, format, mode='w')
    # walk the directory tree from the dataset home directory
    #clear dataframe & add column headers
    df = pd.DataFrame(columns=[ACTIVE_COLUMN_NAME,REACTIVE_COLUMN_NAME])
    found = False
    for current_dir, dirs_in_current_dir, files in os.walk(gjw_path):
        if current_dir.find('.git')!=-1 or current_dir.find('.ipynb') != -1:
            #print( 'Skipping ', current_dir)
            continue
        print( 'checking', current_dir)
        m = bld_re.search(current_dir)
        if m: #The csv files may be further down the tree so this section may be repeated
            building_name = m.group()
            building_nbr = int(bld_nbr_re.search(building_name).group())
            meter_nbr = 1
            key = Key(building=building_nbr, meter=meter_nbr)
        for items in fnmatch.filter(files, "4*.csv"):
            # process any .CSV files found
            found = True
            ds = iso_date_re.search(items).group()
            print( 'found files for date:', ds,end=" ")
            # found files to process
            df1 = _read_file_pair(current_dir,ds) # read two csv files into a dataframe    
            df = pd.concat([df,df1]) # concatenate the results into one long dataframe
        if found:
            found = False
            df = _prepare_data_for_toolkit(df)
            _summarise_dataframe(df,'Prepared for tool kit')
            store.put(str(key), df)
            #clear dataframe & add column headers
            #df = pd.DataFrame(columns=[ACTIVE_COLUMN_NAME,REACTIVE_COLUMN_NAME])
            break # only 1 folder with .csv files at present
    store.close()
    convert_yaml_to_hdf5(join(gjw_path, 'metadata'),output_filename)
    print("Done converting gjw to HDF5!")

def _read_and_standardise_file(cdir,ds,mtype):   
    """
    parameters 
        cdir  - the directory path where the files may be found
        ds   - the date string which identifies the pair of files
        type - the type of data to be read
    The filename is constructed using the appropriate prefixes and suffixes
    The data is then read, merged, de-duplicated, converted to the correct time zone
    and converted to a time series and resampled per second
    """
    fn = filename_prefix_mapping[mtype]+ds+filename_suffix_mapping[mtype]+'.csv'
    ffn = join(cdir,fn)
    df = pd.read_csv(ffn,names=[TIMESTAMP_COLUMN_NAME,type_column_mapping[mtype]])
    df.drop_duplicates(subset=[TIMESTAMP_COLUMN_NAME], inplace=True) # remove duplicate rows with same timestamp
    df.index = pd.to_datetime(df.timestamp.values, unit='s', utc=True) # convert the index to time based
    df = df.tz_convert(TIMEZONE) #deal with summertime etc. for London timezone
    # re-sample on single file only as there may be gaps between dumps            
    df = df.resample('S',fill_method='ffill') # make sure we have a reading for every second
    # resample seems to remove the timestamp column so put it back
    df[TIMESTAMP_COLUMN_NAME] = df.index
    df.drop_duplicates(subset=TIMESTAMP_COLUMN_NAME, inplace=True)
    return df

def _read_file_pair(cdir,ds):
    """"
    parameters 
        cdir - the directory path where the files may be found
        ds  - the date string which identifies the pair of files
    The files are processed individually then the columns merged on matching timestamps   
    """
    df1 = _read_and_standardise_file(cdir,ds,TYPE_A)
    #_summarise_dataframe(df1,'read file: '+TYPE_A)
    df2 = _read_and_standardise_file(cdir,ds,TYPE_R)
    #_summarise_dataframe(df2,'read file: '+TYPE_R)  
    df3 = pd.merge(df1,df2,on=TIMESTAMP_COLUMN_NAME, how='outer') #merge the two column types into 1 frame
    df3.fillna(value=0, inplace=True) # may need to enter initial entries to reactive sequence
    #_summarise_dataframe(df3,'return from merge and fillna)
    print(df3[TIMESTAMP_COLUMN_NAME].head(1),"to",df3[TIMESTAMP_COLUMN_NAME].tail(1)) #print first and last entries
    return df3

def _prepare_data_for_toolkit(df):
    #remove any duplicate timestamps between files
    df.drop_duplicates(subset=["timestamp"], inplace=True) # remove duplicate rows with same timestamp
    df.index = pd.to_datetime(df.timestamp.values, unit='s', utc=True) # convert the index to time based
    df = df.tz_convert(TIMEZONE) #deal with summertime etc. for London timezone
    df = df.drop(TIMESTAMP_COLUMN_NAME,1) # remove the timestamp column  
    df.rename(columns=lambda x: column_mapping[x], inplace=True) # Renaming from gjw header to nilmtk controlled vocabulary
    df.columns.set_names(LEVEL_NAMES, inplace=True) # Needed for column levelling (all converter need this line)
    df = df.convert_objects(convert_numeric=True) # make sure everything is numeric
    df = df.dropna() # drop rows with empty cells
    df = df.astype(np.float32) # Change float 64 (default) to float 32 
    df = df.sort_index() # Ensure that time series index is sorted
    return df

def _summarise_dataframe(df,loc):
    print(df.head(4))
    print("...", len(df.index),"rows at", loc)
    print (df.tail(4))
    
def main():
    convert_gjw('c:/Users/GJWood/nilm_gjw_data', None)

if __name__ == '__main__':
    main()