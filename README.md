# nilm_gjw_data
Non Intrusive Load Monitoring data repository and data converter for NILMTK
This repository is electricity monitoring data collected by me, and the necessary metadata and converter to import the
data into the Non Intrusive Load Monitor Tool Kit https://github.com/nilmtk/nilmtk/
Further information about the metadata can be found here https://github.com/nilmtk/nilm_metadata and 
the documentation can be found here https://github.com/nilmtk/nilmtk.github.io
    directory and file structure
    nilm_gjw_data
        building<1>
            elec
                4-POWER_REAL_FINE <date> Dump.csv
                5-POWER_REACTIVE_STANDARD <date> Dump.csv
                ...
                meter1.data <----------------- output file in CSV format
        ...
        building<n>
        HDF5
            nilm_gjw_data.hdf5 <-------------- output file in HDM5 format
        metadata
            building1.yaml
            dataset.yaml
            meter_devices.yaml
        <other files such as>
        gjw_converter.py  <------------------ the converter code
        gjw_converter_test.ipnb <------------ runs of the convertr code in iPython Notebook
