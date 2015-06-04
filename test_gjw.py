import numpy as np
import pandas as pd
from os.path import join

from pylab import rcParams
import matplotlib.pyplot as plt
%matplotlib inline
rcParams['figure.figsize'] = (14, 6)

plt.style.use('ggplot')
import nilmtk
from nilmtk import DataSet, TimeFrame, MeterGroup, HDFDataStore
from nilmtk.disaggregate import CombinatorialOptimisation
from nilmtk.utils import print_dict
from nilmtk.metrics import f1_score

import warnings
warnings.filterwarnings("ignore")
def main():
    #Load data
    gjw = DataSet("C:/Users/GJWood/nilm_gjw_data/HDF5/nilm_gjw_data.hdf5")
    print('loaded ' + str(len(gjw.buildings)) + ' buildings')
    
    #Examine metadata
    building_number =1
    print_dict(gjw.buildings[building_number].metadata) #metadata for house
    elec = gjw.buildings[building_number].elec
    print(elec.appliances)
    
    #List & plot coherent blocks of meter readings
    mains = elec.mains()
    mains_good_sections = elec.mains().good_sections()
    Print(elec.mains().good_sections())
    mains_good_sections.plot()
    
    #Examine the power data
    print(mains.available_power_ac_types())
    mains_energy = mains.total_energy(sections=mains_good_sections)
    print(mains_energy)
    whole_house = nilmtk.global_meter_group.select()
    print(whole_house.select(building=1).total_energy())
    whole_house.plot()
    
if __name__ == '__main__':
    main()