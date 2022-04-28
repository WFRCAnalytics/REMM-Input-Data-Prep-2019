import arcpy
from arcpy import env
import os
import sys
from arcgis import GIS
from arcgis.features import GeoAccessor
import pandas as pd
import numpy as np
arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "80%"
print('libraries loaded...')


#========================================
# functions and classes
#========================================

class Unbuffered(object):
    def __init__(self, stream):
        self.stream = stream
    def write(self, data):
        self.stream.write(data)
        self.stream.flush()
    def writelines(self, datas):
        self.stream.writelines(datas)
        self.stream.flush()
    def __getattr__(self, attr):
        return getattr(self.stream, attr)

class building:
    def __init__(self, parcel_id, tazid, county_name, building_type, ind_spaces, non_ind_spaces):
        self.parcel_id = parcel_id
        self.tazid = tazid
        county_dict = {'Weber':57, 'Salt Lake':35, 'Utah':49, 'Davis':11}
        self.cid = county_dict[county_name]
        self.ind_spaces = ind_spaces
        self.non_ind_spaces = non_ind_spaces
        self.building_type = building_type
        
        self.jobs1 = 0
        self.jobs2 = 0
        self.jobs3 = 0
        self.jobs4 = 0
        self.jobs5 = 0
        self.jobs6 = 0
        self.jobs7 = 0
        self.jobs8 = 0
        self.jobs9 = 0
        self.jobs10 = 0
        self.jobs11 = 0
        
        self.ind_jobs = self.jobs5 + self.jobs10
        self.ind_space_remaining =  self.ind_spaces - self.ind_jobs
        
        self.non_ind_jobs = self.jobs1 + self.jobs2 + self.jobs3 + self.jobs4 + self.jobs6 + self.jobs7 + self.jobs8 + self.jobs9 + self.jobs11
        self.non_ind_space_remaining =  self.non_ind_spaces - self.non_ind_jobs 

    def update_counts(self):
        self.ind_jobs = self.jobs5 + self.jobs10
        self.ind_space_remaining =  self.ind_spaces - self.ind_jobs
        self.non_ind_jobs = self.jobs1 + self.jobs2 + self.jobs3 + self.jobs4 + self.jobs6 + self.jobs7 + self.jobs8 + self.jobs9 + self.jobs11
        self.non_ind_space_remaining =  self.non_ind_spaces - self.non_ind_jobs 

    def add_jobs1(self, number):    
        if number <= self.non_ind_space_remaining:
            self.jobs1 = self.jobs1 + number
            undistributed = 0
            self.update_counts()
            return undistributed
        
        if number > self.non_ind_space_remaining and self.non_ind_space_remaining > 0:
            undistributed = number - self.non_ind_space_remaining
            self.jobs1 = self.jobs1 + self.non_ind_space_remaining
            self.update_counts()
            return undistributed

        if self.non_ind_space_remaining == 0:
            undistributed = number
            self.update_counts()
            return undistributed
    
    def add_jobs2(self, number):    
        if number <= self.non_ind_space_remaining:
            self.jobs2 = self.jobs2 + number
            undistributed = 0
            self.update_counts()
            return undistributed
        
        if number > self.non_ind_space_remaining and self.non_ind_space_remaining > 0:
            undistributed = number - self.non_ind_space_remaining
            self.jobs2 = self.jobs2 + self.non_ind_space_remaining
            self.update_counts()
            return undistributed

        if self.non_ind_space_remaining == 0:
            undistributed = number
            self.update_counts()
            return undistributed
    
    def add_jobs3(self, number):    
        if number <= self.non_ind_space_remaining:
            self.jobs3 = self.jobs3 + number
            undistributed = 0
            self.update_counts()
            return undistributed
        
        if number > self.non_ind_space_remaining and self.non_ind_space_remaining > 0:
            undistributed = number - self.non_ind_space_remaining
            self.jobs3 = self.jobs3 + self.non_ind_space_remaining
            self.update_counts()
            return undistributed

        if self.non_ind_space_remaining == 0:
            undistributed = number
            self.update_counts()
            return undistributed
    
    def add_jobs4(self, number):    
        if number <= self.non_ind_space_remaining:
            self.jobs4 = self.jobs4 + number
            undistributed = 0
            self.update_counts()
            return undistributed
        
        if number > self.non_ind_space_remaining and self.non_ind_space_remaining > 0:
            undistributed = number - self.non_ind_space_remaining
            self.jobs4 = self.jobs4 + self.non_ind_space_remaining
            self.update_counts()
            return undistributed

        if self.non_ind_space_remaining == 0:
            undistributed = number
            self.update_counts()
            return undistributed
    
    def add_jobs5(self, number):
        if number <= self.ind_space_remaining:
            self.jobs5 = self.jobs5 + number
            undistributed = 0
            self.update_counts()
            return undistributed
        
        if number > self.ind_space_remaining and self.ind_space_remaining > 0:
            undistributed = number - self.ind_space_remaining
            self.jobs5 = self.jobs5 + self.ind_space_remaining
            self.update_counts()
            return undistributed

        if self.ind_space_remaining == 0:
            undistributed = number
            self.update_counts()
            return undistributed
    
    def add_jobs6(self, number):    
        if number <= self.non_ind_space_remaining:
            self.jobs6 = self.jobs6 + number
            undistributed = 0
            self.update_counts()
            return undistributed
        
        if number > self.non_ind_space_remaining and self.non_ind_space_remaining > 0:
            undistributed = number - self.non_ind_space_remaining
            self.jobs6 = self.jobs6 + self.non_ind_space_remaining
            self.update_counts()
            return undistributed

        if self.non_ind_space_remaining == 0:
            undistributed = number
            self.update_counts()
            return undistributed

    def add_jobs7(self, number):    
        if number <= self.non_ind_space_remaining:
            self.jobs7 = self.jobs7 + number
            undistributed = 0
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(number, undistributed))
            self.update_counts()
            return undistributed
        
        if number > self.non_ind_space_remaining and self.non_ind_space_remaining > 0:
            undistributed = number - self.non_ind_space_remaining
            self.jobs7 = self.jobs7 + self.non_ind_space_remaining
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(self.non_ind_space_remaining, undistributed))
            self.update_counts()
            return undistributed

        if self.non_ind_space_remaining == 0:
            undistributed = number
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(0, undistributed))
            self.update_counts()
            return undistributed
    
    def add_jobs8(self, number):    
        if number <= self.non_ind_space_remaining:
            self.jobs8 = self.jobs8 + number
            undistributed = 0
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(number, undistributed))
            self.update_counts()
            return undistributed
        
        if number > self.non_ind_space_remaining and self.non_ind_space_remaining > 0:
            undistributed = number - self.non_ind_space_remaining
            self.jobs8 = self.jobs8 + self.non_ind_space_remaining
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(self.non_ind_space_remaining, undistributed))
            self.update_counts()
            return undistributed

        if self.non_ind_space_remaining == 0:
            undistributed = number
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(0, undistributed))
            self.update_counts()
            return undistributed
    
    def add_jobs9(self, number):    
        if number <= self.non_ind_space_remaining:
            self.jobs9 = self.jobs9 + number
            undistributed = 0
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(number, undistributed))
            self.update_counts()
            return undistributed
        
        if number > self.non_ind_space_remaining and self.non_ind_space_remaining > 0:
            undistributed = number - self.non_ind_space_remaining
            self.jobs9 = self.jobs9 + self.non_ind_space_remaining
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(self.non_ind_space_remaining, undistributed))
            self.update_counts()
            return undistributed

        if self.non_ind_space_remaining == 0:
            undistributed = number
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(0, undistributed))
            self.update_counts()
            return undistributed

    def add_jobs10(self, number):    
        if number <= self.ind_space_remaining:
            self.jobs10 = self.jobs10 + number
            undistributed = 0
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(number, undistributed))
            self.update_counts()
            return undistributed
        
        if number > self.ind_space_remaining and self.ind_space_remaining > 0:
            undistributed = number - self.ind_space_remaining
            self.jobs10 = self.jobs10 + self.ind_space_remaining
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(self.ind_space_remaining, undistributed))
            self.update_counts()
            return undistributed

        if self.ind_space_remaining == 0:
            undistributed = number
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(0, undistributed))
            self.update_counts()
            return undistributed
    
    def add_jobs11(self, number):    

        if number <= self.non_ind_space_remaining:
            self.jobs11 = self.jobs11 + number
            undistributed = 0
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(number, undistributed))
            self.update_counts()
            return undistributed
        
        if number > self.non_ind_space_remaining and self.non_ind_space_remaining > 0:
            undistributed = number - self.non_ind_space_remaining
            self.jobs11 = self.jobs11 + self.non_ind_space_remaining
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(self.non_ind_space_remaining, undistributed))
            self.update_counts()
            return undistributed

        if self.non_ind_space_remaining == 0:
            undistributed = number
            # print("Jobs distributed: {}. Jobs not distributed: {}".format(0, undistributed))
            self.update_counts()
            return undistributed


    def convert_2_list(self):
        return [self.parcel_id, self.building_type, self.non_ind_spaces, self.ind_spaces,  self.tazid, self.cid, self.jobs1, self.jobs2, self.jobs3, self.jobs4, self.jobs5, self.jobs6, self.jobs7, self.jobs8, self.jobs9, self.jobs10, self.jobs11]

    def export_2_csv(self):
        pass

def distribute_jobs(sector, buildings, njobs, mode='dont_match', btype='non_ind'):
    remaining_jobs = njobs
    jobs_distributed = 0
    #result = remaining_jobs
    
    # Subset pool of buildings if in match mode; job sector:[building_types]
    preferred_building_types =  {1:[4,7], 3:[6,9], 4:[13], 5:[3], 6:[5], 7:[5], 9:[4], 10:[3]}
    if mode == 'match':
        buildings = [b for b in buildings if b.building_type in preferred_building_types[sector]]
    
    remaining_ind_spaces = sum(b.ind_space_remaining for b in buildings)
    remaining_non_ind_spaces = sum(b.non_ind_space_remaining for b in buildings)
    
    if len(buildings) > 0:
    
        if btype == 'non_ind':
            while remaining_jobs > 0 and remaining_non_ind_spaces > 0:
                
                 
                for bldg in buildings:
            
                    # amount of jobs to add to one building at a time
                    jobs_to_add = 1
            
                    # try to add job, get remaining back
                    if bldg.non_ind_space_remaining > 0 and jobs_distributed < njobs:
                        
                        if sector == 1:
                            result = bldg.add_jobs1(jobs_to_add)
                        elif sector == 2:
                            result = bldg.add_jobs2(jobs_to_add) 
                        elif sector == 3:
                            result = bldg.add_jobs3(jobs_to_add)
                        elif sector == 4:
                            result = bldg.add_jobs4(jobs_to_add)
                        elif sector == 5:
                            result = bldg.add_jobs5(jobs_to_add)
                        elif sector == 6:
                            result = bldg.add_jobs6(jobs_to_add)
                        elif sector == 7:
                            result = bldg.add_jobs7(jobs_to_add) 
                        elif sector == 8:
                            result = bldg.add_jobs9(jobs_to_add)
                        elif sector == 9:
                            result = bldg.add_jobs9(jobs_to_add)
                        elif sector == 10:
                            result = bldg.add_jobs10(jobs_to_add)
                        elif sector == 11:
                            result = bldg.add_jobs11(jobs_to_add)
                            
                               
                        if result == 0:
                            remaining_jobs = remaining_jobs - jobs_to_add
                            jobs_distributed = jobs_distributed + jobs_to_add
                        else:
                            remaining_jobs = remaining_jobs - (jobs_to_add + result)
            
                        # update amount of spaces
                        remaining_ind_spaces = sum(b.ind_space_remaining for b in buildings)
                        remaining_non_ind_spaces = sum(b.non_ind_space_remaining for b in buildings)
        
        
        if btype == 'ind':
            while remaining_jobs > 0 and remaining_ind_spaces > 0:
                for bldg in buildings:
                    
                    
                    # amount of jobs to add to one building at a time
                    jobs_to_add = 1
    
                    # try to add job, get remaining back
                    if bldg.ind_space_remaining > 0 and jobs_distributed < njobs:
                        if sector == 1:
                            result = bldg.add_jobs1(jobs_to_add)
                        elif sector == 2:
                            result = bldg.add_jobs2(jobs_to_add) 
                        elif sector == 3:
                            result = bldg.add_jobs3(jobs_to_add)
                        elif sector == 4:
                            result = bldg.add_jobs4(jobs_to_add)
                        elif sector == 5:
                            result = bldg.add_jobs5(jobs_to_add)
                        elif sector == 6:
                            result = bldg.add_jobs6(jobs_to_add)
                        elif sector == 7:
                            result = bldg.add_jobs7(jobs_to_add) 
                        elif sector == 8:
                            result = bldg.add_jobs9(jobs_to_add)
                        elif sector == 9:
                            result = bldg.add_jobs9(jobs_to_add)
                        elif sector == 10:
                            result = bldg.add_jobs10(jobs_to_add)
                        elif sector == 11:
                            result = bldg.add_jobs11(jobs_to_add)
                        
                        
                        
                        if result == 0:
                            remaining_jobs = remaining_jobs - jobs_to_add
                            jobs_distributed = jobs_distributed + jobs_to_add
                        else:
                            remaining_jobs = remaining_jobs - (jobs_to_add + result)
    
                        # update amount of spaces
                        remaining_ind_spaces = sum(b.ind_space_remaining for b in buildings)
                        remaining_non_ind_spaces = sum(b.non_ind_space_remaining for b in buildings)    
                        
    
    taz_jobs.loc[(taz_jobs['TAZID'] == tazid), 'jobs{}_distributed'.format(sector)] = taz_jobs['jobs{}_distributed'.format(sector)] + jobs_distributed
    taz_jobs.loc[(taz_jobs['TAZID'] == tazid), 'jobs{}_leftover'.format(sector)] = taz_jobs['jobs{}_leftover'.format(sector)] + remaining_jobs    

    return remaining_jobs

def convert(df, col, sector_id):
    df = df.reindex(df.index.repeat(df[col]))[['building_id', 'cid']].copy()
    df['sector_id'] = sector_id
    return df

#========================================
# setup output environment
#========================================

sys.stdout = Unbuffered(sys.stdout)

if not os.path.exists('Outputs'):
    os.makedirs('Outputs')
    
outputs = ['.\\Outputs', "job_spaces_2019_v3.gdb"]
gdb = os.path.join(outputs[0], outputs[1])

if not arcpy.Exists(gdb):
    arcpy.CreateFileGDB_management(outputs[0], outputs[1])
    
#========================================
# data sources
#========================================

# store paths
taz = r".\Inputs\WF_v9.0_TAZ\TAZ.shp" 
jobs_spreadsheet = '.\Inputs\Job Space Calculation 20220405.xlsx'

# load parcels (buildings)
print('reading parcels(buildings)...')
parcels_jspaces = pd.DataFrame.spatial.from_featureclass(r'.\Outputs\job_spaces_2019_v2.gdb\_01_parcels_jobs_2019')
parcels_jspaces = parcels_jspaces[(parcels_jspaces['building_type_id'].isin([3,4,5,6,7,9,10,11,13,15,16]) == True) & ((parcels_jspaces['ind_spaces']>0) | (parcels_jspaces['non_ind_spaces']>0))]

#------------------
# prep taz table
#------------------

print('reading in taz...')

# read in taz shapefile
taz_df = pd.DataFrame.spatial.from_featureclass(taz)

# read in EXPORT table from jobs spreadsheet
js_df = pd.read_excel(jobs_spreadsheet, index_col=0, sheet_name='EXPORT')     
    
# add jobs to TAZ geometry
taz_jobs = taz_df.merge(js_df, left_on='TAZID', right_on='TAZID', how='left')
taz_jobs = taz_jobs[['TAZID','CO_NAME','jobs','jobs1','jobs2', 'jobs3', 'jobs4', 'jobs5','jobs6', 'jobs7', 'jobs8','jobs9','jobs10', 'jobs11', 'SHAPE']].copy()
taz_jobs = taz_jobs.fillna(0)
taz_jobs = taz_jobs.sort_values('jobs', ascending=False)

taz_jobs['non_ind_jobs'] = (taz_jobs['jobs1'] + taz_jobs['jobs2'] + taz_jobs['jobs3'] + taz_jobs['jobs4']  + taz_jobs['jobs6'] + 
                            taz_jobs['jobs7'] + taz_jobs['jobs8'] + taz_jobs['jobs9'] + taz_jobs['jobs11'])

taz_jobs['ind_jobs'] = (taz_jobs['jobs5'] + taz_jobs['jobs10'])

# cast id to int
taz_jobs['TAZID'] = taz_jobs['TAZID'].astype(int)

# create tracker columns
for n in range(1,13):
    if n not in [2,8,11,12]:
        taz_jobs["jobs{}_distributed".format(n)] = 0
        taz_jobs["jobs{}_leftover".format(n)] = 0



# for testing specific taz
#taz_jobs = taz_jobs[taz_jobs['TAZID'] == 965]

# create taz tuples to iterate through
taz_tuples = list(taz_jobs[['TAZID','CO_NAME','jobs','jobs1','jobs2', 'jobs3', 'jobs4', 'jobs5','jobs6', 'jobs7', 'jobs8','jobs9','jobs10', 'jobs11', 'non_ind_jobs', 'ind_jobs']].to_records(index=False))   
    
# subset for testing; comment this out for full run
#taz_tuples = taz_tuples[0:100]

#========================================
# main loop
#========================================

print('starting main loop..')
n = len(taz_tuples)
processed_buildings = []
j = 1
for t in taz_tuples:
     
    # get values from tuple
    tazid = int(t[0])
    print('Working on TAZID:{}, {} out of {}'.format(tazid, j, n))
    jobs1 = int(t[3])
    #jobs2 = int(t[4])
    jobs3 = int(t[5])
    jobs4 = int(t[6])
    jobs5 = int(t[7])
    jobs6 = int(t[8])
    jobs7 = int(t[9])
    #jobs8 = int(t[10])
    jobs9 = int(t[11])
    jobs10 = int(t[12])
    #jobs11 = int(t[13])
    
    non_ind_jobs = int(t[14])
    ind_jobs = int(t[15])

    taz_parcels = parcels_jspaces[(parcels_jspaces['TAZID_900']== tazid)].copy()

    # create list of buildings
    global buildings
    buildings = [building(r.parcel_id_REMM, r.TAZID_900, r.CO_NAME, r.building_type_id, r.ind_spaces, r.non_ind_spaces) for r in taz_parcels.itertuples()]
    
    #-------------------
    # distribute jobs
    #-------------------
    
    if len(buildings) > 0:
        
        # ideal fit
        remaining_jobs1 = distribute_jobs(1, buildings, jobs1, 'match', "non_ind")
        #remaining_jobs2 = distribute_jobs(2, buildings, jobs2, 'match', "non_ind")
        remaining_jobs3 = distribute_jobs(3, buildings, jobs3, 'match', "non_ind")
        remaining_jobs4 = distribute_jobs(4, buildings, jobs4, 'match', "non_ind")
        remaining_jobs5 = distribute_jobs(5, buildings, jobs5, 'match', "ind")
        remaining_jobs6 = distribute_jobs(6, buildings, jobs6, 'match', "non_ind")
        remaining_jobs7 = distribute_jobs(7, buildings, jobs7, 'match', "non_ind")
        #remaining_jobs8 = distribute_jobs(8, buildings, jobs8, 'match', "non_ind")
        remaining_jobs9 = distribute_jobs(9, buildings, jobs9, 'match', "non_ind")
        remaining_jobs10 = distribute_jobs(10, buildings, jobs10, 'match', "ind")
        #remaining_jobs11 = distribute_jobs(11, buildings, jobs11, 'match', "non_ind")
        
        
        # non ideal fit
        distribute_jobs(1, buildings, remaining_jobs1, 'dont_match', "non_ind")
        #distribute_jobs(2, buildings, remaining_jobs2, 'dont_match', "non_ind")
        distribute_jobs(3, buildings, remaining_jobs3, 'dont_match', "non_ind")
        distribute_jobs(4, buildings, remaining_jobs4, 'dont_match', "non_ind")
        distribute_jobs(5, buildings, remaining_jobs5, 'dont_match', "ind")
        distribute_jobs(6, buildings, remaining_jobs6, 'dont_match', "non_ind")
        distribute_jobs(7, buildings, remaining_jobs7, 'dont_match', "non_ind")
        #distribute_jobs(8, buildings, remaining_jobs8, 'dont_match', "non_ind")
        distribute_jobs(9, buildings, remaining_jobs9, 'dont_match', "non_ind")
        distribute_jobs(10, buildings, remaining_jobs10, 'dont_match', "ind")
        #distribute_jobs(11, buildings, remaining_jobs11, 'dont_match', "non_ind")
        
        # export the buildings to csv format
        building_list = [b.convert_2_list() for b in buildings]
        processed_buildings.extend(building_list)
        
    taz_jobs.loc[(taz_jobs['TAZID'] == tazid), 'tracker'] = 1
    
    # increment counter
    j = j + 1
    
# export taz
print('Exporting results...')
taz_jobs.spatial.to_featureclass(location=os.path.join(gdb, '_02_taz_jobs_dist_output'),sanitize_columns=False)
taz_jobs.to_csv(r'.\Outputs\jobs_by_taz.csv', index=False)

# convert list of buildings to dataframe and export
df = pd.DataFrame(processed_buildings, columns =['building_id', 'building_type_id', 'non_ind_spaces', 'ind_spaces', 'taz_id', 'cid', 'jobs1', 'jobs2', 'jobs3', 'jobs4', 'jobs5', 'jobs6', 'jobs7', 'jobs8', 'jobs9', 'jobs10', 'jobs11'], dtype=int)
df.to_csv(r'.\outputs\jobs_by_building.csv', index=False)

# create individual job table
cols = ['jobs1','jobs2','jobs3','jobs4','jobs5','jobs6','jobs7','jobs8','jobs9','jobs10','jobs11']
f = pd.concat([convert(df,c, int(c.split("s",1)[1])) for c in cols]).reset_index(drop=True)
f['jobs_id'] = f.index
f = f[['jobs_id','building_id','cid','sector_id']].copy()
f.to_csv(r'.\outputs\jobs.csv', index=False)