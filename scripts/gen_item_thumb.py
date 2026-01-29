import pandas as pd
import numpy as np
import xarray as xr
import dask
import os
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from stac_utils import convert_timerange
from pathlib import Path

tstr = "%Y-%m-%d"

catalog = "/home/Aria.Radick/Documents/catalogs/cmip_spear-med_hist/catalog.csv"

output_path_template = ['institution_id','source_id','experiment_id',"table_id","variable_id","grid_label","version_id"]
output_file_template = ["variable_id","table_id",'source_id',"experiment_id","grid_label"]
base_path = '/work/a3r/Documents/code/stac-scripts/catalog_thumbs/cmip_items/'

class BasicQAPlots:
    def __init__(self, catalog_file, base_output_path, output_path_template, 
                 output_file_template):
        self.catalog = pd.read_csv(catalog_file).dropna(axis=1, how='all')
        # self.catalog = catdf
        self.column_filter = ((self.catalog.columns != 'path') & 
                              (self.catalog.columns != 'time_range') &
                              (self.catalog.columns != 'member_id'))
        self.item_columns = self.catalog.columns[self.column_filter].to_list()
        self.base_output_path = base_output_path
        self.output_path_template = output_path_template
        self.output_file_template = output_file_template
    
    def get_paths(self, group, create=False):
        item_metadata = { c:v for c,v in zip(self.item_columns, group) }
        path = self.base_output_path + '/'.join(
                [item_metadata[o] for o in self.output_path_template]
            ) + '/' 
        if create:
            Path(path).mkdir(parents=True, exist_ok=True)
        fname = '_'.join([item_metadata[o] for o in self.output_file_template])
        csv_fname = path + fname + '.csv'
        avg_fname = path + fname + '_avg.png'
        std_fname = path + fname + '_std.png'
        return csv_fname, avg_fname, std_fname
    
    def calc_vals(self):
        for grp,df in self.catalog.groupby(self.item_columns):
            file_names = self.get_paths(grp, create=True)

            if os.path.isfile(file_names[1]) & os.path.isfile(file_names[2]):
                continue

            if not os.path.isfile(file_names[0]):
                pd.DataFrame(columns=['member_id', 'mean', 'std']).to_csv(
                    file_names[0], index=False
                )

            for m,subdf in df.groupby('member_id'):
                if m in pd.read_csv(file_names[0])['member_id'].to_list():
                    print(f"Skipping {m}")
                    continue
                
                var_id = subdf['variable_id'].unique()[0]
                ds = xr.open_mfdataset(
                    subdf['path'], 
                    parallel=True, 
                    engine='netcdf4'
                )[var_id]
                weights = np.cos(np.deg2rad(ds.lat))
                weights.name = "weights"
                avgval_lazy = ds.weighted(weights).mean()
                stdval_lazy = ds.weighted(weights).std()
                avgval, stdval = dask.compute(avgval_lazy, stdval_lazy)
                results = {
                    'member_id' : [m], 
                    'mean' : [float(avgval)], 
                    'std' : [float(stdval)]
                }
                pd.DataFrame(results).to_csv(
                    file_names[0],
                    mode='a',
                    index=False,
                    header=False
                )

def main():
    qaplots = BasicQAPlots(catalog, base_path, output_path_template, 
                output_file_template)
    qaplots.calc_vals()

if __name__=='__main__':
    main()