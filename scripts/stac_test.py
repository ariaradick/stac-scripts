from dataclasses import dataclass
import pystac
import os,sys
import xarray as xr
from netCDF4 import Dataset
import numpy as np
from datetime import datetime, timezone
from shapely.geometry import Polygon, mapping
import time

from stac_utils import MetadataSlowLoader, convert_timerange

dirs = [None, None, None, None, 'experiment_id', 'member_id', 'realm', 'cell_methods', 'frequency', 'chunk_freq']
fname = ["realm", "time_range", "variable_id"]
all_columns = ["activity_id", "institution_id", "source_id", "experiment_id",
                "frequency", "realm", "table_id",
                "member_id", "grid_label", "variable_id",
                "time_range", "chunk_freq","platform","dimensions",
                "cell_methods","standard_name","path"]
props_template = {c : '' for c in all_columns}
props_template["project"] = "SPEAR-FLP"
props_template["product"] = "model-output"
props_template["institution_id"] = "NOAA-GFDL"
props_template["source_id"] = "SPEAR-MED"
temporal_extents = {"SPEAR_c192_o1_Scen_SSP585_IC2011_K50" : 
                    [datetime(2011,1,1,0), datetime(2100, 12, 31, 23)],
                    "SPEAR_c192_o1_Hist_AllForc_IC1921_K50" : 
                    [datetime(1921, 1, 1, 0), datetime(2010, 12, 31, 23)]}
exp_titles = {
    "SPEAR_c192_o1_Scen_SSP585_IC2011_K50" : "Scenario SSP5-8.5",
    "SPEAR_c192_o1_Hist_AllForc_IC1921_K50" : "Historical"
}



def get_metadata(path_to_file, dir_meta, file_meta, properties=props_template):
    d = dict(properties)
    filename = os.path.basename(path_to_file).split('.')
    dir_structure = os.path.dirname(path_to_file).split('/')[1:]
    for (i,x) in enumerate(dir_meta):
        if x is not None:
            d[x] = dir_structure[i]
    for (i,x) in enumerate(file_meta):
        if x is not None:
            d[x] = filename[i]
    starttime, endtime = convert_timerange(d["time_range"])
    return (d, starttime, endtime)

# [-89.75, 89.75] ; [0.3125, 359.6875]
# two collections, one for Hist and one for SSP585. the temporal extent is 
# hard-coded, so need to figure out a better way to do this
def make_catalog(directory, dir_meta=dirs, f_meta=fname):
    catalog = pystac.Catalog(id="test-catalog", description="Test Catalog")
    
    item_dicts = {}
    slow_metadata = MetadataSlowLoader()

    files = [os.path.join(dirpath,f) for (dirpath, dirnames, filenames) in 
             os.walk(directory) for f in filenames]
    files.sort()
    # N_files = len(files)
    # digits_files = int(np.ceil(np.log10(N_files)))
    # IDs = [str(i).zfill(digits_files) for i in range(N_files)]

    for i,f in enumerate(files):
        # print(f)
        metadata_d, stime, etime = get_metadata(f, dir_meta, f_meta)

        exp_id = metadata_d["experiment_id"]
        if exp_id not in item_dicts:
            item_dicts[exp_id] = {}
            print('here')
        exp_dict = item_dicts[exp_id]

        var_id = metadata_d["variable_id"]
        if var_id not in exp_dict:
            bbox, footprint, long_name, units = slow_metadata.get(f, metadata_d)
            metadata_d["standard_name"] = long_name
            metadata_d["variable_units"] = units

            exp_dict[var_id] = pystac.Item(id=var_id, geometry=footprint, 
                bbox=bbox, properties=dict(metadata_d), start_datetime=stime, 
                end_datetime=etime, datetime=None)
            
            del exp_dict[var_id].properties["time_range"]
            del exp_dict[var_id].properties["member_id"]
            del exp_dict[var_id].properties["path"]

        else:
            item_start = datetime.fromisoformat(exp_dict[var_id].properties["start_datetime"])
            if stime < item_start:
                exp_dict[var_id].properties["start_datetime"] = stime.replace(tzinfo=None).isoformat()+'Z'

            item_end = datetime.fromisoformat(exp_dict[var_id].properties["end_datetime"])
            if etime > item_end:
                exp_dict[var_id].properties["end_datetime"] = etime.replace(tzinfo=None).isoformat()+'Z'

        asset = pystac.Asset(
            href=f,
            title="{}.{}".format(
                metadata_d["time_range"], metadata_d["member_id"]
            ),
            media_type="application/netcdf",
            roles=["data"]
        )

        exp_dict[var_id].add_asset(
            key=asset.title,
            asset=asset
        )
    
    collections = []
    for (k,v) in item_dicts.items():
        unique_bbox_set = set()
        for item in v.values():
            unique_bbox_set.add(tuple(item.bbox))
        unique_bbox_list = []
        for tup in unique_bbox_set:
            unique_bbox_list.append(list(tup))
        collections.append(pystac.Collection(
                id=k,
                description="NA",
                extent=pystac.collection.Extent(
                    pystac.collection.SpatialExtent(unique_bbox_list),
                    pystac.collection.TemporalExtent([
                        temporal_extents[k]
                    ])
                ),
                summaries=pystac.Summaries(
                    {
                        kk : list(set([
                            item.properties[kk] for item in v.values()
                        ]))
                        for kk in v["snow"].properties.keys()
                    }
                )
            ))
        for item in v.values():
            collections[-1].add_item(item)
    
    for coll in collections:
        catalog.add_child(coll)

    return catalog

def main(directory):
    c = make_catalog(directory)
    c.normalize_hrefs(os.path.join(directory, "catalog"))
    c.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

if __name__ == "__main__":
    start = time.time()
    main(os.path.abspath(sys.argv[1]))
    print(time.time() - start)
    