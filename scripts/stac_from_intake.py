import os,sys
from stac_utils import MetadataSlowLoader, convert_timerange
import pystac
import numpy as np
import pandas as pd
import time

baseUrl = "https://noaa-gfdl-spear-large-ensembles-pds.s3.amazonaws.com/SPEAR/"
catPath = "/home/Aria.Radick/Documents/catalogs/cmip_spear-med_hist/catalog.csv"

properties = [
    "activity_id", 
    "institution_id", 
    "source_id", 
    "experiment_id",
    "frequency", 
    "realm", 
    "table_id",
    "grid_label", 
    "variable_id",
    "version_id",
    "chunk_freq",
    "platform",
    "dimensions",
    "cell_methods",
    "standard_name"
]

@np.vectorize
def _sfn(a):
    return int(a.split('i')[0][1:])

def make_add_asset(row, item):
    hyperlink = baseUrl + '/'.join(row['path'].split('/')[3:])
    a = pystac.Asset(
        href=hyperlink,
        title="{}.{}".format(
            row["member_id"],
            row["time_range"]
        ),
        media_type="application/netcdf",
        roles=["data"]
    )
    item.add_asset(key=a.title, asset=a)

def stac_from_csv(catalog_path):
    catalog = pystac.Catalog(id="test-catalog", description="Test Catalog")

    catalog_df = pd.read_csv(catalog_path).fillna('')
    metadata_reader = MetadataSlowLoader()

    item_list = []
    
    for grp,df in catalog_df.groupby(["experiment_id","table_id","variable_id"]):
        title = '.'.join(grp)

        # all metadata from reading netCDF file should be same across group
        nc_metadata = metadata_reader.get(
            df.iloc[0]['path'],
            df.iloc[0].to_dict()
        )

        # some metadata is static right now, some is contained in the assets
        props = df.iloc[0][properties].to_dict()
        props["product"] = "model-output"
        props["institution_id"] = "NOAA-GFDL"
        props["source_id"] = "SPEAR-MED"
        props["standard_name"] = nc_metadata.long_name
        props["variable_units"] = nc_metadata.units

        times = df["time_range"].apply(convert_timerange)
        start_time = times.min()[0]
        end_time = times.max()[-1]

        item = pystac.Item(
            id = title,
            geometry = nc_metadata.footprint,
            bbox = nc_metadata.bbox,
            properties = props,
            start_datetime = start_time,
            end_datetime = end_time,
            datetime = None
        )

        df.sort_values('member_id', key=_sfn, inplace=True)
        
        for i,r in df.iterrows():
            make_add_asset(r,item)

        # df.apply(make_add_asset,axis=1,args=(item,))

        item_list.append(item)

    c = pystac.Collection.from_items(item_list, id="SPEAR-MED")
    c.title = "SPEAR MED CMIP6"
    c.summaries = pystac.Summaries(
        {
            kk : list(set([
                item.properties[kk] for item in item_list
            ]))
            for kk in item_list[0].properties.keys()
        }
    )
    # c.add_asset(
    #     key="Collection Thumbnail",
    #     asset=pystac.Asset(
    #         href="/nbhome/a3r/test.png",
    #         title="Collection Thumbnail",
    #         media_type="image/png",
    #         roles=["thumbnail"]
    #     )
    # )

    catalog.add_child(c)

    return catalog

def main(directory):
    stac_cat = stac_from_csv(catPath)
    stac_cat.normalize_hrefs(os.path.join(directory, "catalog"))
    stac_cat.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

if __name__=="__main__":
    main(sys.argv[1])