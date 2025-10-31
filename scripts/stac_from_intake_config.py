import os,sys
from stac_utils import MetadataSlowLoader, convert_timerange
import pystac
import pandas as pd
import time
import yaml

def _load_config(path_to_config):
    with open(path_to_config) as f:
        try:
            config_dict = yaml.safe_load(f)
            return config_dict
        except yaml.YAMLError as exc:
            print(exc)

class STACfromCSV:
    def __init__(self,path_to_config):
        self.config = _load_config(path_to_config)
    
    def make_add_asset(self, row, item):
        if self.config['asset']['hrefPrepend']:
            
        hyperlink = baseUrl + '/'.join(row['path'].split('/'))
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

        df.apply(make_add_asset,axis=1,args=(item,))

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



def main(path_to_config):
    config = _load_config(path_to_config)
    stac_cat = stac_from_csv()
    stac_cat.normalize_hrefs(os.path.join(directory, "catalog"))
    stac_cat.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

if __name__=="__main__":
    main(sys.argv[1])