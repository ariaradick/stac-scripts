import pystac
from pystac_client import Client

class STAC_Search:

    def __init__(self, catalog_url, collections=None, datetime=None,
                 search_params={}):
        self.catalog_url = catalog_url
        self.client = Client.open(catalog_url)
        self.queryables, self.asset_queryables = self.get_queryables()
        self.collections=collections
        self.datetime=datetime
        self.item_params = {}
        self.asset_params = {}
        self._setSearchParams(search_params)
    
    def _getCollectionIDs(self):
        return [collection.id for collection in self.client.get_collections()]
    
    def _getQueryables(self, *args):
        queryables = []
        asset_queryables = []
        if len(args) > 0:
            queryables_dict = self.client.get_merged_queryables(args)
        else:
            queryables_dict = self.client.get_merged_queryables(
                self._getCollectionIDs()
            )
        for k in queryables_dict['properties'].keys():
            if k.startswith('assets.'):
                asset_queryables.append(k[7:])
            else:
                queryables.append(k)
        return queryables, asset_queryables
    
    def _setSearchParams(self, search_params):
        for k,v in search_params.items():
            if k in self.queryables:
                self.item_params[k] = v
            elif k in self.asset_queryables:
                self.asset_params[k] = v
            else:
                print(f"Warning! Key {k} not in queryable metadata. Skipping...")

    def _get_cql2_str(self, d : dict):
        cql2_strings = []
        for k,v in self.item_params.items():
            cql2_strings.append(f"{k}='{v}'")
        for k,v in self.asset_params.items():
            cql2_strings.append(f"assets.{k}='{v}'")
        return " AND ".join(cql2_strings)

    def asset_filter(self, item : pystac.Item):
        paths = []
        for asset_id, asset in item.assets:
            truths = [asset.extra_fields[k] == v for k,v in self.asset_params.items()]
            truths.append( asset.extra_fields['start_datetime'] <= 
                           self.datetime <= asset.extra_fields['end_datetime'] )
            if all(truths):
                paths.append(asset.get_absolute_href())
        return paths

    def get_paths(self):
        itemSearch = self.client.search(
            collections=self.collections,
            datetime=self.datetime,
            filter=_dict_to_cql2(self.search_params)
        )
        path_list = []
        for item in itemSearch.items():
            path_list.extend(self.asset_filter(item))
        return path_list