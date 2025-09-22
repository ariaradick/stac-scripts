import sys,os
import json
import requests

def main(catalog_url, directory):
    cat_url = catalog_url.rstrip("/") + "/"
    files = [os.path.join(dirpath,f) for (dirpath, dirnames, filenames) in 
             os.walk(directory) for f in filenames]

    collections = []
    items = []

    for f in files:
        fname = os.path.basename(f)
        if "catalog" in fname:
            continue
        elif "collection" in fname:
            collections.append(f)
        else:
            items.append(f)
    
    for c in collections:
        with open(c,'r') as file:
            response = requests.post(
                cat_url + "collections",
                json=json.load(file)
            )
    for i in items:
        with open(i,'r') as file:
            j = json.load(file)
            collection_id = j["collection"]
            response = requests.post(
                cat_url + f"collections/{collection_id}/items",
                json=j
            )
            print(j["id"], response)

if __name__ == "__main__":
    main(sys.argv[2], os.path.abspath(sys.argv[1]))
