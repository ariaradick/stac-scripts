import sys,os,subprocess

def main(directory):
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
        subprocess.run(["pypgstac", "load", "collections", c])
    for i in items:
        subprocess.run(["pypgstac", "load", "items", i])

if __name__ == "__main__":
    main(os.path.abspath(sys.argv[1]))