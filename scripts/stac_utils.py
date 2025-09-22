import os,sys
from dataclasses import dataclass
import numpy as np
from netCDF4 import Dataset
from datetime import datetime, timezone
from shapely.geometry import Polygon, mapping

def convert_timerange(gfdl_timerange):
    times = gfdl_timerange.split('-')
    for i,t in enumerate(times):
        if len(t) == 4:
            times[i] = t + '0101'
        elif len(t) == 6:
            times[i] = t + '01'
        elif len(t) > 8:
            times[i] = t[:8] + 'T' + t[8:]
    return [datetime.fromisoformat(t).replace(tzinfo=timezone.utc) for t in times]

def minmax(array):
    return np.array([float(np.min(array)), float(np.max(array))])

def minmax_bounded(array, bnds):
    x = minmax(array)
    while x[0] < bnds[0]:
        x += 1
    while x[1] > bnds[1]:
        x -= 1
    if x[0] < bnds[0] or x[1] > bnds[1]:
        raise ValueError("bbox out of bounds")
    return x

@dataclass
class MetadataSlow():
    """Class for holding onto metadata that is slow to get"""
    bbox: list
    footprint: Polygon
    long_name: str
    units: str

class MetadataSlowLoader():
    def __init__(self):
        self.metadata = {}

    def _get_bbox_footprint(self, lats, lons):
        bottom,top = lats
        left,right = lons

        bbox = [left, bottom, right, top]
        footprint = Polygon([
            [left, bottom],
            [left, top],
            [right, top],
            [right, bottom]
        ])

        return (bbox, mapping(footprint))
    
    def get(self, path, properties):
        var_id = properties["variable_id"]

        if var_id not in self.metadata:
            ds = Dataset(path, memory=None)

            if properties["realm"] == "ocean":
                bt = minmax_bounded(ds.variables["yh"], [-90,90])
                lr = minmax_bounded(ds.variables["xh"], [-180,180])
            else:
                bt = minmax_bounded(ds.variables["lat"], [-90,90])
                lr = minmax_bounded(ds.variables["lon"], [-180,180])

            long_name = ds.variables[var_id].long_name
            units = ds.variables[var_id].units
            bbox,fp = self._get_bbox_footprint(bt, lr)
            self.metadata[var_id] = MetadataSlow(bbox, fp, long_name, units)
        
        return self.metadata[var_id]