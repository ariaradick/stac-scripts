import sys
import polars as pl
import xarray as xr
import dask
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from pathlib import Path

tstr = "%Y-%m-%d"

catalog = "https://raw.githubusercontent.com/NOAA-GFDL/spear-flp/refs/heads/main/catalog_blue.csv"
output_path_template = ['experiment_id', 'member_id', 'realm']
output_file_template = ['variable_id']
base_path = '/work/a3r/Documents/code/stac-scripts/catalog_thumbs/tftest_ens/'
overwrite = True

def time_str(da_time):
    times = [
        da_time.min().values.tolist().strftime(tstr),
        da_time.max().values.tolist().strftime(tstr)
    ]
    return f'{times[0]} – {times[1]}'

class EnsemblePlot:
    def __init__(
            self,
            catalog_file,
            base_output_path,
            output_path_template,
            output_file_template,
            overwrite=overwrite,
            catalog_filter=None
        ):

        df1 = pl.read_csv(catalog_file)
        if catalog_filter is not None:
            df = df1.filter(catalog_filter)
        self.catalog = df[
            [s.name for s in df if not (s.null_count() == df.height)]
        ].drop(['pass_qc','who_qc'], strict=False)

        self.item_columns = self.catalog.columns
        for i in ['path', 'time_range']:
            self.item_columns.remove(i)

        self.base_output_path = base_output_path
        self.output_path_template = output_path_template
        self.output_file_template = output_file_template

        self.overwrite = overwrite

    def get_thumb_path(self, group, create=False):
        item_metadata = { c:v for c,v in zip(self.item_columns, group) }
        path = self.base_output_path + '/'.join(
                [item_metadata[o] for o in self.output_path_template]
            ) + '/'
        if create:
            Path(path).mkdir(parents=True, exist_ok=True)
        fname = '_'.join([item_metadata[o] for o in self.output_file_template]) + '.png'
        return path+fname

    def _make_plot(self, subcat):
        ds = xr.open_mfdataset(
            subcat['path'],
            parallel=True,
            chunks='auto',
            engine='netcdf4'
        )[subcat['variable_id'][0]]
        ds_avg = dask.compute(ds.mean(dim='time', keep_attrs=True))[0]

        time_range_str = time_str(ds.time)

        fig = plt.figure(figsize = (10, 5))
        axis = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
        axis.coastlines()
        p = ds_avg.plot(ax = axis)#, add_colorbar=False)
        # plt.colorbar(mappable=p, label=f"{ds_avg.name} [{ds_avg.attrs['units']}]")
        plt.title(f"Average {ds_avg.name} {time_range_str}")
        # plt.title(f"{ds_avg.attrs['long_name']} {time_str}")
        plt.text(0, -0.1, 'GFDL-SPEAR-MED', ha='left', va='bottom', transform=axis.transAxes)
        plt.text(0.5, -0.1, subcat['experiment_id'][0], ha='center', va='bottom', transform=axis.transAxes)
        plt.text(1, -0.1, subcat['member_id'][0], ha='right', va='bottom', transform=axis.transAxes)
        print('here')
        return fig

    def gen_plots(self):
        for grp, df in self.catalog.group_by(self.item_columns):
            print(grp)
            thumb_file = Path(self.get_thumb_path(grp, create=True))
            print(thumb_file)
            print(thumb_file.exists())
            if (not self.overwrite) and thumb_file.exists():
                continue

            f = self._make_plot(df)
            f.savefig(str(thumb_file), bbox_inches="tight")
            plt.close()

def main(catalog_url, member_id):
    ensPlot = EnsemblePlot(
        catalog_url,
        base_path,
        output_path_template,
        output_file_template,
        catalog_filter=(pl.col('member_id') == member_id)
    )

    ensPlot.gen_plots()

if __name__=='__main__':
    ens_id = f'pp_ens_{sys.argv[1].zfill(2)}'
    main(catalog, ens_id)
