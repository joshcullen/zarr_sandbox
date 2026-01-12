
### Example code for storing multiple variables in single Zarr file ###

import xarray as xr
import zarr
import matplotlib.pyplot as plt


# Load in SST data from leatherback iSDM project
sst_path = "/Users/joshcullen/Documents/UCSC_NOAA_Projects/Projects/leatherback_iSDM/data_processed/rasters/sst.nc"
ds_sst = xr.open_dataset(sst_path)

# View size of dataset
print(f"Size in TB: {ds_sst.nbytes / 1e12:.2f} TB")
ds_sst.sizes
print(ds_sst)


# Load in MLD data from leatherback iSDM project
mld_path = "/Users/joshcullen/Documents/UCSC_NOAA_Projects/Projects/leatherback_iSDM/data_processed/rasters/mld.nc"
ds_mld = xr.open_dataset(mld_path)

# View size of dataset
print(f"Size in TB: {ds_mld.nbytes / 1e12:.2f} TB")
ds_mld.sizes
print(ds_mld)


# Load in SSS data from leatherback iSDM project
sss_path = "/Users/joshcullen/Documents/UCSC_NOAA_Projects/Projects/leatherback_iSDM/data_processed/rasters/sss.nc"
ds_sss = xr.open_dataset(sss_path)

# View size of dataset
print(f"Size in TB: {ds_sss.nbytes / 1e12:.2f} TB")
ds_sss.sizes
print(ds_sss)


# Load in bathymetry from leatherback iSDM project
bathym_path = "/Users/joshcullen/Documents/UCSC_NOAA_Projects/Projects/leatherback_iSDM/data_processed/rasters/gebco_2024.nc"
ds_bathym = xr.open_dataset(bathym_path)

# View size of dataset
print(f"Size in TB: {ds_bathym.nbytes / 1e12:.2f} TB")
ds_bathym.sizes
print(ds_bathym)


# Merge datasets together
ds = xr.merge([ds_sst, ds_mld, ds_sss])
print(ds)

ds = ds.merge(ds_bathym, join="left")
print(ds)

ds['elevation'].plot()
plt.close()

ds['thetao'].isel(time=-1).plot()
plt.close()



### Try opening files as DataArrays and then merging into single Dataset

da_sst = xr.open_dataarray(sst_path)
da_mld = xr.open_dataarray(mld_path)
da_sss = xr.open_dataarray(sss_path)

ds2 = xr.Dataset({'sst': da_sst, 'mld': da_mld, 'sss': da_sss})
print(ds2)

ds2['sst'].isel(time=-1).plot()
plt.close()

ds2['mld'].isel(time=-1).plot()
plt.close()

ds2['sss'].isel(time=-1).plot()
plt.close()



### Export subset of data as Zarr file

# Subset
ds_sub = ds.sel(
  latitude=slice(20, 50), 
  longitude=slice(220, 260), 
  time=slice('2020-01-01', '2025-12-31')
  )
print(f"Size in GB: {ds_sub.nbytes / 1e9:.2f} GB")  #4 GB
ds_sub.sizes
print(ds_sub)

# Chunk
ds_sub = ds_sub.chunk({
    "time": 31  # uniform chunks except last chunk (Zarr requirement)
})
ds_sub.chunks


# Write Zarr file
%%time
ds_sub.to_zarr(
    "./zarr_multivar",
    mode="w-",
    consolidated=False
)
#took x6 min to run