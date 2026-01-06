
### Example code to benchmark Zarr chunking for read/write ###

import xarray as xr
import matplotlib.pyplot as plt
import datetime as dt
import fsspec


# Load in SST data from leatherback iSDM project
sst_path = "/Users/joshcullen/Documents/UCSC_NOAA_Projects/Projects/leatherback_iSDM/data_processed/rasters/sst.nc"
ds = xr.open_dataarray(sst_path)

# View size of dataset
print(f"Size in TB: {ds.nbytes / 1e12:.2f} TB")
ds.sizes
print(ds)



# Viz latest SST layer
ds.sel(time=ds.time.max()).plot()
plt.close()


### Define different chunking schemes ###
d_monthly = ds.chunk({
    "time": 31  # uniform chunks except last chunk (Zarr requirement)
})

d_annually = ds.chunk({
    "time": 365
})

d_st1 = ds.chunk({
    "time": 31,  # uniform chunks except last chunk (Zarr requirement)
    "latitude": 500,
    "longitude": 500
})

d_st2 = ds.chunk({
    "time": 365,  # uniform chunks except last chunk (Zarr requirement)
    "latitude": 500,
    "longitude": 500
})



### Write Zarr file ###

# Annually
%%time
d_annually.to_zarr(
    "./zarr_annually",
    mode="w",
    consolidated=False
)
#took 2.5 min to run

# Monthly
%%time
d_monthly.to_zarr(
    "./zarr_monthly",
    mode="w",
    consolidated=False
)
#took 35 sec to run

# Monthly & space
%%time
d_st1.to_zarr(
    "./zarr_month_space",
    mode="w",
    consolidated=False
)
#took 2.25 min to run

# Annually & space
%%time
d_st2.to_zarr(
    "./zarr_annual_space",
    mode="w",
    consolidated=False
)
#took 1.5 min to run





### Read Zarr files ###

# Monthly
ds_monthly = xr.open_dataset("./zarr_monthly", engine='zarr', consolidated=False)
print(ds_monthly)
ds_monthly.chunks

dc_monthly = ds_monthly['thetao'].sel(
  latitude=slice(-20, 50), 
  longitude=slice(220, 260), 
  time=slice('2000-01-01', '2025-12-31')
  )
print(f"Size in GB: {dc_monthly.nbytes / 1e9:.2f} GB")  #30 GB
dc_monthly.sizes
print(dc_monthly)


%%time
# Load the data into memory so the next steps are fast
dc_monthly.load()  #21 sec to load

dc_monthly.sel(time=dc_monthly.time.max()).plot()
plt.close()


dc_monthly2 = ds_monthly['thetao'].sel(
  latitude=slice(-20, 50), 
  longitude=slice(220, 260), 
  time=slice('2025-12-31', '2025-12-31')
  )

%%time
# Load the data into memory so the next steps are fast
dc_monthly2.load()  #1.5 ms to load




# Annually
ds_annually = xr.open_dataset("./zarr_annually", engine='zarr', consolidated=False)
print(ds_annually)
ds_annually.chunks

dc_annually = ds_annually['thetao'].sel(
  latitude=slice(-20, 50), 
  longitude=slice(220, 260), 
  time=slice('2000-01-01', '2025-12-31')
  )
print(f"Size in GB: {dc_annually.nbytes / 1e9:.2f} GB")  #30 GB
dc_annually.sizes
print(dc_annually)


%%time
# Load the data into memory so the next steps are fast
dc_annually.load()  #40 sec to load

dc_annually.sel(time=dc_annually.time.max()).plot()
plt.close()


dc_annually2 = ds_annually['thetao'].sel(
  latitude=slice(-20, 50), 
  longitude=slice(220, 260), 
  time=slice('2025-12-31', '2025-12-31')
  )

%%time
# Load the data into memory so the next steps are fast
dc_annually2.load()  #1.2 ms to load




# Monthly, space
ds_st1 = xr.open_dataset("./zarr_month_space", engine='zarr', consolidated=False)
print(ds_st1)
ds_st1.chunks

dc_st1 = ds_st1['thetao'].sel(
  latitude=slice(-20, 50), 
  longitude=slice(220, 260), 
  time=slice('2000-01-01', '2025-12-31')
  )
print(f"Size in GB: {dc_st1.nbytes / 1e9:.2f} GB")  #30 GB
dc_st1.sizes
print(dc_st1)


%%time
# Load the data into memory so the next steps are fast
dc_st1.load()  #36 sec to load

dc_st1.sel(time=dc_st1.time.max()).plot()
plt.close()


dc_st1_2 = ds_st1['thetao'].sel(
  latitude=slice(-20, 50), 
  longitude=slice(220, 260), 
  time=slice('2025-12-31', '2025-12-31')
  )

%%time
# Load the data into memory so the next steps are fast
dc_st1_2.load()  #1.5 ms to load




# Annually, space
ds_st2 = xr.open_dataset("./zarr_annual_space", engine='zarr', consolidated=False)
print(ds_st2)
ds_st2.chunks

dc_st2 = ds_st2['thetao'].sel(
  latitude=slice(-20, 50), 
  longitude=slice(220, 260), 
  time=slice('2000-01-01', '2025-12-31')
  )
print(f"Size in GB: {dc_st2.nbytes / 1e9:.2f} GB")  #30 GB
dc_st2.sizes
print(dc_st2)


%%time
# Load the data into memory so the next steps are fast
dc_st2.load()  #36 sec to load

dc_st2.sel(time=dc_st2.time.max()).plot()
plt.close()


dc_st2_2 = ds_st2['thetao'].sel(
  latitude=slice(-20, 50), 
  longitude=slice(220, 260), 
  time=slice('2025-12-31', '2025-12-31')
  )

%%time
# Load the data into memory so the next steps are fast
dc_st2_2.load()  #1.5 ms to load






### Append single day of data ###

# Create single layer to append (w/ new datetime value)
test_dat = ds_monthly.sel(time="2025-04-06").expand_dims("time")
print(test_dat)

test_dat["time"] = [dt.datetime.today()]
print(test_dat)


# Annually
%%time
test_dat.to_zarr(
    "./zarr_annually",
    mode="a-",  # append only specified dims
    append_dim="time",
    consolidated=False
)
#took 2.3 sec to run

# Monthly
%%time
test_dat.to_zarr(
    "./zarr_monthly",
    mode="a-",  # append only specified dims
    append_dim="time",
    consolidated=False
)
#took 157 ms to run

# Monthly & space
%%time
test_dat.to_zarr(
    "./zarr_month_space",
    mode="a-",  # append only specified dims
    append_dim="time",
    consolidated=False
)
#took 80 ms to run

# Annually & space
%%time
test_dat.to_zarr(
    "./zarr_annual_space",
    mode="a-",  # append only specified dims
    append_dim="time",
    consolidated=False
)
#took 600 ms to run


# When considering all different read/write/append steps, the monthly chunking seems to be best (although a case could be made for month-space chunking)