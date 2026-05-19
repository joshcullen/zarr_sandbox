

### Test for writing png to public NODD bucket ###

import xarray as xr
import gcsfs
import matplotlib.pyplot as plt

# 1. Define the GCS path
path = 'gs://nmfs_odp_swfsc/ESD/climate-ecosystems/gebco_2024.nc'

# 2. Initialize the GCS filesystem 
# If the bucket is public, use token='anon'. 
# For private buckets, omit the token to use your local GCP credentials.
fs = gcsfs.GCSFileSystem(token='anon')

# 3. Create a file object
file_obj = fs.open(path)

# 4. Open the dataset with xarray
# Note: Using the 'h5netcdf' engine is often faster for cloud-hosted netCDF4 files
ds = xr.open_dataset(file_obj, engine='h5netcdf')

# Plot the entire dataset (or a specific variable)
# Using 'robust=True' handles outliers and improves color contrast
ds.elevation.plot(figsize=(12, 6), cmap='viridis', robust=True)
plt.title("GEBCO 2024 Elevation")

# 3. Save the figure
# 'bbox_inches="tight"' ensures labels aren't cut off at the edges
cloud_img_path = 'gs://nmfs_odp_swfsc/ESD/climate-ecosystems/gebco_elevation_map.png'

with fs.open(cloud_img_path, 'wb') as f:
    plt.savefig(f, format='png', dpi=300, bbox_inches='tight')

# 4. Clear/close the plot memory
plt.close()

print(f"Plot successfully saved to: {cloud_img_path}")