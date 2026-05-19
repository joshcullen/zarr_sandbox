
### Test for writing csv to public NODD bucket ###

library(terra)
library(stars)
library(readr)
library(dplyr)


# 1. Translate the gs:// path to a public HTTPS URL
gcs_http_url <- "https://storage.googleapis.com/nmfs_odp_swfsc/ESD/climate-ecosystems/gebco_2024.nc"

# Stream in from cloud
gebco_stars <- read_stars(gcs_http_url)

# Convert to terra SpatRaster
depth <- rast(gebco_stars) |> 
  flip()

# plot(depth)


## Write CSV
df <- as.data.frame(depth, xy = TRUE) |> 
  rename(depth = gebco_2024.nc)
write_csv(df, "")