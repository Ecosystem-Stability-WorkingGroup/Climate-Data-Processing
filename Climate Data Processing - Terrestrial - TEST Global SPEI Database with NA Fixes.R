###### BEFORE RUNNING #########

# - Line 14: Set path to terrestrial site data
# - Line 39: Set path to Global SPEI Database file: spei01.nc
# - Lines 121-122: Set working directory to file save location, remove "#" from write.csv code lines

############################################################
# Load Packages
librarian::shelf(raster, sf, tidyverse, terra, fs, httr, purrr, sp, rvest, ncdf4, progress, here)

############################################################
# Initial Dataframes - All Sites with Standardized IDs

all.climate.terr.sites<-read.csv(here("Data" , "Climate_data" , "all.climate.terr.sites.csv"))

#Remove FIA Data Until Site Crosscheck
#all.climate.terr.sites<-all.climate.terr.sites%>%     
#  filter(!grepl("FIA", std_id, ignore.case = TRUE))  

########################################################################################################################
########################################################################################################################
########################################################################################################################
# Step 5
# SPEI Global Database
########################################################################################################################
########################################################################################################################
########################################################################################################################

time_start<-Sys.time() #Saving Time for Reference

############################################################
# Load Site Data
spei.climate.dat<-all.climate.terr.sites

############################################################
# Set NetCDF Location

# Open the NetCDF file
nc_file <- ncdf4::nc_open(here("Data", "Climate_data", "Global SPEI Database", "spei01.nc"))

############################################################
# Pulling Data from NetCDF

# Extract latitude, longitude, and time data from the NetCDF file
lon <- ncdf4::ncvar_get(nc_file, "lon")
lat <- ncdf4::ncvar_get(nc_file, "lat")
time <- ncdf4::ncvar_get(nc_file, "time")
time_units <- ncdf4::ncatt_get(nc_file, "time", "units")$value
start_date <- as.Date("1900-01-01") + time

# Function to find the nearest non-NA value
find_nearest_non_na <- function(lon_value, lat_value) {
  lon_index <- which.min(abs(lon - lon_value))
  lat_index <- which.min(abs(lat - lat_value))
  
  spei_values <- ncdf4::ncvar_get(nc_file, "spei", start = c(1, 1, 1), 
                                  count = c(-1, -1, -1))
  
  # Get indices where SPEI values are non-NA
  valid_indices <- which(!is.na(spei_values), arr.ind = TRUE)
  
  if (length(valid_indices) == 0) {
    return(rep(NA, length(start_date)))  # No non-NA values found
  }
  
  # Calculate distances between the target point and all valid points
  distances <- sqrt((lon[valid_indices[, 1]] - lon_value)^2 + 
                      (lat[valid_indices[, 2]] - lat_value)^2)
  
  # Find the index of the closest non-NA point
  nearest_index <- valid_indices[which.min(distances), ]
  
  # Extract the SPEI values at the closest non-NA point
  nearest_spei_values <- spei_values[nearest_index[1], nearest_index[2], ]
  
  return(nearest_spei_values)
}

# Initialize progress bar
n <- nrow(spei.climate.dat)
pb <- progress_bar$new(
  format = "  Processing [:bar] :percent | ETA: :eta",
  total = n, clear = FALSE, width = 60
)

# Apply the extraction function with progress bar
spei_values <- purrr::pmap(list(spei.climate.dat$longitude, spei.climate.dat$latitude), function(lon_value, lat_value) {
  pb$tick()  # Update progress bar
  find_nearest_non_na(lon_value, lat_value)
})

# Combine all the data into the final dataframe
spei.climate.dat.full <- spei.climate.dat %>%
  mutate(SPEI = spei_values) %>%
  unnest(SPEI) %>%
  mutate(
    Date = rep(start_date, times = nrow(spei.climate.dat)),
    Year = year(Date),
    Month = month(Date)
  ) %>%
  dplyr::select(std_id, latitude,  longitude, ecosystem, Year, Month, SPEI)

# Close the NetCDF file after extraction
ncdf4::nc_close(nc_file)

length(unique(nadat$std_id))
length(unique(spei.climate.dat.fin$std_id))

########################################################################
#Time Logs
time_done_spei<-Sys.time()
time_done_spei - time_start

########################################################################
#Save Files:

#setwd(path/to/save/location)
#write.csv(spei.climate.dat.full, here("Data", "Processed_climatedata" , "spei.climate.dat.fin.csv"), row.names = F)
