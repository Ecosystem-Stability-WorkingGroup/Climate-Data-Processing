###### BEFORE RUNNING #########

# - Line 22: Set path to terrestrial site data
# - Line 47: Set path to Global SPEI Database file: spei01.nc
# - Lines 108-109: Set working directory to file save location, remove "#" from write.csv code lines

############################################################
# Load Packages
librarian::shelf(raster, sf, tidyverse, terra, fs, httr, rvest, ncdf4, progress, here)

############################################################
# Initial Dataframes - All Sites with Standardized IDs

all.climate.terr.sites<-read.csv(here("Data" , "Climate_data" , "all.climate.terr.sites.csv"))

#Remove FIA Data Until Site Crosscheck
all.climate.terr.sites<-all.climate.terr.sites%>%     
  filter(!grepl("FIA", std_id, ignore.case = TRUE))  

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

# Function to extract SPEI data for given latitude and longitude
extract_spei_data <- function(lon_value, lat_value, std_id, ecosystem) {
  lon_index <- which.min(abs(lon - lon_value))
  lat_index <- which.min(abs(lat - lat_value))
  spei_values <- ncdf4::ncvar_get(nc_file, "spei", start = c(lon_index, lat_index, 1), count = c(1, 1, -1))
  
  data.frame(
    Std_id = std_id,
    Latitude = lat_value,
    Longitude = lon_value,
    Ecosystem = ecosystem,
    Date = start_date,
    Year = year(start_date),
    Month = month(start_date),
    SPEI = spei_values
  )
}

############################################################
# Run for Data and Close NetCDF

#Run for Dataset
spei.climate.dat.full <- spei.climate.dat %>%
  rowwise() %>%
  do(extract_spei_data(.$longitude, .$latitude, .$std_id, .$ecosystem)) %>%
  ungroup()

#Reformat
spei.climate.dat.fin<-spei.climate.dat.full%>%
  dplyr::select(Std_id, Latitude, Longitude, Ecosystem, Year, Month, SPEI)%>%
  rename("month_spei" = "SPEI",
         "std_id" = "Std_id",
         "latitude" = "Latitude",
         "longitude" = "Longitude",
         "ecosystem" = "Ecosystem")

# Close the NetCDF file after extraction
ncdf4::nc_close(nc_file)


########################################################################
#Time Logs
time_done_spei<-Sys.time()

time_done_spei - time_start

########################################################################
#Save Files:

#setwd(path/to/save/location)
#write.csv(spei.climate.dat.fin, file="spei.climate.dat.fin.csv", row.names = F)
