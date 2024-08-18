###### BEFORE RUNNING #########

# - Line 14: Set path to terrestrial site data
# - Line 39: Set path to Global SPEI Database file: spei01.nc
# - Lines 121-122: Set working directory to file save location, remove "#" from write.csv code lines

############################################################
# Load Packages
librarian::shelf(raster, sf, tidyverse, terra, fs, httr, purrr, sp, rvest, ncdf4, progress, here, parallel)

############################################################
# Initial Dataframes - All Sites with Standardized IDs
#setwd('/home/lemoinelab2/Documents/server-share/ryf_climate_data')

all.climate.terr.sites<-read.csv(here("Data" , "Climate_data", "all.climate.terr.sites.csv"))

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
nc_file <- ncdf4::nc_open(here("Data","Climate_data", "Global SPEI Database", "spei01.nc"))


############################################################
# Pulling Data from NetCDF

# Extract latitude, longitude, and time data from the NetCDF file
lon <- ncdf4::ncvar_get(nc_file, "lon")
lat <- ncdf4::ncvar_get(nc_file, "lat")
time <- ncdf4::ncvar_get(nc_file, "time")
time_units <- ncdf4::ncatt_get(nc_file, "time", "units")$value
start_date <- as.Date("1900-01-01") + time

spei_values <- ncdf4::ncvar_get(nc_file, "spei", start = c(1, 1, 1), 
                                  count = c(-1, -1, -1))

head(spei_values)

## CAspei_values## CALCULATE THE VALID LATS AND LONS ONLY ONCE!!! THIS IS SUPER SLOW AND TAKES ABOUT
## SIX SECONDS. IF YOU DO THIS FOR EVERY SINGLE ROW, IT WILL TAKE MONTHS TO RUN
valid_indices <- which(!is.na(spei_values), arr.ind = TRUE)
valid_lons = lon[valid_indices[,1]]
valid_lats = lat[valid_indices[,2]]


## VALID LONS AND VALID LATS ARE CALCULATES OVER EVERY YEAR/MONTH, SO THERE IS TONS OF REPITION. 
## dim(valid_lats) = 97324221 vs. dim(unique(valid_lats)) = 280
## SO YOU WERE CALCULATING PAIRWISE DIFFERENCES
M = cbind(valid_lons, valid_lats)
#dim(M) = 97324221, 2

# if we drop it to stop searching across every year and month
valid_indices2 <- valid_indices[valid_indices[,3]==1,]
valid_lons2 = lon[valid_indices2[,1]]
valid_lats2 = lat[valid_indices2[,2]]
M2 <- cbind(valid_lons2, valid_lats2)
# dim(M2) = 66495, 2

head(valid_indices2)

# so valid_indices# so you calculate pairwise distances to only 65,000 unique points
# instead of 97,300,000 repeated points

# Function to find the nearest non-NA value
find_nearest_non_na <- function(x, lon, lat, spei_values, M2, dates) {
  lon_index <- which.min(abs(lon - x$longitude))
  lat_index <- which.min(abs(lat - x$latitude))
  
  # Get indices where SPEI values are non-NA
  if (length(valid_indices) == 0) {
    return(rep(NA, length(dates)))  # No non-NA values found
  }

  # Calculate distances between the target point and all valid points
  distances <- abs(M2[,1] - x$longitude) + 
                      abs(M2[,2] - x$latitude)
  
  # Find the index of the closest non-NA point
  nearest_index <- valid_indices[which.min(distances), ]
  
  # Extract the SPEI values at the closest non-NA point
  nearest_spei_values <- spei_values[nearest_index[1], nearest_index[2], ]
  
  return(nearest_spei_values)
}

# MAKE A WRAPPER FUNCTION TO PARALLELIZE THE DATASET
wrapper_func <- function(dataset, lon, lat, spei_values, M2, dates){
  results=list()
  for(i in 1:nrow(dataset)){
    results[[i]] = data.frame('latitude'=dataset[i,'latitude'],
                              'longitude'=dataset[i,'longitude'],
                              'ecosystem'=dataset[i,'ecosystem'],
                              'std_id'=dataset[i,'std_id'],
                              'SPEI'=find_nearest_non_na(dataset[i,c('longitude', 'latitude')],
                                                         lon, lat, spei_values,
                                                         M2),
                              'Date'=start_date,
                              'Year'=year(start_date),
                              'Month'=month(start_date))
  }
  return(bind_rows(results))
}


# Initialize progress bar
pb <- progress_bar$new(
  format = "  Processing [:bar] :percent | ETA: :eta",
  total = n, clear = FALSE, width = 60
)

# Apply the extraction function with progress bar
ncores = detectCores()
n <- nrow(spei.climate.dat)

## SPLIT THE DATASET INTO CHUNKS AND RUN PARALLELIZE THE OPRTATION
chunks =  split(spei.climate.dat, (seq(nrow(spei.climate.dat))-1) %/% ncores)
time1 <-Sys.time()
spei_results <- mclapply(chunks, wrapper_func, lon, lat, spei_values, M2, start_date)
time2 <- Sys.time()
print(time2 - time1)

spei_results2 = bind_rows(spei_results)


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
write.csv(spei_results2,"spei.climate.dat.fin.csv", row.names = F)
