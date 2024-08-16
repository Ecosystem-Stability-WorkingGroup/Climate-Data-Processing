###### BEFORE RUNNING #########

# - Line 35: Set path to terrestrial site data
# - Line 41: Set path to marine site data
# - Line 182: Set working directory to location of WorldClim Data (Folders within can remain as ZIP)
# - Line 496: Set path to Global AI and ET0 Database Zip: Global-AI_monthly_v3.zip (Can remain as ZIP)
# - Line 553: Set path to Global AI and ET0 DB - Global-ET0_monthly_v3.zip  (Can remain as ZIP)
# - Lines 608 & 663: Set path to Global AI and ET0 DB - Global-AI_ET0_annual_v3.zip (Can remain as ZIP)
# - Line 809: Set path to Global SPEI Database file: spei01.nc
# - Lines 1528-1530: Set working directory to file save location, remove "#" from write.csv code lines

############################################################
# Load Packages
library(raster)
library(sf)
library(tidyverse)
library(terra)
library(fs)
library(httr)
library(rvest)
library(ncdf4)
library(progress)
library(parallel)
########################################################################################################################
########################################################################################################################
########################################################################################################################
# Step 1
# Creating Data Frame
########################################################################################################################
########################################################################################################################
########################################################################################################################

############################################################
#Option to remove FIA data
#all.climate.terr.sites<-all.climate.terr.sites%>%     
#  filter(!grepl("FIA", std_id, ignore.case = TRUE))  

all.climate.mar.sites<-read.csv("/home/lemoinelab2/Documents/server-share/ryf_climate_data/all.climate.mar.sites.csv")

########################################################################################################################
########################################################################################################################
########################################################################################################################
# Step 2
# Marine Data - NOAA CRW 
########################################################################################################################
########################################################################################################################
########################################################################################################################

############################################################
# Read Dataframe with Coordinates
marine.climate.map.dat<-all.climate.mar.sites

############################################################
# Set URL and Years of Data to Pull
base_url <- "https://www.ncei.noaa.gov/data/oceans/crw/5km/v3.1/nc/v1.0/monthly/"
years <- 1985:2023

############################################################
# Function to get the list of .nc files for a given year
get_nc_files <- function(year) {
  print(paste('Retrieving', year))
  url <- paste0(base_url, year, "/")
  webpage <- read_html(url)
  files <- webpage %>% html_nodes("a") %>% html_attr("href")
  nc_files <- grep("\\.nc$", files, value = TRUE)
  full_urls <- paste0(url, nc_files)
  return(full_urls)
}

############################################################
# Retrieve all .nc files for all years
print('File Retrieval')
nc_files_urls <- unlist(lapply(years, get_nc_files))

############################################################
# Function to extract data from a .nc file
extract_nc_data <- function(file_url, latitudes, longitudes) {
  # Open the .nc file
  temp <- tempfile()
  download.file(file_url, temp, mode = "wb")
  nc <- nc_open(temp)
  
  # Extract the variable name
  var_name <- names(nc$var)[1]
  
  # Get the variable data
  var_data <- ncvar_get(nc, var_name)
  
  # Get the dimensions
  lat <- ncvar_get(nc, "lat")
  lon <- ncvar_get(nc, "lon")

  # vectorize the lat function
  lat_idx1 <- sapply(latitudes, function(x){which.min(abs(lat-x))})
  lon_idx1 <- sapply(longitudes, function(x){which.min(abs(lon-x))})
  extracted_data <- var_data[cbind(lon_idx1, lat_idx1)]


  nearest_idx <- which(!is.na(var_data), arr.ind = TRUE)
  na_indices <- which(is.na(extracted_data))

  d_func = function(x, var_data, nearest_idx, lat_idx1, lon_idx1){
      lat_i <- lat_idx1[x]
      lon_i <- lon_idx1[x]
      distances <- (nearest_idx[, 2] - lat_i)^2 + 
                   (nearest_idx[, 1] - lon_i)^2
      nearest_point <- nearest_idx[which.min(distances), ]
      nn <- var_data[nearest_point[1], nearest_point[2]]
      return(nn)
  }

  res = unlist(mclapply(na_indices[1:10], d_func, var_data, nearest_idx, lat_idx1, lon_idx1))
  extracted_data[na_indices] = res
  # Close the .nc file
  nc_close(nc)
  unlink(temp)
  return(extracted_data)
  
}


############################################################
# Initialize progress bars
total_files <- length(nc_files_urls)
pb_total <- progress_bar$new(
  format = " Total Progress [:bar] :percent | ETA: :eta",
  total = total_files,
  clear = FALSE
)

############################################################
# Extract data for all files
all_data <- list()

print('Starting Extraction')

for (file_url in nc_files_urls) {
  print(file_url)
  pb_total$tick() # Update total progress bar
  
  # Initialize a progress bar for the current file
  pb_file <- progress_bar$new(
    format = paste0("File: ", basename(file_url), " [:bar] :percent | ETA: :eta"),
    total = 1,
    clear = FALSE
  )
  
  file_name <- basename(file_url)
  column_name <- sub("ct5km_", "", sub("_v3.1_", "_", sub("\\.nc$", "", file_name)))
  extracted_data <- extract_nc_data(file_url, marine.climate.map.dat$latitude, marine.climate.map.dat$longitude)
  all_data[[column_name]] <- extracted_data
}

############################################################
# Combine Data - Wide Format
marine.climate.map.dat.wide <- cbind(marine.climate.map.dat[, c("std_id", "latitude", "longitude", "ecosystem")], as.data.frame(all_data)) 

# Combine Data - Long Format
marine.climate.map.dat.long <- marine.climate.map.dat.wide %>%
  pivot_longer(
    cols = -c(std_id, ecosystem, latitude, longitude),
    names_to = c(".value", "YearMonth"),
    names_sep = "_"
  ) %>%
  mutate(
    Year = substr(YearMonth, 1, 4),
    Month = substr(YearMonth, 5, 6)
  ) %>%
  select(-YearMonth)

# Save the CSV
write.csv(marine.climate.map.dat.long, file = "Marine.Sites.Full.Climate.Data.csv", row.names = F)

