###### BEFORE RUNNING #########

# - Line 24: Set path to terrestrial site data
# - Line 56: Set path to Global AI and ET0 Database Zip: Global-AI_monthly_v3.zip (Can remain as ZIP)
# - Line 113: Set path to Global AI and ET0 DB - Global-ET0_monthly_v3.zip  (Can remain as ZIP)
# - Lines 168 & 223: Set path to Global AI and ET0 DB - Global-AI_ET0_annual_v3.zip (Can remain as ZIP)
# - Lines 357-359: Set working directory to file save location, remove "#" from write.csv code lines

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

############################################################
# Initial Dataframes - All Sites with Standardized IDs

all.climate.terr.sites<-read.csv("C:/Users/rfidler/Desktop/Powell Ecosystem Stability/Data/All Sites - Data Harmonization/STD_ID Full Lists/all.climate.terr.sites.csv")

#Remove FIA Data Until Site Crosscheck
all.climate.terr.sites<-all.climate.terr.sites%>%     
  filter(!grepl("FIA", std_id, ignore.case = TRUE))  

########################################################################################################################
########################################################################################################################
########################################################################################################################
# Step 4
# Terrestrial Data - Global AI and ET0
########################################################################################################################
########################################################################################################################
########################################################################################################################

time_start<-Sys.time() #Saving Time for Reference

############################################################
# Read Dataframe with Coordinates
climate.arid.map.dat<-all.climate.terr.sites

############################################################
############################################################
# Part 1
# Aridity Index Monthly Data
############################################################
############################################################

############################################################
# Get Files and Set Dataframe 

# Set Filepath and Create Temporary Directory
zip_file <- "C:/Users/rfidler/Desktop/Powell Ecosystem Stability/Data/Climate Data/Global AI and ET0 Climate Database/7504448/Global-AI_monthly_v3.zip"
temp_dir <- tempdir()

# Unzip Files to the Temporary Directory
unzip(zip_file, exdir = temp_dir)

# List All .tif Files in the Folder
tif_files <- list.files(temp_dir, pattern = "ai_v3_\\d{2}.tif$", recursive = TRUE, full.names = TRUE)

# Check if tif_files is Populated Correctly
print(tif_files)

# Convert Dataframe to sf Object
climate.arid.map.dat.sf <- st_as_sf(climate.arid.map.dat, coords = c("longitude", "latitude"), crs = 4326)

#Convert Coordinates to Matrix
coords <- st_coordinates(climate.arid.map.dat.sf)

#Create Dataframe to Store Results
climate.arid.map.dat.AI <- climate.arid.map.dat

############################################################
# Extract Data and Add to New Dataframe 

for (tif_file in tif_files) {
  # Load the GeoTIFF file using terra
  tiff_raster <- rast(tif_file)
  
  # Extract values from the GeoTIFF at the locations specified in your dataframe
  values <- extract(tiff_raster, coords)
  
  # Get the month from the filename
  month <- sub(".*ai_v3_(\\d{2}).tif$", "\\1", basename(tif_file))
  
  # Create a new column name
  col_name <- paste0("ai_v3_", month)
  
  # Add the extracted values to the dataframe
  climate.arid.map.dat.AI[[col_name]] <- values
  
}

############################################################
# Delete the temporary directory and its contents
fs::dir_delete(temp_dir)

############################################################
############################################################
# Part 2
# Potential Evapotransporation (ET0) Monthly Data
############################################################
############################################################

############################################################
# Get Files and Set Dataframe 

# Set Filepath and Create Temporary Directory
zip_file <- "C:/Users/rfidler/Desktop/Powell Ecosystem Stability/Data/Climate Data/Global AI and ET0 Climate Database/7504448/Global-ET0_monthly_v3.zip"
temp_dir <- tempdir()

# Unzip Files to the Temporary Directory
unzip(zip_file, exdir = temp_dir)

# List All .tif Files in the Folder
tif_files <- list.files(temp_dir, pattern = "(?i)et0_v3_\\d{2}.tif$", recursive = TRUE, full.names = TRUE)

# Check if tif_files is Populated Correctly
print(tif_files)

# Convert Dataframe to sf Object
climate.arid.map.dat.sf <- st_as_sf(climate.arid.map.dat, coords = c("longitude", "latitude"), crs = 4326)

#Convert Coordinates to Matrix
coords <- st_coordinates(climate.arid.map.dat.sf)

#Create Dataframe to Store Results
climate.arid.map.dat.ET0 <- climate.arid.map.dat

############################################################
# Extract Data and Add to New Dataframe 
for (tif_file in tif_files) {
  # Load the GeoTIFF file using terra
  tiff_raster <- rast(tif_file)
  
  # Extract values from the GeoTIFF at the locations specified in your dataframe
  values <- extract(tiff_raster, coords)
  
  # Get the month from the filename
  month <- sub(".*et0_v3_(\\d{2}).tif$", "\\1", basename(tif_file))
  
  # Create a new column name
  col_name <- paste0("et0_v3_", month)
  
  # Add the extracted values to the dataframe
  climate.arid.map.dat.ET0[[col_name]] <- values
}

############################################################
# Delete the temporary directory and its contents
fs::dir_delete(temp_dir)

############################################################
############################################################
# Part 3
# Potential Evapotransporation (ET0) Annual Data
############################################################
############################################################

############################################################
# Get Files and Set Dataframe 

# Set Filepath and Create Temporary Directory
zip_file <- "C:/Users/rfidler/Desktop/Powell Ecosystem Stability/Data/Climate Data/Global AI and ET0 Climate Database/7504448/Global-AI_ET0_annual_v3.zip"
temp_dir <- tempdir()

# Unzip Files to the Temporary Directory
unzip(zip_file, exdir = temp_dir)

# List All .tif Files in the Folder
tif_files <- list.files(temp_dir, pattern = "(?i)et0_v3_.*\\.tif$", recursive = TRUE, full.names = TRUE)

# Check if tif_files is Populated Correctly
print(tif_files)

# Convert Dataframe to sf Object
climate.arid.map.dat.sf <- st_as_sf(climate.arid.map.dat, coords = c("longitude", "latitude"), crs = 4326)

#Convert Coordinates to Matrix
coords <- st_coordinates(climate.arid.map.dat.sf)

#Create Dataframe to Store Results
climate.arid.map.dat.ET0_ann <- climate.arid.map.dat

############################################################
# Extract Data and Add to New Dataframe 
for (tif_file in tif_files) {
  # Load the GeoTIFF file using terra
  tiff_raster <- rast(tif_file)
  
  # Extract values from the GeoTIFF at the locations specified in your dataframe
  values <- extract(tiff_raster, coords)
  
  # Get the identifier after the prefix from the filename
  identifier <- sub("(?i).*et0_v3_(.*).tif$", "\\1", basename(tif_file))
  
  # Create a new column name
  col_name <- paste0("et0_v3_", identifier)
  
  # Add the extracted values to the dataframe
  climate.arid.map.dat.ET0_ann[[col_name]] <- values
}

############################################################
# Delete the temporary directory and its contents
fs::dir_delete(temp_dir)

############################################################
############################################################
# Part 4
# Aridity Index Annual Data
############################################################
############################################################

############################################################
# Get Files and Set Dataframe 

# Set Filepath and Create Temporary Directory
zip_file <- "C:/Users/rfidler/Desktop/Powell Ecosystem Stability/Data/Climate Data/Global AI and ET0 Climate Database/7504448/Global-AI_ET0_annual_v3.zip"
temp_dir <- tempdir()

# Unzip Files to the Temporary Directory
unzip(zip_file, exdir = temp_dir)

# List All .tif Files in the Folder
tif_files <- list.files(temp_dir, pattern = "(?i)ai_v3_.*\\.tif$", recursive = TRUE, full.names = TRUE)

# Check if tif_files is Populated Correctly
print(tif_files)

# Convert Dataframe to sf Object
climate.arid.map.dat.sf <- st_as_sf(climate.arid.map.dat, coords = c("longitude", "latitude"), crs = 4326)

#Convert Coordinates to Matrix
coords <- st_coordinates(climate.arid.map.dat.sf)

#Create Dataframe to Store Results
climate.arid.map.dat.AI_ann <- climate.arid.map.dat

############################################################
# Extract Data and Add to New Dataframe 
for (tif_file in tif_files) {
  # Load the GeoTIFF file using terra
  tiff_raster <- rast(tif_file)
  
  # Extract values from the GeoTIFF at the locations specified in your dataframe
  values <- extract(tiff_raster, coords)
  
  # Get the identifier after the prefix from the filename
  identifier <- sub("(?i).*ai_v3_(.*).tif$", "\\1", basename(tif_file))
  
  # Create a new column name
  col_name <- paste0("ai_v3_", identifier)
  
  # Add the extracted values to the dataframe
  climate.arid.map.dat.AI_ann[[col_name]] <- values
}

############################################################
# Delete the temporary directory and its contents
fs::dir_delete(temp_dir)

############################################################
############################################################
# Part 5
# Create Combined Dataframes
############################################################
############################################################
colnames(climate.arid.map.dat.AI)
colnames(climate.arid.map.dat.ET0)
colnames(climate.arid.map.dat.AI_ann)
colnames(climate.arid.map.dat.ET0_ann)

# Unnest columns that are dataframes
climate.arid.map.dat.AI <- climate.arid.map.dat.AI %>%
  unnest(cols = everything())

climate.arid.map.dat.ET0 <- climate.arid.map.dat.ET0 %>%
  unnest(cols = everything())

climate.arid.map.dat.AI_ann <- climate.arid.map.dat.AI_ann %>%
  unnest(cols = everything())

climate.arid.map.dat.ET0_ann <- climate.arid.map.dat.ET0_ann %>%
  unnest(cols = everything())

#Create Wide Dataframe
climate.arid.map.dat.all.wide<-bind_cols(
  climate.arid.map.dat[,c("std_id", "latitude", "longitude", "ecosystem")],
  climate.arid.map.dat.AI[,c(6:17)],
  climate.arid.map.dat.ET0[,c(6:17)],
  climate.arid.map.dat.AI_ann[,c(6)],
  climate.arid.map.dat.ET0_ann[,c(6:7)]
)%>%
  mutate(ai_01 = awi_pm_sr_01 * 0.0001,    
         ai_02 = awi_pm_sr_02 * 0.0001,
         ai_03 = awi_pm_sr_03 * 0.0001,
         ai_04 = awi_pm_sr_04 * 0.0001,
         ai_05 = awi_pm_sr_05 * 0.0001,
         ai_06 = awi_pm_sr_06 * 0.0001,
         ai_07 = awi_pm_sr_07 * 0.0001,
         ai_08 = awi_pm_sr_08 * 0.0001,
         ai_09 = awi_pm_sr_09 * 0.0001,
         ai_10 = awi_pm_sr_10 * 0.0001,
         ai_11 = awi_pm_sr_11 * 0.0001,
         ai_12 = awi_pm_sr_12 * 0.0001,
         pt0_01 = pet_pm_sr_01,
         pt0_02 = pet_pm_sr_02,     
         pt0_03 = pet_pm_sr_03,    
         pt0_04 = pet_pm_sr_04,     
         pt0_05 = pet_pm_sr_05,     
         pt0_06 = pet_pm_sr_06,     
         pt0_07 = pet_pm_sr_07,     
         pt0_08 = pet_pm_sr_08,     
         pt0_09 = pet_pm_sr_09,     
         pt0_10 = pet_pm_sr_10,     
         pt0_11 = pet_pm_sr_11,
         pt0_12 = pet_pm_sr_12,
         ai_yr = awi_pm_sr_yr * 0.0001,
         pt0_yr = pet_pm_sr_yr,     
         pt0_yr.sd = pet_pm_sr_sd)%>%
  dplyr::select(std_id, latitude, longitude, ecosystem,
                ai_yr, pt0_yr, pt0_yr.sd,
                ai_01, ai_02, ai_03, ai_04, ai_05, ai_06,
                ai_07, ai_08, ai_09, ai_10, ai_11, ai_12,
                pt0_01, pt0_02, pt0_03, pt0_04, pt0_05, pt0_06,
                pt0_07, pt0_08, pt0_09, pt0_10, pt0_11, pt0_12,)


#Create Long Dataframe
climate.arid.map.dat.all.long<-climate.arid.map.dat.all.wide%>%
  pivot_longer(
    cols = -c(std_id, ecosystem, latitude, longitude),
    names_to = "Metric_Month", # Temporary column to hold combined names
    values_to = "Value" # Column for the values
  ) %>%
  separate(
    col = Metric_Month,
    into = c("Metric", "Month"),
    sep = "_", # Splits on the "_"
    remove = TRUE # Remove the original combined column
  ) 

########################################################################
#Time Logs
time_done_ai_eT0<-Sys.time()

time_done_ai_eT0 - time_start

########################################################################
#Save Files:

#setwd(path/to/save/location)
#write.csv(climate.arid.map.dat.all.wide, file="climate.arid.map.dat.all.wide.csv", row.names = F)
#write.csv(climate.arid.map.dat.all.long, file="climate.arid.map.dat.all.long.csv", row.names = F)



