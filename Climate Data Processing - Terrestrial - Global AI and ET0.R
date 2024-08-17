###### BEFORE RUNNING #########

# - Line 24: Set path to terrestrial site data
# - Line 55: Set path to Global AI and ET0 Database Zip: Global-AI_monthly_v3.zip (Can remain as ZIP)
# - Line 145: Set path to Global AI and ET0 DB - Global-ET0_monthly_v3.zip  (Can remain as ZIP)
# - Lines 232 & 319: Set path to Global AI and ET0 DB - Global-AI_ET0_annual_v3.zip (Can remain as ZIP)
# - Lines 486-488: Set working directory to file save location, remove "#" from write.csv code lines

############################################################
# Load Packages
library(data.table)
library(raster)
library(sf)
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
#all.climate.terr.sites<-all.climate.terr.sites%>%     
#  dplyr::filter(!grepl("FIA", std_id, ignore.case = TRUE))  

########################################################################################################################
########################################################################################################################
########################################################################################################################
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

# Convert sf object to SpatVector for terra compatibility
climate.arid.map.dat.terra <- vect(climate.arid.map.dat.sf)

# Convert dataframe to data.table for faster operations
climate.arid.map.dat.AI <- as.data.table(climate.arid.map.dat)

############################################################
# Extract Data and Add to New Dataframe 
for (tif_file in tif_files) {
  # Load the GeoTIFF file using terra
  tiff_raster <- rast(tif_file)
  
  # Extract values from the raster at the locations specified in your dataframe
  extracted <- extract(tiff_raster, climate.arid.map.dat.terra, method = "near")  # Use "near" for nearest neighbor extraction
  
  # Check if extraction was successful
  if (is.null(extracted) || nrow(extracted) == 0) {
    stop("Extraction failed for the file: ", tif_file)
  }
  
  # Extract just the second column which contains the raster values
  values <- extracted[, 2]  # Assuming the second column holds the extracted raster values
  
  # Convert the values to integers to avoid decimals
  values <- as.integer(values)
  
  # Handle NA values: Fill NA values using nearest non-NA values if necessary
  if (any(is.na(values))) {
    # Optionally, use na.approx or similar methods to fill NAs
    values <- zoo::na.approx(values, rule = 2, na.rm = FALSE)  # Linear interpolation for NA values
  }
  
  # Check length of extracted values
  num_values <- length(values)
  num_rows <- nrow(climate.arid.map.dat.AI)
  
  # Print debug information
  print(paste("Number of values extracted:", num_values))
  print(paste("Number of rows in dataframe:", num_rows))
  
  # Create a new column name based on the current file
  month <- sub(".*ai_v3_(\\d{2}).tif$", "\\1", basename(tif_file))
  col_name <- paste0("ai_v3_", month)
  
  # Handle length mismatch
  if (num_values > num_rows) {
    # Trim values to match dataframe rows
    values <- values[1:num_rows]
  } else if (num_values < num_rows) {
    # Expand values to match dataframe rows (recycle or pad with NAs)
    values <- rep_len(values, num_rows)
  }
  
  # Add the extracted values to the dataframe
  climate.arid.map.dat.AI[, (col_name) := values]
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

# Convert sf object to SpatVector for terra compatibility
climate.arid.map.dat.terra <- vect(climate.arid.map.dat.sf)

# Convert dataframe to data.table for faster operations
climate.arid.map.dat.ET0 <- as.data.table(climate.arid.map.dat)

############################################################
# Extract Data and Add to New Dataframe 
for (tif_file in tif_files) {
  # Load the GeoTIFF file using terra
  tiff_raster <- rast(tif_file)
  
  # Extract values from the raster at the locations specified in your dataframe
  extracted <- extract(tiff_raster, climate.arid.map.dat.terra, method = "near")  # Use "near" for nearest neighbor extraction
  
  # Check if extraction was successful
  if (is.null(extracted) || nrow(extracted) == 0) {
    stop("Extraction failed for the file: ", tif_file)
  }
  
  # Extract just the second column which contains the raster values
  values <- extracted[, 2]  # Assuming the second column holds the extracted raster values
  
  # Convert the values to integers to avoid decimals
  values <- as.integer(values)
  
  # Handle NA values: Fill NA values using nearest non-NA values if necessary
  if (any(is.na(values))) {
    # Optionally, use na.approx or similar methods to fill NAs
    values <- zoo::na.approx(values, rule = 2, na.rm = FALSE)  # Linear interpolation for NA values
  }
  
  # Check length of extracted values
  num_values <- length(values)
  num_rows <- nrow(climate.arid.map.dat.ET0)
  
  # Print debug information
  print(paste("Number of values extracted:", num_values))
  print(paste("Number of rows in dataframe:", num_rows))
  
  # Create a new column name based on the current file
  month <- sub(".*et0_v3_(\\d{2}).tif$", "\\1", basename(tif_file))
  col_name <- paste0("et0_v3_", month)
  
  # Handle length mismatch
  if (num_values > num_rows) {
    # Trim values to match dataframe rows
    values <- values[1:num_rows]
  } else if (num_values < num_rows) {
    # Expand values to match dataframe rows (recycle or pad with NAs)
    values <- rep_len(values, num_rows)
  }
  
  # Add the extracted values to the dataframe
  climate.arid.map.dat.ET0[, (col_name) := values]
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

# Convert sf object to SpatVector for terra compatibility
climate.arid.map.dat.terra <- vect(climate.arid.map.dat.sf)

# Convert dataframe to data.table for faster operations
climate.arid.map.dat.ET0_ann <- as.data.table(climate.arid.map.dat)

############################################################
# Extract Data and Add to New Dataframe 
for (tif_file in tif_files) {
  # Load the GeoTIFF file using terra
  tiff_raster <- rast(tif_file)
  
  # Extract values from the raster at the locations specified in your dataframe
  extracted <- extract(tiff_raster, climate.arid.map.dat.terra, method = "near")  # Use "near" for nearest neighbor extraction
  
  # Check if extraction was successful
  if (is.null(extracted) || nrow(extracted) == 0) {
    stop("Extraction failed for the file: ", tif_file)
  }
  
  # Extract just the second column which contains the raster values
  values <- extracted[, 2]  # Assuming the second column holds the extracted raster values
  
  # Convert the values to integers to avoid decimals
  values <- as.integer(values)
  
  # Handle NA values: Fill NA values using nearest non-NA values if necessary
  if (any(is.na(values))) {
    # Optionally, use na.approx or similar methods to fill NAs
    values <- zoo::na.approx(values, rule = 2, na.rm = FALSE)  # Linear interpolation for NA values
  }
  
  # Check length of extracted values
  num_values <- length(values)
  num_rows <- nrow(climate.arid.map.dat.ET0_ann)
  
  # Print debug information
  print(paste("Number of values extracted:", num_values))
  print(paste("Number of rows in dataframe:", num_rows))
  
  # Create a new column name based on the current file
  identifier <- sub("(?i).*et0_v3_(.*).tif$", "\\1", basename(tif_file))
  col_name <- paste0("et0_v3_", identifier)
  
  # Handle length mismatch
  if (num_values > num_rows) {
    # Trim values to match dataframe rows
    values <- values[1:num_rows]
  } else if (num_values < num_rows) {
    # Expand values to match dataframe rows (recycle or pad with NAs)
    values <- rep_len(values, num_rows)
  }
  
  # Add the extracted values to the dataframe
  climate.arid.map.dat.ET0_ann[, (col_name) := values]
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

# Convert sf object to SpatVector for terra compatibility
climate.arid.map.dat.terra <- vect(climate.arid.map.dat.sf)

# Convert dataframe to data.table for faster operations
climate.arid.map.dat.AI_ann <- as.data.table(climate.arid.map.dat)


############################################################
# Extract Data and Add to New Dataframe 
for (tif_file in tif_files) {
  # Load the GeoTIFF file using terra
  tiff_raster <- rast(tif_file)
  
  # Extract values from the raster at the locations specified in your dataframe
  extracted <- extract(tiff_raster, climate.arid.map.dat.terra, method = "near")  # Use "near" for nearest neighbor extraction
  
  # Check if extraction was successful
  if (is.null(extracted) || nrow(extracted) == 0) {
    stop("Extraction failed for the file: ", tif_file)
  }
  
  # Extract just the second column which contains the raster values
  values <- extracted[, 2]  # Assuming the second column holds the extracted raster values
  
  # Convert the values to integers to avoid decimals
  values <- as.integer(values)
  
  # Handle NA values: Fill NA values using nearest non-NA values if necessary
  if (any(is.na(values))) {
    # Optionally, use na.approx or similar methods to fill NAs
    values <- zoo::na.approx(values, rule = 2, na.rm = FALSE)  # Linear interpolation for NA values
  }
  
  # Check length of extracted values
  num_values <- length(values)
  num_rows <- nrow(climate.arid.map.dat.AI_ann)
  
  # Print debug information
  print(paste("Number of values extracted:", num_values))
  print(paste("Number of rows in dataframe:", num_rows))
  
  # Create a new column name based on the current file
  identifier <- sub("(?i).*ai_v3_(.*).tif$", "\\1", basename(tif_file))
  col_name <- paste0("ai_v3_", identifier)
  
  # Handle length mismatch
  if (num_values > num_rows) {
    # Trim values to match dataframe rows
    values <- values[1:num_rows]
  } else if (num_values < num_rows) {
    # Expand values to match dataframe rows (recycle or pad with NAs)
    values <- rep_len(values, num_rows)
  }
  
  # Add the extracted values to the dataframe
  climate.arid.map.dat.AI_ann[, (col_name) := values]
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
  tidyr::unnest(cols = everything())

climate.arid.map.dat.ET0 <- climate.arid.map.dat.ET0 %>%
  tidyr::unnest(cols = everything())

climate.arid.map.dat.AI_ann <- climate.arid.map.dat.AI_ann %>%
  tidyr::unnest(cols = everything())

climate.arid.map.dat.ET0_ann <- climate.arid.map.dat.ET0_ann %>%
  tidyr::unnest(cols = everything())

#Create Wide Dataframe
climate.arid.map.dat.all.wide<-dplyr::bind_cols(
  climate.arid.map.dat[,c("std_id", "latitude", "longitude", "ecosystem")],
  climate.arid.map.dat.AI[,c(6:17)],
  climate.arid.map.dat.ET0[,c(6:17)],
  climate.arid.map.dat.AI_ann[,c(6)],
  climate.arid.map.dat.ET0_ann[,c(6:7)]
)%>%
  dplyr::mutate(ai_01 = ai_v3_01 * 0.0001,    
                ai_02 = ai_v3_02 * 0.0001,
                ai_03 = ai_v3_03 * 0.0001,
                ai_04 = ai_v3_04 * 0.0001,
                ai_05 = ai_v3_05 * 0.0001,
                ai_06 = ai_v3_06 * 0.0001,
                ai_07 = ai_v3_07 * 0.0001,
                ai_08 = ai_v3_08 * 0.0001,
                ai_09 = ai_v3_09 * 0.0001,
                ai_10 = ai_v3_10 * 0.0001,
                ai_11 = ai_v3_11 * 0.0001,
                ai_12 = ai_v3_12 * 0.0001,
                pt0_01 = et0_v3_01,
                pt0_02 = et0_v3_02,     
                pt0_03 = et0_v3_03,    
                pt0_04 = et0_v3_04,     
                pt0_05 = et0_v3_et0_V3_05.tif,     
                pt0_06 = et0_v3_et0_V3_06.tif,     
                pt0_07 = et0_v3_et0_V3_07.tif,     
                pt0_08 = et0_v3_et0_V3_08.tif,     
                pt0_09 = et0_v3_et0_V3_09.tif,     
                pt0_10 = et0_v3_10,     
                pt0_11 = et0_v3_11,
                pt0_12 = et0_v3_12,
                ai_yr = ai_v3_yr * 0.0001,
                pt0_yr = et0_v3_yr,     
                pt0_yr.sd = et0_v3_yr_sd)%>%
  dplyr::select(std_id, latitude, longitude, ecosystem,
                ai_yr, pt0_yr, pt0_yr.sd,
                ai_01, ai_02, ai_03, ai_04, ai_05, ai_06,
                ai_07, ai_08, ai_09, ai_10, ai_11, ai_12,
                pt0_01, pt0_02, pt0_03, pt0_04, pt0_05, pt0_06,
                pt0_07, pt0_08, pt0_09, pt0_10, pt0_11, pt0_12,)


#Create Long Dataframe
climate.arid.map.dat.all.long<-climate.arid.map.dat.all.wide%>%
  tidyr::pivot_longer(
    cols = -c(std_id, ecosystem, latitude, longitude),
    names_to = "Metric_Month", # Temporary column to hold combined names
    values_to = "Value" # Column for the values
  ) %>%
  tidyr::separate(
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



