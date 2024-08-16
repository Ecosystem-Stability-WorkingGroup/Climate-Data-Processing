###### BEFORE RUNNING #########

# - Line 13: Set path to terrestrial site data
# - Line 145: Set path to location to download WorldClim files to locallly
# - Lines 373-382: Set working directory to file save location, remove "#" from write.csv code lines

############################################################
# Load Packages
librarian::shelf(raster, sf, tidyverse, terra, fs, httr, rvest, ncdf4, progress, here, tidylog)

############################################################
# Initial Dataframes - All Sites with Standardized IDs

all.climate.terr.sites<-read.csv(here("Data", "Climate_data", "all.climate.terr.sites.csv"))

#Remove FIA Data Until Site Crosscheck
all.climate.terr.sites<-all.climate.terr.sites%>%     
  filter(!grepl("FIA", std_id, ignore.case = TRUE))  

########################################################################################################################
########################################################################################################################
########################################################################################################################
# Terrestrial Data - WorldClim
########################################################################################################################
########################################################################################################################
########################################################################################################################

# Read Dataframe with Coordinates
climate.map.dat <- all.climate.terr.sites

time_start<-Sys.time() #Saving Time for Reference

############################################################
# Define the function to extract data from geoTIFF
extract_data_from_tiff <- function(tiff_file, climate_df) {
  # Read the raster file
  raster_layer <- raster(tiff_file)
  
  # Extract the values for each lat/lon point
  points <- st_as_sf(climate_df, coords = c("longitude", "latitude"), crs = crs(raster_layer))
  extracted_values <- raster::extract(raster_layer, points)
  
  # Return the extracted values
  return(extracted_values)
}

# Define the function to process each zip file and save it locally
process_zip_file <- function(zip_url, climate_df, local_dir) {
  # Create the local file path
  zip_name <- basename(zip_url)
  local_zip <- file.path(local_dir, zip_name)
  
  # Download the zip file to the local directory if not already downloaded
  if (!file.exists(local_zip)) {
    download.file(zip_url, local_zip, mode = "wb")
  } else {
    message(paste("File already downloaded:", local_zip))
  }
  
  # Unzip the contents
  unzip(local_zip, exdir = local_dir)
  
  # List all geoTIFF files in the local directory
  tiff_files <- list.files(local_dir, pattern = "\\.tif$", full.names = TRUE)
  
  # Create an empty list to store the results for this ZIP file
  results_list <- list()
  
  # Loop through each tiff file
  for (tiff_file in tiff_files) {
    # Extract the month/year from the file name
    file_name <- basename(tiff_file)
    column_name <- sub("wc2.1_2.5m_", "", sub("\\.tif$", "", file_name))
    
    # Extract data from the tiff file
    extracted_data <- extract_data_from_tiff(tiff_file, climate_df)
    
    # Add the data to the results list
    results_list[[column_name]] <- extracted_data
  }
  
  # Combine the results into a dataframe specific to this ZIP file
  results_df <- as.data.frame(results_list)
  
  # Combine with the original climate data, ensuring it's only for this ZIP
  combined_df <- cbind(climate_df, results_df)
  
  # Clean up the local directory to prevent carry-over of files between iterations
  unlink(tiff_files)
  
  return(combined_df)
}
############################################################
# Define the list of base URLs and file names (unchanged from your original code)
base_urls <- list(
  "https://geodata.ucdavis.edu/climate/worldclim/2_1/base/",
  "https://geodata.ucdavis.edu/climate/worldclim/2_1/hist/cts4.06/2.5m/"
)

file_names_base  <- c(
  "wc2.1_30s_tavg.zip",
  "wc2.1_30s_tmax.zip",
  "wc2.1_30s_tmin.zip",
  "wc2.1_30s_prec.zip",
  "wc2.1_30s_srad.zip",
  "wc2.1_30s_wind.zip",
  "wc2.1_30s_vapr.zip",
  "wc2.1_30s_bio.zip",
  "wc2.1_30s_elev.zip"
)

file_names_hist <- c(
  "wc2.1_cruts4.06_2.5m_prec_1960-1969.zip",
  "wc2.1_cruts4.06_2.5m_prec_1970-1979.zip",
  "wc2.1_cruts4.06_2.5m_prec_1980-1989.zip",
  "wc2.1_cruts4.06_2.5m_prec_1990-1999.zip",
  "wc2.1_cruts4.06_2.5m_prec_2000-2009.zip",
  "wc2.1_cruts4.06_2.5m_prec_2010-2019.zip",
  "wc2.1_cruts4.06_2.5m_prec_2020-2021.zip",
  "wc2.1_cruts4.06_2.5m_tmin_1960-1969.zip",
  "wc2.1_cruts4.06_2.5m_tmin_1970-1979.zip",
  "wc2.1_cruts4.06_2.5m_tmin_1980-1989.zip",
  "wc2.1_cruts4.06_2.5m_tmin_1990-1999.zip",
  "wc2.1_cruts4.06_2.5m_tmin_2000-2009.zip",
  "wc2.1_cruts4.06_2.5m_tmin_2010-2019.zip",
  "wc2.1_cruts4.06_2.5m_tmin_2020-2021.zip",
  "wc2.1_cruts4.06_2.5m_tmax_1960-1969.zip",
  "wc2.1_cruts4.06_2.5m_tmax_1970-1979.zip",
  "wc2.1_cruts4.06_2.5m_tmax_1980-1989.zip",
  "wc2.1_cruts4.06_2.5m_tmax_1990-1999.zip",
  "wc2.1_cruts4.06_2.5m_tmax_2000-2009.zip",
  "wc2.1_cruts4.06_2.5m_tmax_2010-2019.zip",
  "wc2.1_cruts4.06_2.5m_tmax_2020-2021.zip"
)

# Combine the base URLs with the file names to create the full URLs
zip_urls <- c(
  paste0(base_urls[[1]], file_names_base),
  paste0(base_urls[[2]], file_names_hist)
)

############################################################
# Define the local directory where files will be saved
local_dir <- "C:/Users/rfidler/Desktop/temp_test"

# Create the directory if it doesn't exist
if (!dir.exists(local_dir)) {
  dir.create(local_dir, recursive = TRUE)
}

############################################################
# Create an empty list to store the results
final_results <- list()

############################################################
# Loop through each zip file URL and process it
options(timeout = 20000)

for (zip_url in zip_urls) {
  combined_df <- process_zip_file(zip_url, climate.map.dat, local_dir)
  zip_name <- basename(zip_url)
  final_results[[zip_name]] <- combined_df
}

############################################################
# Creating Dataframes of Covariates
############################################################

########################################################################
# - TOTAL Average Monthly Temperatures - Mean/Min/Max: 1970 -2000
########################################################################

########################################################################
#Create Individual Total Average Dataframes: Wide Format

#Total Monthly Average, Min, Max Temperature
climate.map.dat.tav.wide<-bind_cols(
  climate.map.dat[,c("site", "latitude", "longitude", "ecosystem")],
  final_results$wc2.1_30s_tavg.zip[,c(6:17)],
  final_results$wc2.1_30s_tmin.zip[,c(6:17)],
  final_results$wc2.1_30s_tmax.zip[,c(6:17)]
)

############################################################
#Create Individual Total Average Dataframes: Long Format

#Total Monthly Average, Min, Max Temperature
climate.map.dat.tav.long<- climate.map.dat.tav.wide %>%
  pivot_longer(
    cols = starts_with("wc2.1_30s_"),
    names_to = c(".value", "Month"),
    names_pattern = "wc2.1_30s_(.*)_(\\d{2})"
  )%>%
  mutate(Month = as.numeric(Month))

########################################################################
# - TOTAL Average Monthly Precipitation, Solar Radiation, Wind Speed, Water Vapor Pressure: 1970 -2000
########################################################################

########################################################################
#Create Individual Total Average Dataframes: Wide Format

#Total Monthly Average Precipitation, Solar Radiation, Wind Speed, Water Vapor Pressure
climate.map.dat.prec.sol.wind.wat.wide<-bind_cols(
  climate.map.dat[,c("site", "latitude", "longitude", "ecosystem")],
  final_results$wc2.1_30s_prec.zip[,c(6:17)],
  final_results$wc2.1_30s_srad.zip[,c(6:17)],
  final_results$wc2.1_30s_wind.zip[,c(6:17)],
  final_results$wc2.1_30s_vapr.zip[,c(6:17)]
)
############################################################
#Create Individual Total Average Dataframes: Long Format

#Total Monthly Average Precipitation, Solar Radiation, Wind Speed, Water Vapor Pressure
climate.map.dat.prec.sol.wind.wat.long<- climate.map.dat.prec.sol.wind.wat.wide %>%
  pivot_longer(
    cols = starts_with("wc2.1_30s_"),
    names_to = c(".value", "Month"),
    names_pattern = "wc2.1_30s_(.*)_(\\d{2})"
  ) %>%
  mutate(
    Month = as.numeric(Month)) 

########################################################################
# - Monthly Precipitation, Min/Max Temperatures: 1970 -2000
########################################################################

############################################################
#Create Individual Monthly Dataframes: Wide Format

#Precipitation
climate.map.dat.prec.wide<-dplyr::bind_cols(
  climate.map.dat[,c("site", "latitude", "longitude", "ecosystem")],
  final_results$`wc2.1_cruts4.06_2.5m_prec_1960-1969.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_prec_1970-1979.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_prec_1980-1989.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_prec_1990-1999.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_prec_2000-2009.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_prec_2010-2019.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_prec_2020-2021.zip`[,c(6:29)]
)

#Min Temp
climate.map.dat.tmin.wide<-dplyr::bind_cols(
  climate.map.dat[,c("site", "latitude", "longitude", "ecosystem")],
  final_results$`wc2.1_cruts4.06_2.5m_tmin_1960-1969.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_tmin_1970-1979.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_tmin_1980-1989.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_tmin_1990-1999.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_tmin_2000-2009.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_tmin_2010-2019.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_tmin_2020-2021.zip`[,c(6:29)]
)

#Max Temp
climate.map.dat.tmax.wide<-dplyr::bind_cols(
  climate.map.dat[,c("site", "latitude", "longitude", "ecosystem")],
  final_results$`wc2.1_cruts4.06_2.5m_tmax_1960-1969.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_tmax_1970-1979.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_tmax_1980-1989.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_tmax_1990-1999.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_tmax_2000-2009.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_tmax_2010-2019.zip`[,c(6:125)],
  final_results$`wc2.1_cruts4.06_2.5m_tmax_2020-2021.zip`[,c(6:29)]
)

############################################################4
#Create Individual Monthly Dataframes: Long Format

#Precipitation
climate.map.dat.prec.long<- climate.map.dat.prec.wide %>%
  pivot_longer(
    cols = 5:748,
    names_to = c("Year", "Month"),
    names_pattern = "prec_(\\d{4})\\.(\\d{2})",
    values_to = "precip"
  ) %>%
  mutate(
    Year = as.integer(Year),
    Month = as.integer(Month)
  )

#Min Temp
climate.map.dat.tmin.long<- climate.map.dat.tmin.wide %>%
  pivot_longer(
    cols = 5:748,
    names_to = c("Year", "Month"),
    names_pattern = "tmin_(\\d{4})\\.(\\d{2})",
    values_to = "min_temp"
  ) %>%
  mutate(
    Year = as.integer(Year),
    Month = as.integer(Month)
  )

#Max Temp
climate.map.dat.tmax.long<- climate.map.dat.tmax.wide %>%
  pivot_longer(
    cols = 5:748,
    names_to = c("Year", "Month"),
    names_pattern = "tmax_(\\d{4})\\.(\\d{2})",
    values_to = "max_temp"
  ) %>%
  mutate(
    Year = as.integer(Year),
    Month = as.integer(Month)
  )

########################################################################
# - Bioclimatic Variables, Elevation
########################################################################

########################################################################
#Create Individual Total Average Dataframes: Wide Format

#Bioclimatic Variables, Elevation
climate.map.dat.bioclim.wide<-dplyr::bind_cols(
  climate.map.dat[,c("site", "latitude", "longitude", "ecosystem")],
  final_results$wc2.1_30s_bio.zip[,c(6:24)],
  final_results$wc2.1_30s_elev.zip[,c(6)]
)%>%
  mutate(annual_mean_temp_70_00 = wc2.1_30s_bio_1,
         mean_diurnal_range_70_00 = wc2.1_30s_bio_2,
         isothermality_70_00 = wc2.1_30s_bio_3,
         temp_seasonality_70_00 = wc2.1_30s_bio_4,
         tmax_warmest_m_70_00 = wc2.1_30s_bio_5,
         tmin_coldest_m_70_00 = wc2.1_30s_bio_6,
         temp_ann_range_70_00 = wc2.1_30s_bio_7,
         tavg_wettest_q_70_00 = wc2.1_30s_bio_8,
         tavg_driest_q_70_00 = wc2.1_30s_bio_9,
         tavg_warmest_q_70_00 = wc2.1_30s_bio_10,
         tavg_coldest_q_70_00 = wc2.1_30s_bio_11,
         annual_precip_70_00 = wc2.1_30s_bio_12,
         prec_wettest_m_70_00 = wc2.1_30s_bio_13,
         prec_driest_m_70_00 = wc2.1_30s_bio_14,
         prec_seasonality_70_00 = wc2.1_30s_bio_15,
         prec_wettest_q_70_00 = wc2.1_30s_bio_16,
         prec_driest_q_70_00 = wc2.1_30s_bio_17,
         prec_warmest_q_70_00 = wc2.1_30s_bio_18,
         prec_coldest_q_70_00 = wc2.1_30s_bio_19,
         elevation = ...24)%>%
  dplyr::select(site, latitude, longitude, ecosystem,
                annual_mean_temp_70_00, mean_diurnal_range_70_00, isothermality_70_00,
                temp_seasonality_70_00, tmax_warmest_m_70_00, tmin_coldest_m_70_00, temp_ann_range_70_00,
                tavg_wettest_q_70_00, tavg_driest_q_70_00, tavg_warmest_q_70_00, tavg_coldest_q_70_00,
                annual_precip_70_00, prec_wettest_m_70_00, prec_driest_m_70_00, prec_seasonality_70_00,
                prec_wettest_q_70_00, prec_driest_q_70_00, prec_warmest_q_70_00, prec_coldest_q_70_00,
                elevation)

########################################################################
#Create Individual Total Average Dataframes: Long Format

#Bioclimatic Variables, Elevation
climate.map.dat.bioclim.long<-climate.map.dat.bioclim.wide%>%
  pivot_longer(
    cols = -c(site, ecosystem, latitude, longitude),
    names_to = "variable",
    values_to = "value"
  )


########################################################################
#Time Logs
time_done_worldclim<-Sys.time()

time_done_worldclim - time_start 


########################################################################
#Save Files:

#setwd(path/to/save/location)
#write.csv(climate.map.dat.bioclim.wide, file="climate.map.dat.bioclim.wide.csv", row.names = F)
#write.csv(climate.map.dat.bioclim.long, file="climate.map.dat.bioclim.long.csv", row.names = F)

#write.csv(climate.map.dat.tmin.long, file="climate.map.dat.tmin.long.csv", row.names = F)
#write.csv(climate.map.dat.tmax.long, file="climate.map.dat.tmax.long.csv", row.names = F)
#write.csv(climate.map.dat.tav.long, file="climate.map.dat.tav.long.csv", row.names = F)

#write.csv(climate.map.dat.prec.long, file="climate.map.dat.prec.long.csv", row.names = F)
#write.csv(climate.map.dat.prec.sol.wind.wat.long, file="climate.map.dat.prec.sol.wind.wat.long.csv", row.names = F)


