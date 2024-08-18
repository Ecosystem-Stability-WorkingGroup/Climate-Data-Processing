## ---------------------------
##
## Script name: Connect to Google Drive
##
## Author: Dr. Joan Dudney
##
## Date Created: 2024-08-17
##
## Copyright (c) Joan Dudney, 2024
## Email: dudney@ucsb.edu
##
## ---------------------------
##
## Notes: This code creates a connection to 
##    Google Drive for desktop, allowing you to read in data stored on google drive without
##    having to store it locally on you computer
## ---------------------------


librarian::shelf(R.utils, here, readr, progress)

# Define the path to your local code directory
code_dir <- here("Data")

# Change this path so that it defines the path to your local google drive data folder
data_dir <- "/Users/treelife/Library/CloudStorage/GoogleDrive-dudney@ucsb.edu/My Drive/Data"

## create a folder called "Remote" that directly links to the shared google drive
createLink(paste0(code_dir, '/Remote'), data_dir, overwrite = FALSE)


## testing to see how slow the network is
test <- read_csv(here("Data", "Remote", "Climate Data", "spei.climate.dat.fin.csv"))



