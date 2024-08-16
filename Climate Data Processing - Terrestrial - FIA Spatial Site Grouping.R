########################
library(geosphere)
library(data.table)
library(sf)
library(here)


###############################
# MAKE CSV of input data and pull inhere

p.fia.grouptest<-read.csv(here("Data" , "Climate_data", "Plot_files",  "FIA.sites.with.multi.years.csv"))

###############################
#Run Spatial Grouping

# Convert to sf Object with EPSG:4326
#p.fia.grouptest_sf <- st_as_sf(p.fia.grouptest[1:35000,], coords = c("longitude", "latitude"), crs = 4326) #If need to run in chunks
p.fia.grouptest_sf <- st_as_sf(p.fia.grouptest, coords = c("longitude", "latitude"), crs = 4326)

# Extract Coordinates
coords <- st_coordinates(p.fia.grouptest_sf)
coords_df <- as.data.frame(coords)
names(coords_df) <- c("longitude", "latitude")

# Function to Perform Hierarchical Clustering
cluster_sites_hclust <- function(df, max_distance_km = 1) {
  # Calculate pairwise geodesic distances
  dist_matrix <- distm(df[, c("longitude", "latitude")], fun = distHaversine)
  
  # Convert Distances to KM
  dist_matrix <- dist_matrix / 1000
  
  # Perform Hierarchical Clustering
  hc <- hclust(as.dist(dist_matrix), method = "complete")
  
  # Cut Tree into Clusters based on Maximum Distance
  clusters <- cutree(hc, h = max_distance_km)
  
  # Add Cluster Assignments
  df$spatial_group <- as.factor(clusters)
  
  return(df)
}

# Apply Hierarchical Clustering - Set Max Distance Value Here under Max_Distance_KM
library(parallel)
ncores = detectCores()
chunks =  split(coords_df, (seq(nrow(coords_df))-1) %/% ncores) 
df_clustered_hclust <- mclapply(chunks, cluster_sites_hclust, 1)

# Function to Compute Midpoints
compute_midpoints_dt <- function(dt) {
  # Calculate the Midpoints Using Original Latitude and Longitude
  midpoints <- aggregate(cbind(latitude, longitude) ~ spatial_group, data = dt, FUN = mean)
  names(midpoints)[names(midpoints) == "latitude"] <- "group_lat"
  names(midpoints)[names(midpoints) == "longitude"] <- "group_long"
  
  # Merge Midpoints with the Clustered Data
  merged <- merge(dt, midpoints, by = "spatial_group")
  
  return(merged)
}

# Compute Midpoints
library(dplyr)# have to wait to load due to conflicts with data.table
df_with_midpoints_hclust <- bind_rows(mclapply(df_clustered_hclust, compute_midpoints_dt))

###############################
#Assign New IDs and Cross with Original STD_IDs for Later Use
#NOTE:: Need to adjust this code if running in batches 
#Cluster values will be repeated if running in batches - save files individually and provide unique STD_ID_CLUSTER values


#Creating New Data Frame
new_fia_clusters<-df_with_midpoints_hclust%>%
  mutate(std_id_cluster = paste0("cluster_FIA_", spatial_group))

#Creating New Data Frame for Cross
new_fia_clusters_for_cross<-df_with_midpoints_hclust%>%
  mutate(std_id_cluster = paste0("cluster_FIA_", spatial_group))%>%
  select(std_id_cluster, latitude, longitude)%>%
  distinct()

#Creating Cross-Data Frame for STD_ID and STD_ID_CLUSTER Matching
cluster_std_id_cross<-p.fia.grouptest%>%
  left_join(new_fia_clusters_for_cross, by = c("latitude", "longitude"))


#Creating Final Data Frame for Climate Processing
final_fia_cluster_data<-new_fia_clusters%>%
  mutate(std_id = std_id_cluster,
         site = "fia_clustered_site",
         ecosystem = "Forests",
         latitude = group_lat,
         longitude = group_long)%>%
  select(std_id, site, ecosystem, latitude, longitude)%>%
  distinct()

###############################
#Saving Files - *ENSURE VERSION HISTORY IS CORRECT - Will need for back-crossing later

write.csv(cluster_std_id_cross, file = "FIA.STD_ID.STD_ID_CLUSTER.Crosses.V1.16.08.2024.csv")
write.csv(final_fia_cluster_data, file = "FIA.STD_ID_CLUSTER.Values.V1.16.08.2024.csv")
