# Loading required libraries

library(pastecs)
library(ggpubr)
library(readr)
library(aws.s3)
library(ggplot2)
library(forecast)
library(dplyr)
library(tidyr)
library(lubridate)
library(Rtsne)
library(cluster)
library(fpc)
library(NbClust)
library(factoextra)
library(ggpubr)


# Setting up a default folder 

setwd("C:/Users/Objective 2")



# Importing data 

clustering_input <- read.csv(file = 'CMDC_NPI_LIST_x_API_joined.csv', stringsAsFactors = TRUE)

# Inspecting data 

head(clustering_input)
glimpse(clustering_input)
class(clustering_input$MONTH_END_DATE)

# Renaming columns, adjusting formats

clustering_input_updated <- clustering_input %>%
  rename (npi_id = NPI_Number, 
          time = MONTH_END_DATE, 
          sales = TRX, 
          default_specialty = SPECIALTY, 
          detailed_specialty = RESULT_desc,
          city = RESULT_City,
          state = RESULT_state,
          license = RESULT_license,
          gender = RESULT_gender) %>%
  mutate (time = dmy(time)) %>%
  group_by(npi_id,
           time, 
           sales, 
           default_specialty, 
           detailed_specialty,
           city,
           state,
           license,
           gender) %>%
  summarize(sales = sum(sales))

# Inspecting new data 

glimpse(clustering_input_updated)

# Evaluating the number of missing records per selected columns

sum(is.na(clustering_input_updated$default_specialty))
sum(is.na(clustering_input_updated$detailed_specialty))

# After examining data, the interest in clustering was based on hypothesis
# if there are any distinct clusters based on the TRX transanctional history by state and 
# specialty



# Finalizing input data: keeping variables of interest only & removing NA values

clustering_input_clean <- clustering_input_updated %>% 
                          drop_na(detailed_specialty) %>% 
                          group_by(detailed_specialty,
                                   state,
                                   sales) %>%
                          summarize(sales = sum(sales))
                          

sum(is.na(clustering_input_clean$detailed_specialty))

glimpse(clustering_input_clean)
View(clustering_input_clean)

# Reviewing summary statistics for the clustering_input_clean

sum_statistics <- stat.desc(clustering_input_clean$sales)
round(sum_statistics, 2)


ggboxplot(clustering_input_clean, y = "sales", width = 0.5)


# Creating samples of the clustering_input_clean as my machine
# has limitations on how much dedicated memory R can use

set.seed(123)
clustering_sample_clean <- clustering_input_clean[sample(nrow(clustering_input_clean), 2311), ]



# Based on the reviewed data, the possible suitable method for clustering was selected 
# partition around medoids (PAC). To be sure about method, we have to test it first


# Computing Gower distance to measures the dissimilarity of 
# numerical and non-numeric data
# 

gower_df <- daisy(x = clustering_sample_clean,
                  metric = "gower" ,
                  type = list(logratio = 2))

summary(gower_df)
class(gower_df)

# Plotting a Silhouette Width to select the optimal number of clusters

silhouette <- c()
silhouette = c(silhouette, NA)
for(i in 2:10){
  pam_clusters = pam(as.matrix(gower_df),
                     diss = TRUE,
                     k = i)
  silhouette = c(silhouette ,pam_clusters$silinfo$avg.width)
}
plot(1:10, silhouette,
     xlab = "Clusters",
     ylab = "Silhouette Width")
lines(1:10, silhouette)

# Result: the optimum number of clusters is 7

# Clustering data with defined number of clusters = 7

pam_fit <- pam(daisy(clustering_sample_clean,"gower"), diss = TRUE, 7)  # performs cluster analysis

pam_results <- as.data.frame(clustering_sample_clean) %>%
  mutate(cluster = pam_fit$clustering)  %>%
  group_by(cluster) 



class(pam_results)
class(pam_results$cluster)


# Writing a csv file that will contain the results of clustering 


write.csv(pam_results, file ='pam_clustering_results.csv', row.names=FALSE)



# Summary statistics for each cluster

pam_results_stat <- as.data.frame.matrix(clustering_sample_clean) %>%
  mutate(cluster = pam_fit$clustering)  %>%
  group_by(cluster) %>%
  do(cluster_summary = summary(.))


pam_results_stat$cluster_summary

pam_fit$medoids

clustering_sample_clean[pam_fit$medoids, ]

# Visualization

tsne_obj <- Rtsne(gower_df, is_distance = TRUE)

tsne_data <- tsne_obj$Y %>%
  data.frame() %>%
  setNames(c("X", "Y")) %>%
  mutate(cluster = factor(pam_fit$clustering),
         state = clustering_sample_clean$state)

ggplot(aes(x = X, y = Y), data = tsne_data) +
  geom_point(aes(color = cluster))



# Using provided credentials for accessing S3 Bucket

key <- "XYZ" # Shared key 
secret <- "XYZ" # Shared secret key
region <- "us-east-1"

Sys.setenv("AWS_ACCESS_KEY_ID" = key,
           "AWS_SECRET_ACCESS_KEY" = secret,
           "AWS_REGION" = region)

get_bucket(bucket = "etkachenko-use-case")


#
#
# writing files to S3
#

# Creating a separate folder for the first objective 

put_folder("Objective_2", bucket = "etkachenko-use-case")



# putting the file with pam clusterization results to the ____ folder with an upload progress bar
put_object(file = "pam_clustering_results.csv", object = "pam_clustering_results.csv", bucket = "etkachenko-use-case", show_progress = TRUE)

# putting the script file to the _____ folder with an upload progress bar
put_object(file = "July 7, 2021 - case study, objective 2 - final version.R", object = "Objective_2/July 7, 2021 - case study, objective 2 - final version.R", bucket = "etkachenko-use-case", show_progress = TRUE)

# putting the exported dashboard from Google Data Studio to the ____ folder with an upload progress bar
put_object(file = "Report_in_Google_Data_Studio_objective_2.pdf", object = "Objective_2/Report_in_Google_Data_Studio_objective_2.pdf", bucket = "etkachenko-use-case", show_progress = TRUE)

