# Installing all required libraries

install.packages("aws.s3")
install.packages("devtools")
install_github("armstrtw/AWS.tools")


# Setting Up a default folder path

setwd("C:/Users/Working Directory")


# Loading all required libraries

library("devtools") 
library(dplyr)
library(jsonlite)
library(tibble)
library(aws.s3)
library(httr)
library(data.table)
library(tidyr)



# Using provided credentials for accessing S3 Bucket

key <- "XYZ" # Shared key 
secret <- "XYZ" # Shared secret key
region <- "us-east-1"

Sys.setenv("AWS_ACCESS_KEY_ID" = key,
           "AWS_SECRET_ACCESS_KEY" = secret,
           "AWS_REGION" = region)

get_bucket(bucket = "etkachenko-use-case") 



#Saving CMDC NPI List.csv file (both locally in RStudio and physically to the machine)

save_object("From/CMDC NPI List.csv", file = "CMDC NPI List.csv", bucket = "etkachenko-use-case")
cmdc_npi_list <- read.csv(file = "CMDC NPI List.csv", header = TRUE)
view(cmdc_npi_list)

# Examining the structure and data of the CMDC NPI List.csv file

head(cmdc_npi_list, 10)
dim(cmdc_npi_list)
glimpse(cmdc_npi_list)
summary(cmdc_npi_list)

# Adjusting the naming of the NPI Number (ID column)

names(cmdc_npi_list)[1] <- "NPI_Number"
head(cmdc_npi_list, 10)

# Counting the number of distinct NPI Numbers we will be working with: 10,000

n_distinct(cmdc_npi_list$NPI_Number)


# Saving NPI_Number column as a vector of unique values so it will be easier to work with when submitting calls to API
input <- as.data.frame(unique(cmdc_npi_list$NPI_Number))
names(input)[1] <- "npi_num"

# Adding Row Num column which will be used for controlling amount of data for extraction as there is a time limit on API's side of 10000 millisecond

input$Row_Num <- seq.int(nrow(input))

# Inspecting Input 

view(input)
typeof(input)
n_distinct(input)

# Selecting the range of records we would like to retrieve from API (any number of records under 3000 as over 3000 records timeout error will be returned)

input <- input %>% 
  select(npi_num, Row_Num) %>% 
  filter(between(Row_Num, 8500, 10000)) # 8500 and 10000 should be adjusted depending on how much data we want to extract via API
                                        # data was extracted in 4 batches to overcome API timeout specs: 
                                          # 1-2999; 3000-5999; 6000-8499; 8500-10000
#view(input)



# The scrip below should be used for extracting data from API and saving it locally in the csv format
#
#
#
#
# Looping through each row (from 1 to the total number of rows) in the data frame with users
for(i in 1:nrow(input)){
  
# grabbing number from each row/ID & write grabbed values into variables to be passed onto API call
  
number <- input$npi_num[i]

# making API call for each row by dynamically populating API's querry with firstname, lastname, state for each row/person
  
api_call <-  GET("https://npiregistry.cms.hhs.gov/api/", query = list(number=number,enumeration_type = "NPI-1", first_name="", last_name="",state="", version=2.1), timeout(60)) #timeout parameter doesn't work on API
  
# converting API call resulting object into R's list object
  
api_content <-  content(api_call)
 
# grabbing the value of the total return result per search and write into variable
  
api_content_count <-  api_content$result_count


# Optional
#view(api_content_count)

# if API call returns no values as some NPI IDs could have expired, add columns to the output with respective NA values 

if(is.null(api_content_count)) {
  input$RESULT_NPI[i] = 'NA'
  input$RESULT_Address1[i] <- 'NA'
  input$RESULT_City[i] <- 'NA'
  input$RESULT_IsPresent[i] <- 'False'
  input$RESULT_MatchCount[i] <- 'NA'
  input$RESULT_enumeration_type[i] <- 'NA'
  input$RESULT_last_updated_epoch[i] <- 'NA'
  input$RESULT_created_epoch[i] <- 'NA'
  input$RESULT_first_name[i] <- 'NA'
  input$RESULT_last_name[i] <- 'NA'
  input$RESULT_sole_proprietor[i] <- 'NA'
  input$RESULT_gender[i] <- 'NA'
  input$RESULT_enumeration_date[i] <- 'NA'
  input$RESULT_last_updated[i] <- 'NA'
  input$RESULT_status[i] <- 'NA'
  input$RESULT_name[i] <- 'NA'
  input$RESULT_country_code[i] <- 'NA'
  input$RESULT_country_name[i] <- 'NA'
  input$RESULT_address_purpose[i] <- 'NA'
  input$RESULT_address_type[i] <- 'NA'
  input$RESULT_address_2[i] <- 'NA'
  input$RESULT_state[i] <- 'NA'
  input$RESULT_postal_code[i] <- 'NA'
  input$RESULT_telephone_number[i] <- 'NA'
  input$RESULT_code[i] <- 'NA'
  input$RESULT_desc[i] <- 'NA'
  input$RESULT_primary[i] <- 'NA'
  input$RESULT_taxonomystate[i] <- 'NA'
  input$RESULT_license[i] <- 'NA'
  next
}  


# if API call returns more than 0 results, add columns to the output:

if(api_content_count>0){
  # reset the iterator so we don't go out of bounds when selecting elements later 
  n = 1  
  # if we get more than 1 result per search
  if (api_content_count > 1){
    #loop though all results per search
    for (j in 1:length(api_content$results)){
      
      # if middle name returned in any result per search AND it is equal to the middle name in the input, PULL the NPI info for that user with matching middlename, otherwise pick the first result
      
      # functions around are just for more precise results like making sure to handle NA's, case sensitivity, no white spaces
      if(!is.null(api_content$results[[j]]$basic$middle_name) && trim(tolower(api_content$results[[j]]$basic$middle_name)) == trim(tolower(ifelse(is.na(middle_name), '', as.character(middle_name))))){
        # assigns the position of the middlename match to n
        n = j
        # optional, sets MatchCount to 1 since now the search is narrowed down
        api_content_count <- 1
        #breaking the search for matching middlename loop if successfully found the match so the n selector points to correct value
        break
      }
        if(is.null(api_content)){
          # assigns the position of the middlename match to n
          n = 0
          # optional, sets MatchCount to 1 since now the search is narrowed down
          api_content_count <- 0
          #breaking the search for matching middlename loop if successfully found the match so the n selector points to correct value
          break
      }
      else{
        # if not middlename searches matched, set the default selector value to 1 to pick the first API result for search parameters
        n = 1
      }
      #cat(j, trim(tolower(api_content$results[[j]]$basic$middle_name)), "\n")
      }
  }
    
    
    

  # RESULT_NPI column with NPI number from API for that row/person
  input$RESULT_NPI[i] <- api_content$results[[n]]$number
  
  # RESULT_Address1 column with Address1 value from API for that row/person
  input$RESULT_Address1[i] <- api_content$results[[n]]$addresses[[1]]$address_1
  
  # RESULT_City column with City value from API for that row/person
  input$RESULT_City[i] <- api_content$results[[n]]$addresses[[1]]$city
  
  # RESULT_IsPresent column with TRUE/FALSE flag where input row/user has been found in API
  input$RESULT_IsPresent[i] <- 'True'
  
  # IMPORTANT: RESULT_MatchCount column shows how many people were found in NPI with the SAME firsname, lastname living in the same state
  input$RESULT_MatchCount[i] <- api_content_count
  
  # Adding all possible result columns from API
  input$RESULT_enumeration_type[i] <- api_content$results[[n]]$enumeration_type
  input$RESULT_last_updated_epoch[i] <- api_content$results[[n]]$last_updated_epoch
  input$RESULT_created_epoch[i] <- api_content$results[[n]]$created_epoch
  input$RESULT_first_name[i] <- api_content$results[[n]]$basic$first_name
  input$RESULT_last_name[i] <- api_content$results[[n]]$basic$last_name
  input$RESULT_sole_proprietor[i] <- api_content$results[[n]]$basic$sole_proprietor
  input$RESULT_gender[i] <- api_content$results[[n]]$basic$gender
  input$RESULT_enumeration_date[i] <- api_content$results[[n]]$basic$enumeration_date
  input$RESULT_last_updated[i] <- api_content$results[[n]]$basic$last_updated
  input$RESULT_status[i] <- api_content$results[[n]]$basic$status
  input$RESULT_name[i] <- api_content$results[[n]]$basic$name
  input$RESULT_country_code[i] <- api_content$results[[n]]$addresses[[1]]$country_code
  input$RESULT_country_name[i] <- api_content$results[[n]]$addresses[[1]]$country_name
  input$RESULT_address_purpose[i] <- api_content$results[[n]]$addresses[[1]]$address_purpose
  input$RESULT_address_type[i] <- api_content$results[[n]]$addresses[[1]]$address_type
  input$RESULT_address_2[i] <- api_content$results[[n]]$addresses[[1]]$address_2
  input$RESULT_state[i] <- api_content$results[[n]]$addresses[[1]]$state
  input$RESULT_postal_code[i] <- api_content$results[[n]]$addresses[[1]]$postal_code
  
  # Loop to deal with instances when there are no phone numbers / incorrect formats provided
  if (is.null(api_content$results[[n]]$addresses[[1]]$telephone_number)) 
  {
    input$RESULT_telephone_number[i] <- 'NA'
    next
  }
  
  input$RESULT_telephone_number[i] <- api_content$results[[n]]$addresses[[1]]$telephone_number
  input$RESULT_code[i] <- api_content$results[[n]]$taxonomies[[1]]$code
  input$RESULT_desc[i] <- api_content$results[[n]]$taxonomies[[1]]$desc
  input$RESULT_primary[i] <- api_content$results[[n]]$taxonomies[[1]]$primary
  input$RESULT_taxonomystate[i] <- api_content$results[[n]]$taxonomies[[1]]$state
  input$RESULT_license[i] <- api_content$results[[n]]$taxonomies[[1]]$license
  
  
    

}




# optional line of code. Shows progress. Countdown of rows
cat("\r", " remaining: ", nrow(input) - i, "\r")
}


# writing output into output.csv file stored on the desktop
write.csv(input, file ='API_output_8500-10000.csv', row.names=FALSE)

# showing output in R window
View(input)

#
#
#
# Once all 10,000 records are extracted and saved in 4 csv files,
# we need to join them to produce a master file and then join with the CMDC list file
#
#
#
# Reading all 4 csv files before joining

API_output_1_2999 <- read.csv(file = "API_output_1-2999.csv", header = TRUE)
API_output_3000_5999 <- read.csv(file = "API_output_3000-5999.csv", header = TRUE)
API_output_6000_8499 <- read.csv(file = "API_output_6000-8499.csv", header = TRUE)
API_output_8500_10000 <- read.csv(file = "API_output_8500-10000.csv", header = TRUE)

# Combining all files in 1 master file before joining with the CMDC list file

API_output_combined_1_10000 <- rbind(API_output_1_2999,
                                      API_output_3000_5999,
                                      API_output_6000_8499,
                                      API_output_8500_10000)

# view(API_output_combined_1_10000)

# Creating a final csv file with all values
write.csv(API_output_combined_1_10000, file ='API_output_combined_1_10000.csv', row.names=FALSE)


# Joining data

cmdc_API_joined <- cmdc_npi_list %>%
  full_join (API_output_combined_1_10000, by = c("NPI_Number" = "npi_num"),
             suffix = c("_CMDC", "_API"))


# view(cmdc_API_joined)

# Writing a csv file with joined data

write.csv(cmdc_API_joined, file ='CMDC_NPI_LIST_x_API_joined.csv', row.names=FALSE)


# Using provided credentials for confirming access to S3 Bucket again

key <- "XYZ" # Shared key 
secret <- "XYZ" # Shared secret key
region <- "us-east-1"

Sys.setenv("AWS_ACCESS_KEY_ID" = key,
           "AWS_SECRET_ACCESS_KEY" = secret,
           "AWS_REGION" = region)

get_bucket(bucket = "etkachenko-use-case")



# Creating a separate folder for the first objective 

put_folder("Objective_1", bucket = "etkachenko-use-case")



# Uploading the combined CMDC NPI List data and npiregistry data to the ____ folder in S3
#
# writing file to S3
#
#
# putting combined file to the ____ folder with an upload progress bar
put_object(file = "CMDC_NPI_LIST_x_API_joined.csv" , object = "Objective_1/CMDC_NPI_LIST_x_API_joined.csv", bucket = "etkachenko-use-case", show_progress = TRUE)

# putting Script file to the ____ folder with an upload progress bar
put_object(file = "june 29, 2021 - case study, objective 1 - Final Version.R", object = "Objective_1/june 29, 2021 - case study, objective 1 - Final Version.R", bucket = "etkachenko-use-case", show_progress = TRUE)
