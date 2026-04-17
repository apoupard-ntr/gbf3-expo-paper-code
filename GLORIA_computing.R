options(scipen = 999)

# Load necessary libraries
install.packages("openxlsx")
library(openxlsx)
library(readxl)
library(tibble)
library(Matrix)

# Set the Sys.setenv (in SSP Cloud, click in My account > Connect to storage > Select "R (aws.S3)", copy the code and paste it here)

year = 2022
bucket <- "projet-esteem"

install.packages("aws.s3", repos = "https://cloud.R-project.org")

Sys.setenv("AWS_ACCESS_KEY_ID" = "TJN2MD6H17WU3FDEGVGO",
           "AWS_SECRET_ACCESS_KEY" = "4sCLuuwPSQp4WO53yzXyDC6o33EzaRD9QD6xGROs",
           "AWS_DEFAULT_REGION" = "us-east-1",
           "AWS_SESSION_TOKEN" = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJUSk4yTUQ2SDE3V1UzRkRFR1ZHTyIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sImF1ZCI6WyJtaW5pby1kYXRhbm9kZSIsIm9ueXhpYSIsImFjY291bnQiXSwiYXV0aF90aW1lIjoxNzYwNzAzODA1LCJhenAiOiJvbnl4aWEiLCJlbWFpbCI6InNhbnRvc2Nhcm5laXJvZy5leHRAYWZkLmZyIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTc2MjE2Mjk2MiwiZmFtaWx5X25hbWUiOiJTYW50b3MgQ2FybmVpcm8iLCJnaXZlbl9uYW1lIjoiR2FicmllbCIsImdyb3VwcyI6WyJVU0VSX09OWVhJQSIsImVzdGVlbSJdLCJpYXQiOjE3NjE1NTgxNjEsImlzcyI6Imh0dHBzOi8vYXV0aC5sYWIuc3NwY2xvdWQuZnIvYXV0aC9yZWFsbXMvc3NwY2xvdWQiLCJqdGkiOiJvbnJ0cnQ6ZDNjZDU5YmItMzgxNS04YTBkLWM1NDktYzc1MTg1ZjQ4OTYyIiwibG9jYWxlIjoiZnIiLCJuYW1lIjoiR2FicmllbCBTYW50b3MgQ2FybmVpcm8iLCJwb2xpY3kiOiJzdHNvbmx5IiwicHJlZmVycmVkX3VzZXJuYW1lIjoic2FudG9zY2FybmVpcm9nIiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iLCJkZWZhdWx0LXJvbGVzLXNzcGNsb3VkIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiIsImRlZmF1bHQtcm9sZXMtc3NwY2xvdWQiXSwic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBncm91cHMgZW1haWwiLCJzaWQiOiJkMDk5YWM3ZC0yZmQ2LTRiMTMtOTc1NS01OGJjMWEwNmFkZjciLCJzdWIiOiJlNmMyNjA4MS01MjEwLTRlMTktYWU2MC05YzE1YjgzZmUyNDAiLCJ0eXAiOiJCZWFyZXIifQ.zqb715N-Ijym_2AYEbH4q_pW4GP4MAjc-ep8rF5hAM-qbdSFCuBB4WuX-X3JrJ-R9wCGPieFclQzAD9adRDddw",
           "AWS_S3_ENDPOINT"= "minio.lab.sspcloud.fr")

library("aws.s3")
bucketlist(region="")

# Loading labels
label_Q <- s3read_using(FUN = data.table::fread, encoding = "UTF-8",
                        object = "Gloria/labels/label_Q.rds",
                        bucket = bucket, opts = list("region" = ""))

label_IO <- s3read_using(FUN = data.table::fread, encoding = "UTF-8",
                         object = "Gloria/labels/label_IO.rds",
                         bucket = bucket, opts = list("region" = ""))

label_FD <- s3read_using(FUN = data.table::fread, encoding = "UTF-8",
                         object = "Gloria/labels/label_FD.rds",
                         bucket = bucket, opts = list("region" = ""))

label_VA <- s3read_using(FUN = data.table::fread, encoding = "UTF-8",
                         object = "Gloria/labels/label_VA.rds",
                         bucket = bucket, opts = list("region" = ""))

# Loading data

x <- as.matrix(s3read_using(FUN = data.table::fread,
                            encoding = "UTF-8",
                            #Reading arguments
                            object = paste("Gloria/matrices/059/global/2022/x_2022.rds",sep=""), #the path and name of the file to read
                            bucket = bucket, #The name of the bucket (identifiant IDEP in "Mon compte")
                            opts = list("region" = "") #Purely technical option, but source of error if "region" = "" not specified
))

VA <- as.matrix(s3read_using(FUN = data.table::fread,
                             encoding = "UTF-8",
                             #Reading arguments
                             object = paste("Gloria/matrices/059/global/2022/VA_2022.rds",sep=""), #the path and name of the file to read
                             bucket = bucket, #The name of the bucket (identifiant IDEP in "Mon compte")
                             opts = list("region" = "") #Purely technical option, but source of error if "region" = "" not specified
))


FD <- as.matrix(s3read_using(FUN = data.table::fread,
                             encoding = "UTF-8",
                             #Reading arguments
                             object = paste("Gloria/matrices/059/global/2022/FD_2022.rds",sep=""), #the path and name of the file to read
                             bucket = bucket, #The name of the bucket (identifiant IDEP in "Mon compte")
                             opts = list("region" = "") #Purely technical option, but source of error if "region" = "" not specified
))


A <- as.matrix(s3read_using(FUN = data.table::fread,
                            encoding = "UTF-8",
                            #Reading arguments
                            object = paste("Gloria/matrices/059/global/2022/A_2022.rds",sep=""), #the path and name of the file to read
                            bucket = bucket, #The name of the bucket (identifiant IDEP in "Mon compte")
                            opts = list("region" = "") #Purely technical option, but source of error if "region" = "" not specified
))

L <- as.matrix(s3read_using(FUN = data.table::fread,
                            encoding = "UTF-8",
                            #Reading arguments
                            object = paste("Gloria/matrices/059/global/2022/L_2022.rds",sep=""), #the path and name of the file to read
                            bucket = bucket, #The name of the bucket (identifiant IDEP in "Mon compte")
                            opts = list("region" = "") #Purely technical option, but source of error if "region" = "" not specified
))



# Basic Results -----------------------------------------------------------

# --- Load and Prepare Data ---
Direct <- read_excel("data/toGLORIA/MRIO impact GLORIA sectors_AP1305.xlsx", sheet = "Gabriel")

# Define the columns to process
columns_to_process <- c(7, 9, 11, 13, 15)

# Define the custom names for each corresponding output Excel file
# The first name corresponds to the first column, the second to the second, and so on.
output_names <- c("AllanLoss", "Shen30GlobSPLoss", "Shen30GlobVulCLoss", "Shen30CountrySPLoss", "Shen30CountryVulCLoss")

# --- Loop, Calculate, and Save ---
# Loop through the columns using an index to match columns with custom names
for (i in seq_along(columns_to_process)) {
  
  col_index <- columns_to_process[i] # Get the current column number
  
  # --- Perform calculations, ensuring each result is a simple numeric vector ---
  Dir <- as.numeric(x * Direct[[col_index]])
  Tt  <- as.numeric(L %*% Dir)
  Ind <- as.numeric(Tt - Dir)
  
  # Apply capping logic, also ensuring results are simple vectors
  Total_capped <- as.numeric(ifelse(Tt > x, x, Tt))
  Ind_capped   <- as.numeric(Total_capped - Dir)
  
  # --- Create the final data frame using tibble() for robustness ---
  ResultsDF <- tibble(
    "Region" = as.vector(Direct[[1]]),
    "Sector" = as.vector(Direct[[2]]),
    "Dir" = Dir,
    "Ind not capped" = Ind,
    "Tt not capped" = Tt,
    "Total capped" = Total_capped,
    "Ind capped" = Ind_capped,
    "Output" = x,
    "Share of output loss" = Total_capped / x
  )
  
  # Assign the results to a separate data frame in the global environment
  assign(output_names[i], ResultsDF, envir = .GlobalEnv)
  
  # Define output filename using the custom name and save the results
  output_filename <- paste0(output_names[i], ".xlsx")
  write.xlsx(
    ResultsDF,
    file = output_filename,
    asTable = TRUE,
    overwrite = TRUE
  )
}


# Totals per income region  -----------------------------------------------------

# Sums without removing outliers
sum(AllanLoss[,6])*1000
sum(Shen30GlobSPLoss[,6])*1000
sum(Shen30GlobVulCLoss[,6])*1000
sum(Shen30CountrySPLoss[,6])*1000
sum(Shen30CountryVulCLoss[,6])*1000

# Country list
Countries <- data.frame(
  pais = Direct[seq(1, nrow(Direct), by = 120), 1],
  stringsAsFactors = FALSE
)

high_indices <- unique(c(8,11,12,15,20,21,27,31,32,33,42,43,44,47,
                         53,54,56,57,59,65,68,69,71,74,77,78,79,82,
                         87,88,94,95,96,103,114,115,117,118,121,125,
                         127,129,130,133,136,140,142,143,144,156,157))
middle_indices <- unique(c(7,9,10,13,19,22,23,24,26,29,34,35,39,40,
                           41,48,50,58,60,64,66,67,72,73,76,80,83,92,
                           98,100,101,109,110,122,128,131,138,148,150,
                           152,162))
low_indices <- unique(c(1,2,3,4,5,6,14,16,17,18,25,28,30,36,37,38,
                        45,46,49,51,52,55,61,62,63,70,75,81,84,85,
                        86,89,90,91,93,97,99,102,104,105,106,107,108,
                        111,112,113,116,119,120,123,124,126,132,134,
                        135,137,139,141,145,146,147,149,151,153,154,
                        155,158,159,160,161,163,164))

# Removing Belarus, Ethiopia and Zimbabwe
middle_indices_mod <- unique(c(7,9,10,13,19,22,24,26,29,34,35,39,40,
                               41,48,50,58,60,64,66,67,72,73,76,80,83,92,
                               98,100,101,109,110,122,128,131,138,148,150,
                               152,162))
low_indices_mod <- unique(c(1,2,3,4,5,6,14,16,17,18,25,28,30,36,37,38,
                            45,46,49,51,52,61,62,63,70,75,81,84,85,
                            86,89,90,91,93,97,99,102,104,105,106,107,108,
                            111,112,113,116,119,120,123,124,126,132,134,
                            135,137,139,141,145,146,147,149,151,153,154,
                            155,158,159,160,161,163))

### Allan - per group income

## Total

somas <- tapply(X   = AllanLoss[, 6],INDEX = (seq_len(nrow(AllanLoss)) - 1) %/% 120, FUN  = sum)

AllanLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(AllanLoss_Country[high_indices, ])
sum(AllanLoss_Country[middle_indices_mod, ])
sum(AllanLoss_Country[low_indices_mod, ])

sum(AllanLoss_Country)

# Total without BEL, ETH and ZWB
sum(AllanLoss_Country[high_indices, ]) + sum(AllanLoss_Country[middle_indices_mod, ]) + sum(AllanLoss_Country[low_indices_mod, ])

sum(AllanLoss_Country[high_indices, ])/sum(AllanLoss_Country)*100
sum(AllanLoss_Country[middle_indices_mod, ])/sum(AllanLoss_Country)*100
sum(AllanLoss_Country[low_indices_mod, ])/sum(AllanLoss_Country)*100

## Direct

somas <- tapply(X   = AllanLoss[, 3],INDEX = (seq_len(nrow(AllanLoss)) - 1) %/% 120, FUN  = sum)

AllanLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(AllanLoss_Country[high_indices, ])
sum(AllanLoss_Country[middle_indices_mod, ])
sum(AllanLoss_Country[low_indices_mod, ])

## Indirect

somas <- tapply(X   = AllanLoss[, 7],INDEX = (seq_len(nrow(AllanLoss)) - 1) %/% 120, FUN  = sum)

AllanLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(AllanLoss_Country[high_indices, ])
sum(AllanLoss_Country[middle_indices_mod, ])
sum(AllanLoss_Country[low_indices_mod, ])

### Shen30CountrySP - per group income

## Total

somas <- tapply(X   = Shen30CountrySPLoss[, 6],INDEX = (seq_len(nrow(Shen30CountrySPLoss)) - 1) %/% 120, FUN  = sum)

Shen30CountrySPLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30CountrySPLoss_Country[high_indices, ])
sum(Shen30CountrySPLoss_Country[middle_indices_mod, ])
sum(Shen30CountrySPLoss_Country[low_indices_mod, ])

sum(Shen30CountrySPLoss_Country)

# Total without BEL, ETH and ZWB
sum(Shen30CountrySPLoss_Country[high_indices, ]) + sum(Shen30CountrySPLoss_Country[middle_indices_mod, ]) + sum(Shen30CountrySPLoss_Country[low_indices_mod, ])


sum(Shen30CountrySPLoss_Country[high_indices, ])/sum(Shen30CountrySPLoss_Country)*100
sum(Shen30CountrySPLoss_Country[middle_indices_mod, ])/sum(Shen30CountrySPLoss_Country)*100
sum(Shen30CountrySPLoss_Country[low_indices_mod, ])/sum(Shen30CountrySPLoss_Country)*100

## Direct

somas <- tapply(X   = Shen30CountrySPLoss[, 3],INDEX = (seq_len(nrow(Shen30CountrySPLoss)) - 1) %/% 120, FUN  = sum)

Shen30CountrySPLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30CountrySPLoss_Country[high_indices, ])
sum(Shen30CountrySPLoss_Country[middle_indices_mod, ])
sum(Shen30CountrySPLoss_Country[low_indices_mod, ])

## Indirect

somas <- tapply(X   = Shen30CountrySPLoss[, 7],INDEX = (seq_len(nrow(Shen30CountrySPLoss)) - 1) %/% 120, FUN  = sum)

Shen30CountrySPLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30CountrySPLoss_Country[high_indices, ])
sum(Shen30CountrySPLoss_Country[middle_indices_mod, ])
sum(Shen30CountrySPLoss_Country[low_indices_mod, ])

### Shen30CountryVulC - per group income

## Total

somas <- tapply(X   = Shen30CountryVulCLoss[, 6],INDEX = (seq_len(nrow(Shen30CountryVulCLoss)) - 1) %/% 120, FUN  = sum)

Shen30CountryVulCLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30CountryVulCLoss_Country[high_indices, ])
sum(Shen30CountryVulCLoss_Country[middle_indices_mod, ])
sum(Shen30CountryVulCLoss_Country[low_indices_mod, ])

sum(Shen30CountryVulCLoss_Country)

# Total without BEL, ETH and ZWB
sum(Shen30CountryVulCLoss_Country[high_indices, ]) + sum(Shen30CountryVulCLoss_Country[middle_indices_mod, ]) + sum(Shen30CountryVulCLoss_Country[low_indices_mod, ])


sum(Shen30CountryVulCLoss_Country[high_indices, ])/sum(Shen30CountryVulCLoss_Country)*100
sum(Shen30CountryVulCLoss_Country[middle_indices_mod, ])/sum(Shen30CountryVulCLoss_Country)*100
sum(Shen30CountryVulCLoss_Country[low_indices_mod, ])/sum(Shen30CountryVulCLoss_Country)*100

## Direct

somas <- tapply(X   = Shen30CountryVulCLoss[, 3],INDEX = (seq_len(nrow(Shen30CountryVulCLoss)) - 1) %/% 120, FUN  = sum)

Shen30CountryVulCLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30CountryVulCLoss_Country[high_indices, ])
sum(Shen30CountryVulCLoss_Country[middle_indices_mod, ])
sum(Shen30CountryVulCLoss_Country[low_indices_mod, ])

## Indirect

somas <- tapply(X   = Shen30CountryVulCLoss[, 7],INDEX = (seq_len(nrow(Shen30CountryVulCLoss)) - 1) %/% 120, FUN  = sum)

Shen30CountryVulCLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30CountryVulCLoss_Country[high_indices, ])
sum(Shen30CountryVulCLoss_Country[middle_indices_mod, ])
sum(Shen30CountryVulCLoss_Country[low_indices_mod, ])

### Shen30GlobSP - per group income

## Total

somas <- tapply(X   = Shen30GlobSPLoss[, 6],INDEX = (seq_len(nrow(Shen30GlobSPLoss)) - 1) %/% 120, FUN  = sum)

Shen30GlobSPLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30GlobSPLoss_Country[high_indices, ])
sum(Shen30CountrySPLoss_Country[middle_indices_mod, ])
sum(Shen30CountrySPLoss_Country[low_indices_mod, ])

sum(Shen30GlobSPLoss_Country)

# Total without BEL, ETH and ZWB
sum(Shen30GlobSPLoss_Country[high_indices, ]) + sum(Shen30GlobSPLoss_Country[middle_indices_mod, ]) + sum(Shen30GlobSPLoss_Country[low_indices_mod, ])

sum(Shen30GlobSPLoss_Country[high_indices, ])/sum(Shen30GlobSPLoss_Country)*100
sum(Shen30GlobSPLoss_Country[middle_indices_mod, ])/sum(Shen30GlobSPLoss_Country)*100
sum(Shen30GlobSPLoss_Country[low_indices_mod, ])/sum(Shen30GlobSPLoss_Country)*100

## Direct

somas <- tapply(X   = Shen30GlobSPLoss[, 3],INDEX = (seq_len(nrow(Shen30GlobSPLoss)) - 1) %/% 120, FUN  = sum)

Shen30GlobSPLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30GlobSPLoss_Country[high_indices, ])
sum(Shen30GlobSPLoss_Country[middle_indices_mod, ])
sum(Shen30GlobSPLoss_Country[low_indices_mod, ])

## Indirect

somas <- tapply(X   = Shen30GlobSPLoss[, 7],INDEX = (seq_len(nrow(Shen30GlobSPLoss)) - 1) %/% 120, FUN  = sum)

Shen30GlobSPLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30GlobSPLoss_Country[high_indices, ])
sum(Shen30GlobSPLoss_Country[middle_indices_mod, ])
sum(Shen30GlobSPLoss_Country[low_indices_mod, ])

### Shen30GlobVulC - per group income

## Total

somas <- tapply(X   = Shen30GlobVulCLoss[, 6],INDEX = (seq_len(nrow(Shen30GlobVulCLoss)) - 1) %/% 120, FUN  = sum)

Shen30GlobVulCLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30GlobVulCLoss_Country[high_indices, ])
sum(Shen30GlobVulCLoss_Country[middle_indices_mod, ])
sum(Shen30GlobVulCLoss_Country[low_indices_mod, ])

sum(Shen30GlobVulCLoss_Country)

# Total without BEL, ETH and ZWB
sum(Shen30GlobVulCLoss_Country[high_indices, ]) + sum(Shen30GlobVulCLoss_Country[middle_indices_mod, ]) + sum(Shen30GlobVulCLoss_Country[low_indices_mod, ])

sum(Shen30GlobVulCLoss_Country[high_indices, ])/sum(Shen30GlobVulCLoss_Country)*100
sum(Shen30GlobVulCLoss_Country[middle_indices_mod, ])/sum(Shen30GlobVulCLoss_Country)*100
sum(Shen30GlobVulCLoss_Country[low_indices_mod, ])/sum(Shen30GlobVulCLoss_Country)*100

## Direct

somas <- tapply(X   = Shen30GlobVulCLoss[, 3],INDEX = (seq_len(nrow(Shen30GlobVulCLoss)) - 1) %/% 120, FUN  = sum)

Shen30GlobVulCLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30GlobVulCLoss_Country[high_indices, ])
sum(Shen30GlobVulCLoss_Country[middle_indices_mod, ])
sum(Shen30GlobVulCLoss_Country[low_indices_mod, ])

## Indirect

somas <- tapply(X   = Shen30GlobVulCLoss[, 7],INDEX = (seq_len(nrow(Shen30GlobVulCLoss)) - 1) %/% 120, FUN  = sum)

Shen30GlobVulCLoss_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30GlobVulCLoss_Country[high_indices, ])
sum(Shen30GlobVulCLoss_Country[middle_indices_mod, ])
sum(Shen30GlobVulCLoss_Country[low_indices_mod, ])



# Estimating Value-Added --------------------------------------------------

VAagg <- as.numeric(apply(VA, 2, sum))
VAX <- VAagg/x

# Shen Country SP - Excluded BEL, ETH, and ZWB

VAShen30CountryVulCLoss <- Shen30CountryVulCLoss[, 6]*VAX
VAShen30CountryVulCLoss_Country <- as.numeric(tapply(X = VAShen30CountryVulCLoss, 
                                          INDEX = (seq_len(nrow(VAShen30CountryVulCLoss)) - 1) %/% 120, 
                                          FUN = sum))

sum(VAShen30CountryVulCLoss_Country[high_indices], na.rm = TRUE) + 
  sum(VAShen30CountryVulCLoss_Country[middle_indices_mod], na.rm = TRUE) + 
  sum(VAShen30CountryVulCLoss_Country[low_indices_mod], na.rm = TRUE)

# Shen Country VulC - Excluded BEL, ETH, and ZWB

VAShen30CountrySPLoss <- Shen30CountrySPLoss[, 6]*VAX
VAShen30CountrySPLoss_Country <- as.numeric(tapply(X = VAShen30CountrySPLoss, 
                                                   INDEX = (seq_len(nrow(VAShen30CountrySPLoss)) - 1) %/% 120, 
                                                   FUN = sum))

sum(VAShen30CountrySPLoss_Country[high_indices], na.rm = TRUE) + 
  sum(VAShen30CountrySPLoss_Country[middle_indices_mod], na.rm = TRUE) + 
  sum(VAShen30CountrySPLoss_Country[low_indices_mod], na.rm = TRUE)

# Shen Global SP - Excluded BEL, ETH, and ZWB

VAShen30GlobalSPLoss <- Shen30GlobSPLoss[, 6]*VAX
VAShen30GlobalSPLoss_Country <- as.numeric(tapply(X = VAShen30GlobalSPLoss, 
                                                   INDEX = (seq_len(nrow(VAShen30GlobalSPLoss)) - 1) %/% 120, 
                                                   FUN = sum))

sum(VAShen30GlobalSPLoss_Country[high_indices], na.rm = TRUE) + 
  sum(VAShen30GlobalSPLoss_Country[middle_indices_mod], na.rm = TRUE) + 
  sum(VAShen30GlobalSPLoss_Country[low_indices_mod], na.rm = TRUE)


# Shen Global VulC - Excluded BEL, ETH, and ZWB

VAShen30GlobVulCLoss <- Shen30GlobVulCLoss[, 6]*VAX
VAShen30GlobVulCLoss_Country <- as.numeric(tapply(X = VAShen30GlobVulCLoss, 
                                                  INDEX = (seq_len(nrow(VAShen30GlobVulCLoss)) - 1) %/% 120, 
                                                  FUN = sum))

sum(VAShen30GlobVulCLoss_Country[high_indices], na.rm = TRUE) + 
  sum(VAShen30GlobVulCLoss_Country[middle_indices_mod], na.rm = TRUE) + 
  sum(VAShen30GlobVulCLoss_Country[low_indices_mod], na.rm = TRUE)



# Per capita consumption estimation ---------------------------------------

# Global consumption for the affected sectors 

sum(FD[((seq_len(nrow(FD)) - 1) %% 120) < 20, ])

blocks <- (seq_len(nrow(FD)) - 1) %/% 120
mask   <- ((seq_len(nrow(FD)) - 1) %% 120) < 20
sumFD <- unname(
  tapply(
    rowSums(FD)[mask],
    blocks[mask],
    sum
  )
)

sumFD <- data.frame(sumFD = as.numeric(sumFD), stringsAsFactors = FALSE)

sum(sumFD[high_indices, ])
sum(sumFD[middle_indices, ])
sum(sumFD[low_indices, ])

# Per capita consumption for comparison

Population <- read_excel("API_SP.POP.TOTL_DS2_en_excel_v2_19296.xlsx", sheet = "Sheet2")
Pop <- Population[1:164,c(2,3)]
sumFD <- sumFD*1000

FDperCapita <- as.data.frame(sumFD/Pop[,2])
str(FDperCapita)

rownames(FDperCapita) <- t(Countries)
rownames(sumFD) <- t(Countries)

(sum(sumFD[high_indices, ])/sum(Pop[high_indices,2]))
(sum(sumFD[middle_indices_mod, ])/sum(Pop[middle_indices_mod,2]))
(sum(sumFD[low_indices_mod, ])/sum(Pop[low_indices_mod,2]))

sum(sumFD)/sum(Pop[,2])


# Gini --------------------------------------------------------------------


library(ineq)
rates <- c(A = 0.005, B = 0.006, C = 0.010, D = 0.00125)


# 2. Compute the Lorenz object
lc <- Lc(sort(unlist(FDperCapita)))

# 3. Plot the Lorenz curve
plot(lc,
     col      = "blue",
     lwd      = 2,
     main     = "Lorenz Curve of Per-Person Consumption Rates",
     xlab     = "Cumulative Share of Countries",
     ylab     = "Cumulative Share of Consumption Rate")
abline(0, 1, lty = 2)  # line of equality


# 4. (Optional) Compute the Gini coefficient
gini <- Gini(rates)
cat("Gini coefficient =", round(gini, 3), "\n")


# Impact/Output for countries ---------------------------------------------
## Comparar Impacto vs. total output dos países

somas <- tapply(X   = x,INDEX = (seq_len(nrow(x)) - 1) %/% 120, FUN  = sum)
x_per_country <- data.frame(soma_120 = as.numeric(somas))

## Shen Global SP - output

somas <- tapply(X   = Shen30GlobSPLoss[, 6],INDEX = (seq_len(nrow(Shen30GlobSPLoss)) - 1) %/% 120, FUN  = sum)
Shen30GlobLoss_Country <- data.frame(soma_120 = as.numeric(somas))

Shen30Glob_Country <- Shen30GlobLoss_Country/x_per_country*100

Shen30Glob_Country$income_group <- NA_character_
Shen30Glob_Country$income_group[high_indices]   <- "high income"
Shen30Glob_Country$income_group[middle_indices_mod] <- "middle income"
Shen30Glob_Country$income_group[low_indices_mod]    <- "low income"

rownames(Shen30Glob_Country) <- t(Countries)


# Separating Countries (China, EUA, Brazil and India) and income groups-------------------------

# Indices in order to separate the countries according to World Bank's regions:

# East Asia and Pacific
HighEAP <- c(11,27,42,68,82,87,117,136)
MidEAP <- c(34,72,109,148)
LowEAP <- c(4,86,89,104,105,123,124,126,160)

# Europe and Central Asia
HighECA <- c(12,15,32,42,43,44,47,53,54,56,57,59,65,69,71,74,77,79,94,95,96,103,114,115,125,127,130,140,142,143,144)
MidECA <- c(2,7,10,13,19,22,23,60,83,98,101,131,150,152)
LowECA <- c(85,149,155,158)

# Latin America and the Caribbean
HighLAC <- c(21,33,121,156)
MidLAC <- c(1,9,24,26,39,40,41,48,50,66,67,80,100,122,128,138)
LowLAC <- c(25,113,159)

# Middle East and North Africa
HighMENA <- c(8,20,78,88,118,128,133)
MidMENA <- c(76,92)
LowMENA <- c(46,49,51,70,75,81,90,97,120,145,151,161)

# North America
HighNA <- c(31,157)

# South Asia
MidSA <- c(73)
LowSA <- c(5,18,28,93,116,119)

# Sub-Saharan Africa
MidSSA <- c(29,35,58,64,110,162)
LowSSA <- c(3,6,14,16,17,30,36,37,38,45,52,55,61,62,63,84,91,99,102,106,107,108,111,112,132,134,135,137,139,141,146,147,153,154,163,164)

# Define the region indices
# (For LAC, we combine High, Mid, and Low; for SSA, SA, etc., we combine Mid and Low only)
LAC    <- c(21,33,121,156, 1,9,24,26,39,40,41,48,50,66,67,80,100,122,128,138, 25,113,159)
SSA    <- c(MidSSA, LowSSA)  # previously defined MidSSA and LowSSA
SA     <- c(MidSA, LowSA)    # previously defined MidSA and LowSA
MENA_H <- HighMENA           # previously defined HighMENA
MENA_M <- c(MidMENA, LowMENA) # previously defined MidMENA and LowMENA
EAP_H  <- HighEAP            # previously defined HighEAP
EAP_M  <- c(MidEAP, LowEAP)   # previously defined MidEAP and LowEAP
ECA_H  <- HighECA            # previously defined HighECA
ECA_M  <- c(MidECA, LowECA)   # previously defined MidECA and LowECA
NA_reg <- HighNA             # previously defined HighNA

## SHEN 30 Global SP

# Total 
somas <- tapply(X   = Shen30GlobSPLoss[, 6],INDEX = (seq_len(nrow(Shen30GlobSPLoss)) - 1) %/% 120, FUN  = sum)
Shen30GlobSP_Country <- data.frame(soma_120 = as.numeric(somas))

sum(Shen30GlobSP_Country[34,])
sum(Shen30GlobSP_Country[EAP_M,])-sum(Shen30GlobSP_Country[34,])

sum(Shen30GlobSP_Country[157,])
sum(Shen30GlobSP_Country[NA_reg,])-sum(Shen30GlobSP_Country[157,])

sum(Shen30GlobSP_Country[26,])
sum(Shen30GlobSP_Country[LAC,])-sum(Shen30GlobSP_Country[26,])

sum(Shen30GlobSP_Country[73,])
sum(Shen30GlobSP_Country[SA,])-sum(Shen30GlobSP_Country[73,])

sum(Shen30GlobSP_Country[high_indices,]) - sum(Shen30GlobSP_Country[157,])
sum(Shen30GlobSP_Country[middle_indices_mod,]) - sum(Shen30GlobSP_Country[c(26,34,73),])
sum(Shen30GlobSP_Country[low_indices_mod,])


# Sectoral Analysis -------------------------------------------------------

## SECTORAL ANALYSIS

matriz <- matrix(Shen30GlobSPLoss[[3]], ncol = 120, byrow = TRUE)
resultadoH <- as.data.frame(colSums(matriz[high_indices,]))
resultadoM <- as.data.frame(colSums(matriz[middle_indices_mod,]))
resultadoL <- as.data.frame(colSums(matriz[low_indices_mod,]))

ResultadoD <- resultadoH + resultadoL + resultadoM

matriz2 <- matrix(Shen30GlobSPLoss[[6]], ncol = 120, byrow = TRUE)
resultadoH <- as.data.frame(colSums(matriz2[high_indices,]))
resultadoM <- as.data.frame(colSums(matriz2[middle_indices_mod,]))
resultadoL <- as.data.frame(colSums(matriz2[low_indices_mod,]))

ResultadoT <- resultadoH + resultadoL + resultadoM

resultadoF <- cbind(label_IO[1:120,3],ResultadoD, ResultadoT-ResultadoD, ResultadoT)

write.xlsx(
  x         = resultadoF,         # your data frame
  file      = "SectoralLow.xlsx",  # output filename (in working directory)
  asTable   = FALSE,                     # write as an Excel table (optional)
  overwrite = TRUE                      # overwrite existing file if present
)


# Other Ecological Effects ------------------------------------------------

## OTHER ECOLOGICAL EFFECTS

Q <- s3read_using(FUN = data.table::fread, encoding = "UTF-8",
                  object = paste("Gloria/matrices/059/Gloria downloadfiles/2022/TQ",sep=""),
                  bucket = bucket, opts = list("region" = ""))

# Creadting Q table
QT <- matrix(rep(NA,(120*164)*nrow(Q)),ncol=(120*164))
Q <- as.matrix(Q)
for (i in 1:164) {
  QT[,(i*120-119):(i*120)] <- Q[,((i-1)*240+1):(i*240-120)]
}

dim(QT)

Q2 <- as.matrix(QT[3169,])/(x+0.00001) # GHG Emissions (EDGAR)
Q2 <- cbind(Q2,apply(QT[373:378,],2,FUN=sum)/(x+0.00001)) # Land use
Q2 <- cbind(Q2,apply(QT[385:390,],2,FUN=sum)/(x+0.00001)) # Biodiversity loss
Q2 <- cbind(Q2,apply(QT[393:394,],2,FUN=sum)/(x+0.00001)) # Blue water consumption
Q2 <- cbind(Q2,apply(QT[1:367,],2,FUN=sum)/(x+0.00001)) # Material use, total
Q2 <- cbind(Q2,apply(QT[379:384,],2,FUN=sum)/(x+0.00001)) # Energy, total

Shock <- Shen30GlobSPLoss[,6]
sum(as.matrix(Shock*Q2[,1]))/sum(QT[3169,])*100
sum(as.matrix(Shock*Q2[,2]))/sum(QT[373:378,])*100
sum(as.matrix(Shock*Q2[,3]))/sum(QT[385:390,])*100
sum(as.matrix(Shock*Q2[,4]))/sum(QT[393:394,])*100
sum(as.matrix(Shock*Q2[,5]))/sum(QT[1:367,])*100
sum(as.matrix(Shock*Q2[,6]))/sum(QT[379:384,])*100

sum(Shock*Q2[,2])/sum(apply(QT[373:378,],2,FUN=sum))
sum(Shock*Q2[,3])/sum(apply(QT[385:390,],2,FUN=sum))
sum(Shock*Q2[,4])/sum(apply(QT[393:394,],2,FUN=sum))
sum(Shock*Q2[,5])/sum(apply(QT[1:367,],2,FUN=sum))
sum(Shock*Q2[,6])/sum(apply(QT[379:384,],2,FUN=sum))


# Double Checking  --------------------------------------------------------

FDTot <- apply(FD,1,sum)

LL <- as.matrix(L)

xtot <- LL%*%FDTot
sum(xtot)
sum(x)

Teste <- x*Q2[,1]
sum(Teste)


FD1 <- FD[,199:204]
FD1Tot <- apply(FD1,1,sum)

FD2 <- FD[,-c(199,200,201,202,203,204)]
FD2Tot <- apply(FD2,1,sum)

xtot1 <- LL%*%FD1Tot
xtot2 <- LL%*%FD2Tot

sum(xtot1)+sum(xtot2)


# GDP estimation -----------------------------------------------------------------

Global_GDP <- 101770911729948
sum(Shen30GlobSPLoss[,6])*1000/Global_GDP*100

# Chord Diagram Data ------------------------------------------------------

Groupings <- read_excel("Country groupings.xlsx")
row_indices <- rep(1:nrow(Groupings), each = 120)
Groupings2 <- Groupings[row_indices, ]


# --- DYNAMICALLY DISCOVER AND DEFINE ROW INDEX GROUPS ---
# 1. Find all unique category names from Groupings2
unique_categories <- unique(Groupings2[[3]])

# 2. Create a descriptive group name for each unique category
full_group_names <- paste0(unique_categories, "_Rows")

# 2a. Truncate sheet names to a maximum of 31 characters to comply with Excel's limit.
group_names <- substr(full_group_names, 1, 31)

# 2b. Check for and warn about duplicate names that might arise after truncation
if (any(duplicated(group_names))) {
  warning("Truncating sheet names resulted in duplicate names. Consider using shorter category names in your Groupings2 file.")
}

# 3. For each category, find all the rows that match it and store the indices in a list
row_indices_list <- lapply(unique_categories, function(category) {
  which(Groupings2[[3]] == category)
})

# 4. Create the final tibble that the loop will use
rows_to_process <- tibble(
  GroupName = group_names,
  RowIndices = row_indices_list
)


# --- Define Column Scenario ---
col_index <- 7
col_name <- "AllanLoss_Income"


# --- STEP 1: Initialize a list to store all result data frames ---
results_list <- list()


# --- LOOP through each AUTOMATICALLY DISCOVERED ROW GROUP ---
for (j in 1:nrow(rows_to_process)) {
  
  group_name <- rows_to_process$GroupName[j]
  row_indices <- rows_to_process$RowIndices[[j]]
  
  if (length(row_indices) == 0) {
    print(paste("Skipping group", group_name, "as no rows were found."))
    next
  }
  
  # --- Perform calculations on the FULL data dimension ---
  temp_direct_for_shock <- numeric(nrow(Direct))
  temp_direct_for_shock[row_indices] <- Direct[[col_index]][row_indices]
  Dir <- x * temp_direct_for_shock
  
  Tt  <- as.numeric(L %*% Dir)
  Ind <- as.numeric(Tt - Dir)
  
  Total_capped <- as.numeric(ifelse(Tt > x, x, Tt))
  Ind_capped   <- as.numeric(Total_capped - Dir)
  
  # --- Create the final data frame, ensuring it has all 19680 rows ---
  ResultsDF <- tibble(
    "Region" = as.vector(Direct[[1]]),
    "Sector" = as.vector(Direct[[2]]),
    "Dir" = Dir,
    "Ind not capped" = Ind,
    "Tt not capped" = Tt,
    "Total capped" = Total_capped,
    "Ind capped" = Ind_capped,
    "Output" = x,
    "Share of output loss" = Total_capped / x
  )
  
  # --- STEP 2: Store the current result in the list. The group_name becomes the sheet name ---
  results_list[[group_name]] <- ResultsDF
  
  # Assign the results to a separate data frame in the global environment (optional)
  df_name <- paste(col_name, group_name, sep = "_")
  assign(df_name, ResultsDF, envir = .GlobalEnv)
  
  # The write.xlsx call is REMOVED from inside the loop.
}

# --- STEP 3: AFTER THE LOOP - Write the entire list to a single Excel file ---
# The list 'results_list' now contains all your data frames.
# 'write.xlsx' will create one sheet for each element in the list.
output_filename <- paste0(col_name, "_All_Groups.xlsx")
write.xlsx(
  results_list,
  file = output_filename,
  asTable = TRUE
)


sum(Shen30GlobalSPLoss_Africa_Rows[,6]) + sum(Shen30GlobalSPLoss_Asia_Rows[,6]) + sum(Shen30GlobalSPLoss_Europe_Rows[,6]) + sum(`Shen30GlobalSPLoss_Latin America and the Caribbean`[,6]) + sum(`Shen30GlobalSPLoss_Northern America_Rows`[,6]) + sum(Shen30GlobalSPLoss_Oceania_Rows[,6])


sum(Shen30GlobSPLoss[,6])



# Price Model -------------------------------------------------------------

# Standard Model

Vbase <- as.matrix(apply(VA, 2, sum))
xp <- x + 0.00001
V <- Vbase/xp
P <- t(L) %*% V

# From Weber, we can define endogenous and exogenous prices
# We can study import prices
# CPI based on consumption structure
# Rebote na demanda com elasticidades -> Magacho e Mardones

# Shen 30 Global
Dir <- as.matrix(Direct[, 9])  # Creating a vector only with numeric values
Dir <- unlist(Dir, use.names = FALSE)
Dir <- x*Dir

VShen <- V*Dir
VShen <- Vbase+VShen
Vp <- VShen/xp
PShen <- t(L) %*% Vp

Price <- cbind(label_IO[,c(1,3)], P, PShen, PShen-P)



# Ghosh & Altimiras Models ------------------------------------------------
Z <- as.matrix(s3read_using(FUN = data.table::fread,
                            encoding = "UTF-8",
                            #Reading arguments
                            object = paste("Gloria/matrices/059/global/2022/IO_2022.rds",sep=""), #the path and name of the file to read
                            bucket = bucket, #The name of the bucket (identifiant IDEP in "Mon compte")
                            opts = list("region" = "") #Purely technical option, but source of error if "region" = "" not specified
))

xmat <- x + 0.01
xmat <- diag(as.vector(xmat))
xmat <- solve(xmat)
B <- xmat %*% as.matrix(Z)
I <- diag(ncol(B))
G <- solve(I-B)

kappa_IB <- kappa(I-B)
print(kappa_IB)

# Test
?apply
V <- as.matrix(apply(VA,2,sum))
V <- V + 0.0001
R <- t(V) %*% G

