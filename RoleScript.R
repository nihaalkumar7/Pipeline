
options(java.parameters = "-Xmx40000m")
library(salesforcer)
library(dplyr)
library(ggplot2)
library(lubridate)
library(sqldf)
library(tidyverse)
library(DT)
library(readr)
library(writexl)
library(readxl)
library(readr)
library(arsenal)
library(odbc)

setwd("C:/Users/nikumar/OneDrive - Barracuda Networks, Inc/Desktop/R Projects/Pipeline")
Credentials <- read.csv("Salesforce Credentials.csv")
username <- as.character(Credentials$Username[1])
password <- as.character(Credentials$Password[1])
SecurityToken <- as.character(Credentials$SecurityToken[1])

session <- sf_auth(username, password, SecurityToken)



#Upload quota roles

  
setwd("C:/Users/nikumar/OneDrive - Barracuda Networks, Inc/Desktop/R Projects/Pipeline")
  
Q11 <- read_xlsx("Additional Roles1.xlsx")
Q11 <- Q11 %>% select(-Key,-Count)
col1 <- which(colnames(Q11) == "M1")
col2 <- ncol(Q11)
  
Q12 <- Q11 %>% gather(ValidMonth, Month, col1:col2) %>%
    filter(FY20.Sales.Plan != "cover" & is.na(Salesforce.User.Id) == F & Salesforce.User.Id != "Months" & Salesforce.User.Id != "" & is.na(Month) == F) %>%
    select(Month, Salesforce.User.Id, FY20.Sales.Plan, Fixed.Name) %>% distinct()
  
Q12 <- Q12 %>% mutate(value = 1,grouping = paste(Salesforce.User.Id, Month)) %>%
                      arrange(grouping, FY20.Sales.Plan) %>%
                      mutate(choice = ave(value, grouping, FUN = cumsum))
Q13 <- Q12 %>% filter(choice == 1) %>% select(Month, Salesforce.User.Id, FY20.Sales.Plan, Fixed.Name)
  
CudaAzure <- dbConnect(odbc(), "CudaAzure1", uid = "jori", pwd = "xm2!p49kl")
dbWriteTable(CudaAzure, "SalesOperationsRoles", Q13, overwrite = TRUE)
  
rm(list = c("Q11", "Q12", "Q13"))  

