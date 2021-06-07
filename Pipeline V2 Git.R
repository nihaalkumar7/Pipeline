---
  title: "Pipeline_Report"
output: html_document
---
  
  ```{r setup, include=FALSE}
knitr::opts_chunk$set(echo = TRUE)
```


```{r}
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
```

```{r}

session <- sf_auth(username, password, SecurityToken)
```


```{r}
Opp <- sf_query_bulk("SELECT Id, CloseDate, AccountId, OwnerId, Reseller__c, CreatedDate, Name, Distributor__c, LeadSource, StageName, Business_Group__c, Amount, RecordTypeId, Migration__c,Deal_Reg_Type__c,CurrencyIsoCode,
                      Closed_Lost_Reason__c FROM Opportunity")
```

```{r}
Account <- sf_query_bulk("SELECT Id, Name, Type, Employees_Formula__c, OwnerId, Partner_Level__c, Focus_Partner__c, Nationwide_Partner_for_Channel_Comms__c, Territory_ID__c FROM Account")
```

```{r}
Quotes <- sf_query_bulk("SELECT Id, SBQQ__Opportunity2__c, SBQQ__Primary__c, Created_Date__c, SBQQ__SubscriptionTerm__c FROM SBQQ__Quote__c")
```

```{r}
Record_Type <- sf_query_bulk("SELECT Id, Name FROM RecordType")
```

```{r}
Terr <- sf_query_bulk("SELECT Id, Name, Sub_Territory2__c,Territory2__c, Region2__c, Sub_Theater2__c, Theater2__c FROM Territory2", object_name = "Territory2")
```

```{r}
Product <- sf_query_bulk("SELECT Id, ProductCode, Name, Family, SBCF_Product_Family_Code__c, SBCF_Form_Factor__c, prod_subtype__c, Base_SKU__c FROM Product2", object_name = "Product2")
```

```{r}
User <- sf_query_bulk("SELECT Id,Name FROM User")
```

```{r}
Subscription <- sf_query_bulk("SELECT Id, SBQQ__Product__c, SBQQ__Quantity__c, SBCF_Serial_Number__c, Renewed_By_Subscription__c, SBQQ__StartDate__c, SBQQ__EndDate__c, SBQQ__Account__c FROM SBQQ__Subscription__c")
```

```{r}
QuoteLineGroup <- sf_query_bulk("SELECT Id, SBQQ__Number__c, SBQQ__Optional__c, SBQQ__Quote__c,Name FROM SBQQ__QuoteLineGroup__c")
```

# QUERY FOR QUOTELINE OBJECT:SELECT CurrencyIsoCode, Extended_Price__c,Id,SBQQ__NetTotal__c,SBQQ__Quote__c,SBQQ__PartnerDiscount__c,SBQQ__PartnerTotal__c, SBQQ__Product__c,SBQQ__Group__c,SBQQ__SubscriptionTerm__c, SBCF_Document_Term__c FROM SBQQ__QuoteLine__c
```{r}
#QuoteLine <- read.csv("SBQQ__QuoteLine__c.csv")
QuoteLine <- sf_query_bulk("SELECT CurrencyIsoCode, Extended_Price__c,Id,SBQQ__NetTotal__c,SBQQ__Quote__c,SBQQ__PartnerDiscount__c,SBQQ__PartnerTotal__c, SBQQ__Product__c,SBQQ__Group__c,SBQQ__SubscriptionTerm__c, SBCF_Document_Term__c FROM SBQQ__QuoteLine__c", object_name = "SBQQ__QuoteLine__c")
```

```{r}
setwd("C:/Users/jgrossman/OneDrive - Barracuda Networks, Inc/Barracuda1/Commisions/Quotas")
Currency <- read.csv("Currency.csv")

setwd("C:/Users/jgrossman/OneDrive - Barracuda Networks, Inc/Renewals/R Inputs")
Prod_Category <- read.csv("Product Category Mapping.csv")
Pseudo_Prod_Category <- read.csv("Pseudo Product Family Mapping.csv")
colnames(Pseudo_Prod_Category)[1] <- "Sub.Product"
```


#Rename Fields for consistency
```{r}
Product01 <- Product %>% rename(Product2Id = Id,Product_Name = Name)
Opp01 <- Opp %>% rename(Opportunity_Id = Id, Opportunity_OwnerId = OwnerId,Opportunity_Name = Name, OpportunityCurrency = CurrencyIsoCode)
Quotes01 <- Quotes %>% rename(Quote_Id = Id, Opportunity_Id = SBQQ__Opportunity2__c, Quote_CreatedDate = Created_Date__c, Quote_SubscriptionTerm = SBQQ__SubscriptionTerm__c)
Terr01 <- Terr %>% rename(Sub_Territory__c = Sub_Territory2__c, Territory__c = Territory2__c, Region__c = Region2__c, Sub_Theater__c = Sub_Theater2__c, Theater__c = Theater2__c,Territory_Name = Name)
User01 <- User %>% rename(User_Id = Id, Rep_Name = Name)
Subscription01 <- Subscription %>% rename(SubStartDate = SBQQ__StartDate__c, SubEndDate = SBQQ__EndDate__c,Subscription_Id = Id)
QuoteLine01 <- QuoteLine %>% mutate(SBQQ__SubscriptionTerm__c = ifelse(is.na(SBQQ__SubscriptionTerm__c) | SBQQ__SubscriptionTerm__c == "", SBCF_Document_Term__c, SBQQ__SubscriptionTerm__c)) %>% rename(QuoteLine_Id = Id)
QuoteLineGroup01 <- QuoteLineGroup %>% rename(Group_Name = Name)
Record_Type01 <- Record_Type %>% rename(RecordTypeId = Id, 
                                        Record_Name = Name)
```

#Product Category Mapping
```{r}
Prod_Category01 <- Prod_Category %>% select(Id,Sub.Product) %>% distinct()
```
#Map product categories with product object
```{r}
Product02 <- merge(Product01, Prod_Category01, by.x = "Product2Id", by.y = "Id",all.x = TRUE)
```

#Add in Pseudo Product Family Mapping
```{r}
Product03 <- merge(Product02,Pseudo_Prod_Category, by = "Sub.Product", all.x = TRUE)
```



#Create Opportunity Dates
```{r}
Opp01$Month <- month(Opp01$CloseDate)
Opp01$Year <- year(Opp01$CloseDate)
Opp01$FiscalMonth <- ifelse(Opp01$Month < 3, Opp01$Month + 10, Opp01$Month - 2)
Opp01$FiscalYear <- ifelse(Opp01$Month < 3, Opp01$Year, Opp01$Year + 1)
Opp01$FiscalHalf <- paste(as.character(Opp01$FiscalYear), as.character(ifelse(Opp01$FiscalMonth < 7, 1, 2)), sep = "")
Opp01$Quarter <- ifelse(Opp01$Month <= 2, 4, ifelse(Opp01$Month <= 5, 1, ifelse(Opp01$Month <= 8, 2, ifelse(Opp01$Month <= 11, 3, ifelse(Opp01$Month == 12, 4, "Error")))))
Opp01$Semester <- ifelse(Opp01$FiscalMonth < 7, 1, 2)
Opp01$Month <- ifelse(str_length(Opp01$Month) == 1, paste("0", Opp01$Month, sep = ""), Opp01$Month)
Opp01$QuotaPeriod <- paste(Opp01$Year, Opp01$Month, sep = "")
```




#filter opportunity
```{r}
Opp02 <- Opp01 %>% filter(CreatedDate >= as.Date("2020-09-01") | CloseDate >= as.Date("2020-09-01")) %>% filter(Business_Group__c == "Core")
Opp02$CreatedDate <- as.Date(Opp02$CreatedDate)
```

#Merge opportunity with Record Type
#Filter for new business and core only
```{r}
Opp03 <-  merge(Opp02, Record_Type01, by = "RecordTypeId", all.x = TRUE)
Opp03 <- Opp03 %>% filter(Record_Name %in% c("Locked New Business", "New Business"))
```

```{r}
Quotes01 <- Quotes01 %>% mutate(value = 1, QuoteCount = ave(value,Opportunity_Id, FUN = sum)) %>%
  mutate(grouping = paste(Opportunity_Id,SBQQ__Primary__c), Primary_Count = ave(value, grouping, FUN = sum)) %>%
  arrange(Opportunity_Id,desc(SBQQ__Primary__c),Quote_CreatedDate) %>%
  mutate(Choice = ave(value,Opportunity_Id,FUN = cumsum))
Quotes02 <- Quotes01 %>% filter(Choice == 1) %>% select(Opportunity_Id, Quote_Id, SBQQ__Primary__c, Quote_CreatedDate, Quote_SubscriptionTerm)
```

#Join Quote with Opps
```{r}
Main <- merge(Opp03, Quotes02, by = "Opportunity_Id", all.x = TRUE)
```

#Line item level discount logic
```{r}
QuoteLine01 <- QuoteLine01 %>% 
  mutate(Partner_Total = round((SBQQ__NetTotal__c/ (100 - ifelse(is.na(SBQQ__PartnerDiscount__c) == T,0, SBQQ__PartnerDiscount__c)))*100,2)) %>%
  mutate(Discount = ifelse(Extended_Price__c == 0 &  Partner_Total > 0,
                           100,
                           ifelse(SBQQ__NetTotal__c == 0 | Partner_Total == 0 | SBQQ__NetTotal__c < 0,
                                  0, 
                                  round((1-(Partner_Total/Extended_Price__c))*100,2))))
```

```{r}
#Test11 <- QuoteLine01 %>% mutate(ratio = Partner_Total/ SBQQ__PartnerTotal__c) %>% filter(ratio == 1)
```

#Add on QuoteLine
```{r}
Main01 <- merge(Main, QuoteLine01, by.x = "Quote_Id",by.y = "SBQQ__Quote__c",all.x = TRUE)
```

#Adjust Subscription Term
```{r}
Main01 <- Main01 %>% mutate(SBQQ__SubscriptionTerm__c = ifelse(is.na(SBQQ__SubscriptionTerm__c) | SBQQ__SubscriptionTerm__c == "",
                                                               ifelse(is.na(Quote_SubscriptionTerm), 12, Quote_SubscriptionTerm), SBQQ__SubscriptionTerm__c))
```

#merge with QuoteLine
```{r}
Main02 <- merge(Main01, QuoteLineGroup01, by.x = "SBQQ__Group__c", by.y = "Id",all.x = TRUE)
```

#Create Required/Optional Field
```{r}
Main02 <- Main02 %>% mutate(Required_or_Optional = ifelse(is.na(SBQQ__Optional__c) == T | SBQQ__Optional__c == "TRUE", "Optional", "Required"))

Main02 <- Main02 %>% mutate(grouping = paste(Opportunity_Id, Quote_Id),
                            value = ifelse(Required_or_Optional == "Required", 1, 0),
                            check = ave(value, grouping, FUN = sum)) %>%
  mutate(Required_or_Optional = ifelse(check >  0 | is.na(check), as.character(Required_or_Optional), "Required"))
```

#adjust dollar amounts for currencies
```{r}
C1 <- Currency %>% select(FiscalYear,Conversion,Base,To) %>% distinct()
C2 <- C1 %>% filter(is.na(Conversion) == FALSE & FiscalYear == 2022) %>% filter(Conversion != 0) %>% rename(SplitPriceConversion = Conversion) %>% select(-FiscalYear) %>% distinct()

Main02 <- Main02 %>% mutate(Currency = ifelse(is.na(CurrencyIsoCode), as.character(OpportunityCurrency), as.character(CurrencyIsoCode)),
                            QC = "USD")
Main03 <- merge(Main02, C2, by.x = c("Currency","QC"),by.y = c("Base","To"),all.x = TRUE) %>% filter(Required_or_Optional == "Required")
```

#Bring in account info for the end user
```{r}
Account01 <- Account %>% rename(AccountId = Id, 
                                Account_Type = Type, 
                                Account_Name = Name, 
                                Account_OwnerId = OwnerId, 
                                Account_Territory_Id = Territory_ID__c) %>% 
  select(-Partner_Level__c,-Focus_Partner__c, -Nationwide_Partner_for_Channel_Comms__c)
Main04 <- merge(Main03, Account01, by = "AccountId", all.x = TRUE)
```

#Bring in account info for the reseller
```{r}
Reseller <- Account %>% rename(Reseller__c = Id, 
                               Reseller_Type = Type, 
                               Reseller_Name = Name, 
                               Reseller_OwnerId = OwnerId, 
                               Reseller_Territory_ID__c = Territory_ID__c) %>% 
  
  select(Reseller__c, 
         Reseller_Name, 
         Reseller_Type, 
         Partner_Level__c,
         Focus_Partner__c,
         Nationwide_Partner_for_Channel_Comms__c,
         Reseller_OwnerId,
         Reseller_Territory_ID__c)
Main05 <- merge(Main04, Reseller, by = "Reseller__c", all.x = TRUE)
```


#Add in User
```{r}
Main06 <- merge(Main05, User01, by.x = "Opportunity_OwnerId", by.y = "User_Id",all.x = TRUE)
```

#Add in Product
```{r}
Main07 <- merge(Main06, Product03, by.x = "SBQQ__Product__c",by.y = "Product2Id",all.x = TRUE)
```

#Merge subscription with product
```{r}
Subscription02 <- merge(Subscription01, Product03, by.x = "SBQQ__Product__c", by.y = "Product2Id", all.x = TRUE)
```

#Sales Category logic
```{r}
CSub13 <- Subscription02 %>% select(SBQQ__Account__c, Grouping, SubStartDate) %>% filter(is.na(Grouping) == F)
```
#New Logos
```{r}
CSub13$grouping <- paste(CSub13$SBQQ__Account__c, CSub13$Grouping)
CSub13$value <- 1
CSub13 <- CSub13 %>% arrange(SBQQ__Account__c, SubStartDate)
CSub13$EarliestFamily <- ave(CSub13$value, CSub13$grouping, FUN = cumsum)
CSub14 <- CSub13 %>% filter(EarliestFamily == 1) %>% select(grouping, SubStartDate) %>% rename(EarliestFamilyStartDate = SubStartDate)
```
#Cross Sell
```{r}
CSub15 <- Subscription02 %>% select(SBQQ__Account__c, IntraFamilyGrouping, SubStartDate) %>% filter(is.na(IntraFamilyGrouping) == F)
CSub15$grouping <- paste(CSub15$SBQQ__Account__c, CSub15$IntraFamilyGrouping)
CSub15$value <- 1
CSub15 <- CSub15 %>% arrange(SBQQ__Account__c, SubStartDate)
CSub15$EarliestFamily <- ave(CSub15$value, CSub15$grouping, FUN = cumsum)
CSub16 <- CSub15 %>% filter(EarliestFamily == 1) %>% select(grouping, SubStartDate) %>% rename(EarliestProductPairingStartDate = SubStartDate)
```
#Family Upsell
```{r}
CSub17 <- Subscription02 %>% select(SBQQ__Account__c, Product, SubStartDate) %>% filter(is.na(Product) == F)
CSub17$grouping <- paste(CSub17$SBQQ__Account__c, CSub17$Product)
CSub17$value <- 1
CSub17 <- CSub17 %>% arrange(grouping, SubStartDate)
CSub17$EarliestFamily <- ave(CSub17$value, CSub17$grouping, FUN = cumsum)
CSub18 <- CSub17 %>% filter(EarliestFamily == 1) %>% select(grouping, SubStartDate) %>% rename(EarliestProductStartDate = SubStartDate)
```
#Migrations
```{r}
CSub19 <- Subscription02 %>% select(SBQQ__Account__c, ProductPairing2, SubStartDate) %>% filter(is.na(ProductPairing2) == F)
CSub19$grouping <- paste(CSub19$SBQQ__Account__c, CSub19$ProductPairing2)
CSub19$value <- 1
CSub19 <- CSub19 %>% arrange(grouping, SubStartDate)
CSub19$EarliestFamily <- ave(CSub19$value, CSub19$grouping, FUN = cumsum)
CSub20 <- CSub19 %>% filter(EarliestFamily == 1) %>% select(grouping, SubStartDate) %>% rename(EarliestProductPairing2StartDate = SubStartDate)
```

```{r}
CSub21 <- Subscription02 %>% select(SBQQ__Account__c,SubStartDate) %>% filter(is.na(SBQQ__Account__c) == F)
CSub21$grouping <- paste(CSub21$SBQQ__Account__c)
CSub21$value <- 1
CSub21 <- CSub21 %>% arrange(SBQQ__Account__c, SubStartDate)
CSub21$EarliestAccount <- ave(CSub21$value, CSub21$grouping, FUN = cumsum)
CSub22 <- CSub21 %>% filter(EarliestAccount == 1) %>% select(grouping, SubStartDate) %>% rename(EarliestAccountStartDate = SubStartDate)
```

```{r}
Main07 <- Main07 %>% mutate(FamilyAccountGrouping = paste(AccountId, Grouping),
                            ProductPairingAccountGrouping = paste(AccountId, IntraFamilyGrouping),
                            ProductAccountGrouping = paste(AccountId, Product),
                            ProductPairing2AccountGrouping = paste(AccountId, ProductPairing2))
```
#Merge data for sales category
```{r}
Main08 <- merge(Main07, CSub14, by.x = "FamilyAccountGrouping", by.y = "grouping", all.x = TRUE)
Main09 <- merge(Main08, CSub16, by.x = "ProductPairingAccountGrouping", by.y = "grouping", all.x = TRUE)
Main10 <- merge(Main09, CSub18, by.x = "ProductAccountGrouping", by.y = "grouping", all.x = TRUE)
Main11 <- merge(Main10, CSub20, by.x = "ProductPairing2AccountGrouping", by.y = "grouping", all.x = TRUE)
Main12 <- merge(Main11,CSub22, by.x = "AccountId", by.y = "grouping",all.x = TRUE)
```

```{r}
#New Logo
Main12$Days_Ago <- as.Date(Main12$CloseDate) - as.Date(Main12$EarliestAccountStartDate)
Main12$New_Logo <- ifelse(is.na(Main12$Days_Ago), 1, ifelse(Main12$Days_Ago <= 30, 1, 0))
#Cross Sell
Main12$Family_Days_Ago <- as.Date(Main12$CloseDate) - as.Date(Main12$EarliestFamilyStartDate)
Main12$NewFamily <- ifelse(is.na(Main12$Family_Days_Ago) == F & (Main12$Family_Days_Ago >= 31 & Main12$Family_Days_Ago > 0), 0, 1)
#Family Upsell
Main12$ProductPairing_Days_Ago <- as.Date(Main12$CloseDate) - as.Date(Main12$EarliestProductPairingStartDate)
Main12$NewProductPairing <- ifelse(is.na(Main12$ProductPairing_Days_Ago) == F &  (Main12$ProductPairing_Days_Ago >= 31 & Main12$ProductPairing_Days_Ago > 0), 0, 1)
#Legacy Migration
Main12$ProductPairing2_Days_Ago <- as.Date(Main12$CloseDate) - as.Date(Main12$EarliestProductPairing2StartDate)
Main12$NewProductPairing2 <- ifelse(is.na(Main12$ProductPairing2_Days_Ago) == F &  Main12$ProductPairing2_Days_Ago >= 31, 0, 1)
#Migration
Main12$Product_Days_Ago <- as.Date(Main12$CloseDate) - as.Date(Main12$EarliestProductStartDate)
Main12$NewProduct <- ifelse(is.na(Main12$Product_Days_Ago) == F & (Main12$Product_Days_Ago >= 31 & Main12$Product_Days_Ago > 0), 0, 1)
```
#Define Sales Categories
```{r}
Main12 <- Main12 %>% mutate(Migration = ifelse(is.na(Migration__c) | Migration__c == FALSE, 0, 1),
                            Sales.Category = ifelse(New_Logo == 1, "New Logo",
                                                    ifelse(is.na(Grouping), "None",
                                                           ifelse(NewFamily == 1, "Cross Sell",
                                                                  ifelse(NewProductPairing == 1, "Family Upsell",
                                                                         ifelse(NewProductPairing2 == 1, "Migration",
                                                                                ifelse(NewProduct == 1 , "Legacy Migration",
                                                                                       ifelse(Migration == 1, "IntraProduct Upgrade", "Product Upsell"))))))))
```
Test11 <- Main12 %>% select()
#Add theater hierachy
```{r}
Main15 <- merge(Main12, Terr01, by.x ="Account_Territory_Id", by.y = "Id", all.x = TRUE)
```
```{r}
Main16 <- Main15 %>% rename(Conversion = SplitPriceConversion) %>%
  select(Account_Name,
         SBQQ__Group__c,
         Reseller_Name,
         Opportunity_Id,
         Opportunity_Name,
         Quote_Id,
         QuoteLine_Id,
         Amount,
         CreatedDate,
         CloseDate,
         Quote_CreatedDate,
         SBQQ__SubscriptionTerm__c,
         SBCF_Document_Term__c,
         Deal_Reg_Type__c,
         prod_subtype__c,
         QC,
         Extended_Price__c,
         Discount,
         Partner_Total,
         SBQQ__PartnerTotal__c,
         SBQQ__PartnerDiscount__c,
         SBQQ__NetTotal__c,
         Conversion,
         LeadSource,
         Migration__c,
         StageName,
         Closed_Lost_Reason__c,
         FiscalYear,
         Quarter,
         SBQQ__Primary__c,
         Base_SKU__c,
         ProductCode,
         Product,
         Grouping,
         Employees_Formula__c,
         Partner_Level__c,
         Nationwide_Partner_for_Channel_Comms__c,
         Rep_Name,
         Group_Name,
         Required_or_Optional,
         Sales.Category,
         Sub_Territory__c,
         Territory__c,
         Region__c,
         Sub_Theater__c,
         Theater__c)
```

#Opportunit amount ACV / TCV
```{r}
Main16 <- Main16 %>% mutate(Amount_TCV = Amount / Conversion,
  Amount_ACV = (Amount_TCV / ifelse(is.na(prod_subtype__c) | prod_subtype__c == "" | prod_subtype__c == "DC", 1,
                                                          ifelse(is.na(SBQQ__SubscriptionTerm__c), 1,
                                                                 ifelse(SBQQ__SubscriptionTerm__c < 12, 1, SBQQ__SubscriptionTerm__c / 12)))))
```
#Change quote line values to USD
```{r}
Main16 <- Main16 %>% mutate(NetTotalUSD = SBQQ__NetTotal__c / Conversion,
                            ACV = (NetTotalUSD / ifelse(is.na(prod_subtype__c) | prod_subtype__c == "" | prod_subtype__c == "DC", 1,
                                                        ifelse(is.na(SBQQ__SubscriptionTerm__c), 1,
                                                               ifelse(abs(SBQQ__SubscriptionTerm__c) < 12, 1, abs(SBQQ__SubscriptionTerm__c) / 12)))))
Main16 <- Main16 %>% mutate(TCV = ifelse(is.na(prod_subtype__c) | prod_subtype__c == "" | prod_subtype__c == "DC", ACV, NetTotalUSD))

```
#Adjust values where there is no quote
```{r}
Main16 <- Main16 %>% mutate(Grouping = ifelse(is.na(Grouping), "None", as.character(Grouping)),
                            Product = ifelse(is.na(Product), "None", as.character(Product)),
                            ACV = ifelse(is.na(ACV), ifelse(is.na(Amount_ACV), 0, Amount_ACV), ACV),
                            TCV = ifelse(is.na(TCV), ifelse(is.na(Amount_TCV), 0, Amount_TCV), TCV))
```

```{r}
Main17 <- Main16 %>% filter(!Product %in% c("Essentials Bundle SKU", "Essentials Account"))%>%
  select(-SBQQ__NetTotal__c) %>%
  rename(SBQQ__NetTotal__c = NetTotalUSD) %>%
  select(Account_Name,
         SBQQ__Group__c,
         Reseller_Name,
         Opportunity_Id,
         Opportunity_Name,
         Quote_Id,
         QuoteLine_Id,
         Amount,
         CreatedDate,
         CloseDate,
         Quote_CreatedDate,
         SBQQ__SubscriptionTerm__c,
         SBCF_Document_Term__c,
         Deal_Reg_Type__c,
         prod_subtype__c,
         ACV,
         TCV,
         Amount_ACV,
         Amount_TCV,
         Extended_Price__c,
         Discount,
         Partner_Total,
         SBQQ__PartnerDiscount__c,
         SBQQ__NetTotal__c,
         Conversion,
         QC,
         LeadSource,
         Migration__c,
         StageName,
         Closed_Lost_Reason__c,
         FiscalYear,
         Quarter,
         SBQQ__Primary__c,
         Base_SKU__c,
         ProductCode,
         Product,
         Grouping,
         Employees_Formula__c,
         Partner_Level__c,
         Nationwide_Partner_for_Channel_Comms__c,
         Rep_Name,
         Group_Name,
         Required_or_Optional,
         Sales.Category,
         Sub_Territory__c,
         Territory__c,
         Region__c,
         Sub_Theater__c,
         Theater__c)
```

```{r}
Main17 <- Main17 %>% arrange(Account_Name)
Main17 <- Main17 %>% mutate(New_Logo = ifelse(Sales.Category == "New Logo" & is.na(Grouping) == F & Grouping != "Other" & Grouping != "Professional Services",1,0),
                            Cross_Sell = ifelse(Sales.Category == "Cross Sell" & is.na(Grouping) == F & Grouping != "Other" & Grouping != "Professional Services",1,0),
                            Family_Upsell = ifelse(Sales.Category == "Family Upsell" & is.na(Grouping) == F & Grouping != "Other" & Grouping != "Professional Services",1,0),
                            Migration = ifelse(Sales.Category == "Migration" & is.na(Grouping) == F & Grouping != "Other" & Grouping != "Professional Services",1,0),
                            Legacy_Migration = ifelse(Sales.Category == "Legacy Migration" & is.na(Grouping) == F & Grouping != "Other" & Grouping != "Professional Services",1,0),
                            Product_Upsell = ifelse(Sales.Category == "Product Upsell" & is.na(Grouping) == F & Grouping != "Other" & Grouping != "Professional Services",1,0),
                            Product_Upgrade = ifelse(Sales.Category == "IntraProduct Upgrade" & is.na(Grouping) == F & Grouping != "Other" & Grouping != "Professional Services",1,0)) %>%
  group_by(Opportunity_Id, Required_or_Optional)
Main17 <- Main17 %>% mutate(Opp_New_Logo = sum(New_Logo),
                            Opp_Cross_Sell = sum(Cross_Sell),
                            Opp_Family_Upsell = sum(Family_Upsell),
                            Opp_Migration = sum(Migration),
                            Opp_Legacy_Migration = sum(Legacy_Migration),
                            Opp_Product_Upsell = sum(Product_Upsell),
                            Opp_Product_Upgrade = sum(Product_Upgrade))
```

```{r}
Validation <- Main17 %>% select(Account_Name,SBQQ__Group__c, Required_or_Optional,Opportunity_Id,CreatedDate,CloseDate,Quote_CreatedDate,Quote_Id,SBQQ__Primary__c,QuoteLine_Id,SBQQ__SubscriptionTerm__c,ACV,TCV,Amount_ACV,Amount_TCV,Amount,Extended_Price__c,Discount,Partner_Total,SBQQ__PartnerTotal__c,SBQQ__PartnerDiscount__c,SBQQ__NetTotal__c)
```

```{r}
Main17 <- Main17 %>% ungroup() %>%
  mutate(Opp.Category = ifelse(Opp_New_Logo > 0 , "New Logo",
                               ifelse(Opp_Product_Upgrade > 0 | Opp_Legacy_Migration > 0,
                                      ifelse(Opp_Migration > 0,
                                             ifelse(Opp_Cross_Sell > 0 | Opp_Family_Upsell > 0, "Rip and Replace, Migration and Upsell/Cross Sell",
                                                    "Rip and Replace, and Migration"),
                                             ifelse(Opp_Cross_Sell > 0 | Opp_Family_Upsell > 0, "Rip and Replace and Upsell/Cross Sell", "Rip and Replace")),
                                      
                                      ifelse(Opp_Migration > 0,
                                             ifelse(Opp_Cross_Sell > 0 | Opp_Family_Upsell > 0, "Migration and Upsell/Cross Sell",
                                                    "Migration"),
                                             ifelse(Opp_Cross_Sell > 0 | Opp_Family_Upsell > 0, "Upsell/Cross Sell",
                                                    ifelse(Opp_Product_Upsell > 0, "Product Upsell", "None"))))))
```
#Add opportunity level discounting logic
```{r}
Main17 <- Main17 %>% mutate(grouping = paste(Opportunity_Id, Required_or_Optional, Grouping),
                            SBQQ__SubscriptionTerm__c = ifelse(is.na(prod_subtype__c) == F & (prod_subtype__c %in% c("DC", "NULL", "null") | prod_subtype__c == ""), 12, SBQQ__SubscriptionTerm__c),
                            OpportunityACV = ave(ACV, grouping, FUN = sum),
                            OpportunityTCV = ave(TCV, grouping, FUN = sum),
                            OpportunityTerm = ifelse(is.na(OpportunityTCV) | OpportunityTCV == 0 | OpportunityACV == 0, ave(SBQQ__SubscriptionTerm__c, grouping, FUN = mean), OpportunityTCV / OpportunityACV * 12),
                            ExtendedACV = Extended_Price__c / ifelse(is.na(SBQQ__SubscriptionTerm__c), 12, SBQQ__SubscriptionTerm__c) * 12 / Conversion,
                            OpportunityListACV = ave(ExtendedACV, grouping, FUN = sum),
                            OpportunityExtended = ave(Extended_Price__c, grouping, FUN = sum) / Conversion,
                            OpportunityPartnerTotal = ave(Partner_Total, grouping, FUN = sum) / Conversion,
                            OpportunityDiscount = ifelse(OpportunityExtended == 0 & OpportunityPartnerTotal > 0,
                                                         100,
                                                         ifelse(OpportunityTCV == 0 | OpportunityPartnerTotal == 0,
                                                                0, 
                                                                round((1-(OpportunityPartnerTotal/OpportunityExtended))*100,2))))
Main17 <- Main17 %>%  mutate(MaxDiscount = 100 * (.1392 * log((OpportunityTerm / 12)) + ifelse(Opp.Category == "Renewal", .000003*OpportunityListACV, 
                                                                                               ifelse(Opp.Category == "New Logo", .0007*OpportunityListACV^.5767,
                                                                                                      ifelse(grepl("Migration", Opp.Category) | grepl("Rip", Opp.Category), .0194*OpportunityListACV^.028,
                                                                                                             ifelse(Opp.Category %in% c("Upsell/Cross Sell"), .0018*OpportunityListACV^.4703,
                                                                                                                    ifelse(Opp.Category == "Product Upsell", .0009*OpportunityListACV^.5145, 0)))))),
                             MaxDiscount = ifelse(MaxDiscount > 80, 80, MaxDiscount),
                             MidDiscount = MaxDiscount / 2,
                             DiscountEvaluation = ifelse(OpportunityDiscount <= MidDiscount, 1, ifelse(OpportunityDiscount <= MaxDiscount, .5, 0)))
```
```{r}
PipeDiscount11 <- Main17 %>% select(Account_Name, Opportunity_Id, CloseDate, OpportunityACV, OpportunityTCV, Product.Family, OpportunityListACV, OpportunityTerm,
                                    OpportunityDiscount, MaxDiscount, MidDiscount, DiscountEvaluation,
                                    Quote_Id, Required_or_Optional)
PipeDiscount11 <- PipeDiscount11 %>% mutate(grouping = paste(Opportunity_Id, Quote_Id),
                                            value = ifelse(Required_or_Optional == "Required", 1, 0),
                                            check = ave(value, grouping, FUN = sum)) %>%
  mutate(Required_or_Optional = ifelse(check >  0 | is.na(check), as.character(Required_or_Optional), "Required"))
PipeDiscount12 <- PipeDiscount11 %>% filter(Required_or_Optional == "Required" & !Product.Family %in% c("None", "Other", "Professional Services") & OpportunityListACV > 0) %>% distinct()
PipeDiscount12 <- PipeDiscount12 %>% mutate(ListPriceCategoryACV = ifelse(OpportunityListACV < 0, "0. Negative Order",
                                                                          ifelse(OpportunityListACV < 1, "00. Zero Dollar Order", 
                                                                                 ifelse(OpportunityListACV < 3000, "1. 1 - 3,000",
                                                                                        ifelse(OpportunityListACV < 10000, "2. 3,000 - 10,000",
                                                                                               ifelse(OpportunityListACV < 50000, "3. 10,000 - 50,000",
                                                                                                      ifelse(OpportunityListACV < 200000, "4. 50,000 - 200,000",
                                                                                                             "5. 200,000 +")))))))
#We need to change working drive to save directly to Git
setwd("C:/Users/jgrossman/Downloads")
write.csv(PipeDiscount12, "Pipeline Discounting.csv", row.names = FALSE)
```

```{r}
Main18 <- Main17 %>% select(-New_Logo,-Cross_Sell,-Family_Upsell,-Migration,-Legacy_Migration,-Product_Upsell,-Product_Upgrade) %>%
  select(-SBQQ__Group__c, -Quote_Id, -QuoteLine_Id, -Amount, -Quote_CreatedDate, -SBCF_Document_Term__c, -Amount_ACV, -Amount_TCV, -Conversion, -QC, -SBQQ__Primary__c,
         -Group_Name, -FiscalYear, -Quarter, -Opp_New_Logo, -Opp_Cross_Sell, -Opp_Family_Upsell, -Opp_Migration, -Opp_Legacy_Migration, -Opp_Product_Upsell, -Opp_Product_Upgrade, 
         -grouping, -OpportunityACV, -OpportunityTCV, -OpportunityTerm, -ExtendedACV, -OpportunityListACV, -OpportunityExtended, -OpportunityPartnerTotal, -OpportunityDiscount,
         -MaxDiscount, -MidDiscount, -DiscountEvaluation, -SBQQ__NetTotal__c, -SBQQ__PartnerDiscount__c, -Partner_Total, Extended_Price__c)
````
```{r}
Main18 <- Main18 %>% mutate(OpportunityLink = paste("https://barracuda2018.lightning.force.com/", Opportunity_Id, sep = ""),
                            StageName = ifelse(is.na(Closed_Lost_Reason__c) == F & is.na(StageName) == F & grepl("dupli", tolower(Closed_Lost_Reason__c)) & StageName == "Closed Lost", "Closed Lost Duplicate", as.character(StageName)),
                            Duration = CloseDate - CreatedDate,
                            DurationBucket = ifelse(Duration <= 30, "0-30", ifelse(Duration <= 60, "31-60", ifelse(Duration <= 90, "60-90", "90+"))))
Main18 <- Main18 %>% select(-Duration)
```

```{r}
setwd("C:/Users/jgrossman/Downloads")
write.csv(Main18, "Pipeline.csv", row.names = FALSE)
```


#dont run, keeping here in case it is needed
```{r}
May_Validation <- read_xlsx("AMER New Pipeline Validation.xlsx")
```
```{r}
names(May_Validation)
```
```{r}
OppFound <- as.data.frame(intersect(May_Validation$FullOppID,Main21$Opportunity_Id))
Opp_NotFound <- as.data.frame(setdiff(May_Validation$FullOppID,Main21$Opportunity_Id))
```
```{r}
Opp_NotFound <- Opp_NotFound %>% rename(Opp_Id = "setdiff(May_Validation$FullOppID, Main21$Opportunity_Id)")
OppFound <- OppFound %>% rename(Opp_Id = "intersect(May_Validation$FullOppID, Main21$Opportunity_Id)")
```
```{r}
write_xlsx(OppFound,"OppsFound.xlsx")
write_xlsx(Opp_NotFound,"Opps_NotFound.xlsx")
```
```{r}
Full_OppNotFound <- merge(Opp_NotFound,May_Validation, by.x = "Opp_Id",by.y = "FullOppID")
```
```{r}
Full_OppNotFound <- Full_OppNotFound %>% filter(is.na(Opp_Id == F))
```
```{r}
write_xlsx(Full_OppNotFound,"Info_Unmatched_Opps.xlsx")
```
```{r}
Full_OppFound <- merge(OppFound,May_Validation, by.x = "Opp_Id",by.y = "FullOppID")
```
```{r}
test1 <- merge(OppFound,Main21, by.x = "Opp_Id",by.y = "Opportunity_Id")
```
```{r}
test2 <- test1 %>% group_by(Opp_Id,Required_or_Optional) %>% distinct()
```
```{r}
write_xlsx(Full_OppFound,"Info_Matched_Opps.xlsx")
```


