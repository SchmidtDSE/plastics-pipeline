# title: "Global Plastic Treaty Model 01_A1_14"
# author: "Nivedita Biyani"
# date: "2023-06-26"
# 
# DESCRIPTION
# This code chunk is made using the excel sheet that Dr. Roland Geyer on June 20th 2023 titled 2023227 Global Treatment v12 Plus Additives & Textiles. 
# 
# 1. GOAL
# 
# We want to get the following: 
# 1. Production by region year for fiber 
# 2. Production by region and by year for resin 
# 3. Production by region and year for additives (B26) 

print("== 1 / 6: Set Up ==")

# PROCESS FLOW 
# 1. **Set Up**
# 2. Import
# 3. Tidy
# 4. Transform 
# 5. Visualize
# 6. Export

library(RcppRoll)
library(corrplot)
library(psych)
library(reshape)
library(dplyr)
library(forcats)
library(ggplot2)
library(tidyverse)
library(viridis)
library(scales) # to access break formatting functions
library(ggalt)
library(plotly)
library(tibble)
library(psych)
library(gdata)
library(data.table)
library(heplots)
library(candisc)
library(car)
library(alluvial)
library(ggalluvial)
library(zoo)
library(magrittr)
library(forecast)
library(imputeTS)
library(zoo)
library(fpp2)
library(fpp)
library(readxl)
library(reprex)
library(purrr)
library(glue)

libs <- c('dplyr', 'stringr', 'forcats',     # wrangling
          'knitr',               # table styling
          'ggplot2','alluvial','ggalluvial')                     # data
invisible(lapply(libs, library, character.only = TRUE))

root_dir <- dirname(getwd())
data_dir <- file.path(data_dir, "data")

# 2. GOAL
# In this section we are importing all the data sets used in the modeling exercise. All data feeds are in one place in the model. 
#
# PROCESS FLOW 
# 1. Set Up
# 2. **Import**
# 3. Tidy
# 4. Transform 
# 5. Visualize
# 6. Export

print("== 2 / 6: Import ==")

# Base Inputs of Production
options(digits = 8)
A_01_Resin<-read.csv(glue("{data_dir}/01_Production_of_Resin_(no_fiber).csv"))
A_02_Fiber<-read.csv(glue("{data_dir}/02_Production_Fiber.csv"))
A_03_Add<-read.csv(glue("{data_dir}/03_Production_Additives.csv"))

# Base Inputs of Imports 
A_04_Net_Import_Resin<-read.csv(glue("{data_dir}/04_Net_Import_Resin_no_fiber_copy.csv"))
A_05_Net_Import_Fiber<-read.csv(glue("{data_dir}/05_Net_Import_Fiber_copy.csv"))
A_06_Net_Import_Plastic_Articles<-read.csv(glue("{data_dir}/06_Net_import_plastic_articles copy.csv"))
A_07_Net_Import_Finished_Goods<-read.csv(glue("{data_dir}/07_Net_Import_plastic_in_finished_goods_no_fiber copy.csv"))

# Trade New versions 12 
AB_01_Trade_China<-read.csv(glue("{data_dir}/18_Net_Trade_China.csv"))
AB_02_Trade_NAFTA<-read.csv(glue("{data_dir}/19_Net_Trade_NAFTA.csv"))
AB_03_Trade_EU<-read.csv(glue("{data_dir}/20_Net_Trade_EU.csv"))
AB_04_Trade_Row<-read.csv(glue("{data_dir}/21_Net_Trade_Row.csv"))

# Polymer breakdown by Sector and Region (Sector Polymer Matrices)
# These ones need to be fed in with 5 decimal places to get same values as excel sheet 
AC_01_China<-read.csv(glue("{data_dir}/08_China_End_Use_and_Type.csv"))
AC_02_NAFTA<-read.csv(glue("{data_dir}/09_NAFTA_End_Use_and_Type.csv"))
AC_03_Europe<-read.csv(glue("{data_dir}/10_Europe_End_Use_and_Type.csv"))
AC_04_Row<-read.csv(glue("{data_dir}/11_RoW_End_Use_and_Type.csv"))

# Lifetime distributions 
AI_02<-read.csv(glue("{data_dir}/12_Lifetime copy.csv"))

# 1950 ~ 2004 History Curve Fitting
AHH1<-read.csv(glue("{data_dir}/13_1950-2004 copy.csv"))

# End of Life Breakdowns 
AKE_Eol_China<-read.csv(glue("{data_dir}/14_Eol_China copy.csv"))
AKE_Eol_EU<-read.csv(glue("{data_dir}/15_EoL_EU copy.csv"))
AKE_Eol_NAFTA<-read.csv(glue("{data_dir}/16_Eol_NAFTA copy.csv"))
AKE_Eol_Row<-read.csv(glue("{data_dir}/17_Eol_RoW copy.csv"))

# 3. GOAL 
# In this section we are making all the data long form, even data that we use later in the model. 
#
# PROCESS FLOW 
# 1. Set Up
# 2. Import
# 3. **Tidy**
# 4. Transform 
# 5. Visualize
# 6. Export
# 
# EXTRAS: 
# - I tried using lapply to repeat the same function over multiple data frames, but it did not work. Would be good to make this code shorter.

print("== 3a / 6: Tidy (Long Form) ==")

B_01_Resin<-A_01_Resin %>% pivot_longer(cols=c('China',"NAFTA","EU30","RoW"),
                                          names_to='Region',
                                          values_to='MT_Plastic_Prod_Resin')

B_02_Fiber<-A_02_Fiber %>% pivot_longer(cols=c('China',"NAFTA","EU30","RoW"),
                    names_to='Region',
                    values_to='MT_Plastic_Prod_Fiber')

B_03_Additives<-A_03_Add %>% pivot_longer(cols=c('China',"NAFTA","EU30","RoW"),
                                              names_to='Region',
                                              values_to='MT_Add')

B_04_Net_Import_Resin<-A_04_Net_Import_Resin %>% pivot_longer(cols=c('China',"NAFTA","EU30","RoW"),
                                                  names_to='Region',
                                                  values_to='MT_Plastic_Import_Resin_no_Fiber')

B_05_Net_Import_Fiber<-A_05_Net_Import_Fiber %>% pivot_longer(cols= c('China',"NAFTA","EU30", "RoW"),
                                                              names_to="Region",
                                                              values_to="MT_Import_Fiber")

B_06_Net_Import_Plastic_Article<-A_06_Net_Import_Plastic_Articles %>%pivot_longer(cols=c('China',"NAFTA","EU30","RoW"),
                                                          names_to='Region',
                                                          values_to='MT_Plastic_Import_Plast_Artic')

  
B_07_Net_Import_Finished_Goods<-A_07_Net_Import_Finished_Goods %>% pivot_longer(cols=c('China',"NAFTA","EU30","RoW"),
                                                         names_to='Region',
                                                         values_to='MT_Plastic_Import_Plast_Finished_Goods')

#############################################################
#############################################################
#  Making All Long form Data into 1 Mother Data Set        ##
#############################################################
#############################################################

# Use Merge When Column are the Same 

B1<-B_02_Fiber %>% inner_join(B_01_Resin, 
                                       by=c('Year'="Year","Region"="Region"))

B2<-B1 %>% inner_join(B_04_Net_Import_Resin,
                      by=c("Year"="Year",'Region'="Region"))

B3<-B2 %>% inner_join(B_06_Net_Import_Plastic_Article,
                       by=c("Year"="Year", "Region"="Region"))
  
B4<-B3 %>% inner_join(B_07_Net_Import_Finished_Goods,
                      by=c("Year"="Year","Region"="Region"))

B5<-B4 %>% inner_join(B_03_Additives,
                      by=c("Year"="Year", "Region"="Region"))

B5<-B5 %>% inner_join(B_05_Net_Import_Fiber,
                      by=c("Year"="Year", "Region"="Region"))


#############################################################
#############################################################
#                         Mother Data Set                  ##
#############################################################
#############################################################

# B5 Is Mother Data set for Production of Plastic

#############################################################
#############################################################
#               Making it long form                        ##
# i = Time (16) = Years 
# j = Country (4) = China, NAFTA, EU30, or RoW
# k = Sector (7) = Transportation or Packaging or Building and Construction and Electrical and Electronic or Household_Leisure_Sports 
# or Agriculture or Other 
# l = Type (9)= LLDPE_LDPE or HDPE or PP or PS or PVC or PET or PUR or "Other Thermoplastics" or "Other Thermosets"
#                                                          ##
#############################################################
#############################################################

##### CHINA #####

C_01_China_Long<-AC_01_China %>% pivot_longer(cols=c('LLDPE_LDPE',"HDPE","PP","PS","PVC","PET","PUR","Other.Thermoplastics","Other.Thermosets","Sum"),
                                             names_to='Type',
                                             values_to='Percentage')
C_01_China_Long<-C_01_China_Long %>% 
  add_column(Region = "China")

C_01_China_Long<-C_01_China_Long[c("Region","Major_Market_Sector","Type","Percentage")]


##### NAFTA #####
C_02_NAFTA_Long<-AC_02_NAFTA %>% pivot_longer(cols=c('LLDPE_LDPE',"HDPE","PP","PS","PVC","PET","PUR","Other.Thermoplastics","Other.Thermosets","Sum"),
                                             names_to='Type',
                                             values_to='Percentage')
C_02_NAFTA_Long<-C_02_NAFTA_Long %>% 
  add_column(Region = "NAFTA")

C_02_NAFTA_Long<-C_02_NAFTA_Long[c("Region","Major_Market_Sector","Type","Percentage")]


##### EU30 #####

C_03_EU30_Long<-AC_03_Europe %>% pivot_longer(cols=c('LLDPE_LDPE',"HDPE","PP","PS","PVC","PET","PUR","Other.Thermoplastics","Other.Thermosets","Sum"),
                                             names_to='Type',
                                             values_to='Percentage')
C_03_EU30_Long<-C_03_EU30_Long %>% 
  add_column(Region = "EU30")

C_03_EU30_Long<-C_03_EU30_Long[c("Region","Major_Market_Sector","Type","Percentage")]

##### RoW #####

C_04_RoW_Long<-AC_04_Row %>% pivot_longer(cols=c('LLDPE_LDPE',"HDPE","PP","PS","PVC","PET","PUR","Other.Thermoplastics","Other.Thermosets","Sum"),
                                         names_to='Type',
                                         values_to='Percentage')
C_04_RoW_Long<-C_04_RoW_Long %>% 
  add_column(Region = "RoW")

C_04_RoW_Long<-C_04_RoW_Long[c("Region","Major_Market_Sector","Type","Percentage")]

# 4. GOAL 
# Combining columns for percentage of each type of plastic. This data frame is actually not used for a while. 
#
# PROCESS FLOW 
# 1. Set Up
# 2. Import
# 3. **Tidy**
# 4. Transform 
# 5. Visualize
# 6. Export

print("== 3b / 6: Tidy (Combine) ==")

D1<-rbind(C_01_China_Long,C_02_NAFTA_Long,C_03_EU30_Long,C_04_RoW_Long)

# 5. GOAL 
# Calculating Apparent Consumption 1: Calculations to get it by Year, Region and Sector (Accounting for Trade)
# This data set has now been updated (on June 19th 2023) by Roland, and the "new model" that will be the iteration after this, will have a different set of data sets and calculations here. Here, I am referring to line 22 of the EoL tab of the spreadsheet. 
# 
# PROCESS FLOW 
# 1. Set Up
# 2. Import
# 3. Tidy
# 4. **Transform**
# 5. Visualize
# 6. Export

print("== 4a / 6: Transform (Consumption 1) ==")

#############################################################
#############################################################
#  Combining E1 and G1                  ##
#############################################################
#############################################################

# Somehow the "sum" of all plastics types was not adding up to what was in the spreadsheet.
# So I imported the spreadsheet sum value 
#G2<-G1 %>% select(Region, Major_Market_Sector, Percentage) %>% 
  #group_by(Region, Major_Market_Sector) %>% 
  #summarise(Sum_Percentage=sum(Percentage))

E2.1<-D1 %>% filter(Type=="Sum")

#############################################################
#############################################################
#    End of Section                                        ##
#############################################################
#############################################################

#I did not do this step. 
#C5+G3 

G3<-B5 %>% inner_join(E2.1,
                      by=c("Region"="Region"))

#I got the 153.4 need for this - now * by China Transport sum. 
G3<-B5 %>% mutate(Converter_Consumption_1=
                    MT_Plastic_Prod_Resin+MT_Plastic_Import_Resin_no_Fiber+MT_Add)

G3<-G3 %>% select(Year, Region, Converter_Consumption_1)

G4<-G3 %>% inner_join(E2.1,
                      by=c("Region"="Region"))

G5<-G4 %>% mutate(Converter_Consump_2=Converter_Consumption_1*Percentage)
# Incorporate here the last thing. 

G6<-G5 %>% select(Year, Region, Major_Market_Sector, Converter_Consump_2)

# The one thing I don't have here is fiber. So lets get that in here.
G6.1 <- expand.grid(
  Year = c(2005:2020),
  Region = c("China","NAFTA","EU30","RoW"),
  Major_Market_Sector = c("Textiles")
)
G6.2<-B_02_Fiber %>% inner_join(G6.1,
                                by=c("Year"="Year","Region"="Region"))

colnames(G6.2)[3]<-"Converter_Consump_2"
G6.2<-G6.2 %>% select(Year, Region, Major_Market_Sector, Converter_Consump_2)

# Textiles is now part of the data frame! 
G7<-rbind(G6, G6.2)


# Making Major_Market_Sector Factor variable 
G7$Major_Market_Sector<- factor(G7$Major_Market_Sector, levels = 
                                  c("Transportation", 
                                    "Packaging", 
                                    "Building_Construction", 
                                    "Electrical_Electronic", 
                                    "Household_Leisure_Sports", 
                                    "Agriculture",
                                    "Textiles",
                                    "Others"))
# correct factor leveling 
levels(G7$Major_Market_Sector)

# G7 is the mother data set now 
# Up until here we have all the variables and all the converter consumption, 
# as portraryed in v12 (version 12) of the 20230227 of Global EoL treatment excel
# sheet. 
# 
# For the new version I am adding Trade in the chunk below. Making it long form and then 
# subtracting it from G7. 

####
# CHINA TRADE 

F01_China<-AB_01_Trade_China %>% pivot_longer(cols=c("Transportation","Packaging","Building_Construction","Electrical_Electronic","Household_Leisure_Sports","Agriculture",
                                               "Textiles","Others"),
                                                 names_to='Major_Market_Sector',
                                                 values_to='MT_Trade')
F01.1 <- expand.grid(
  Year = c(2005:2020),
  Region = c("China"))

F01.2<-F01_China %>% inner_join(F01.1,
                                by=c("Year"="Year"))

####
# NAFTA TRADE 

F02_NAFTA<-AB_02_Trade_NAFTA %>% pivot_longer(cols=c("Transportation","Packaging","Building_Construction","Electrical_Electronic","Household_Leisure_Sports","Agriculture",
                                               "Textiles","Others"),
                                                 names_to='Major_Market_Sector',
                                                 values_to='MT_Trade')
F02.1 <- expand.grid(
  Year = c(2005:2020),
  Region = c("NAFTA"))

F02.2<-F02_NAFTA %>% inner_join(F02.1,
                                by=c("Year"="Year"))

####
# EU TRADE 

F03_EU<-AB_03_Trade_EU %>% pivot_longer(cols=c("Transportation","Packaging","Building_Construction","Electrical_Electronic","Household_Leisure_Sports","Agriculture",
                                               "Textiles","Others"),
                                                 names_to='Major_Market_Sector',
                                                 values_to='MT_Trade')
F03.1 <- expand.grid(
  Year = c(2005:2020),
  Region = c("EU30"))

F03.2<-F03_EU %>% inner_join(F03.1,
                                by=c("Year"="Year"))

####
# RoW TRADE

F04_Row<-AB_04_Trade_Row %>% pivot_longer(cols=c("Transportation","Packaging","Building_Construction","Electrical_Electronic","Household_Leisure_Sports","Agriculture",
                                               "Textiles","Others"),
                                                 names_to='Major_Market_Sector',
                                                 values_to='MT_Trade')
F04.1 <- expand.grid(
  Year = c(2005:2020),
  Region = c("RoW"))

F04.2<-F04_Row %>% inner_join(F04.1,
                                by=c("Year"="Year"))

F05<-rbind(F01.2,F02.2,F03.2, F04.2)

# Joing all trade information 
F05<-F05 %>% select(Year, Region, Major_Market_Sector, MT_Trade)

G7$Region<-as.factor(G7$Region)
F05$Major_Market_Sector<- factor(F05$Major_Market_Sector, levels = 
                                  c("Transportation", 
                                    "Packaging", 
                                    "Building_Construction", 
                                    "Electrical_Electronic", 
                                    "Household_Leisure_Sports", 
                                    "Agriculture",
                                    "Textiles",
                                    "Others"))

# Joining G7 with G8
G8<-G7 %>% inner_join(F05,
        by=c("Year"="Year", "Region"="Region","Major_Market_Sector"="Major_Market_Sector"))


# We are now at Line 25 B of Eol Spreadsheet V12 
# Apparent consumption from year 2005 to 2020 

G9<-G8 %>% mutate(PC_Sector=Converter_Consump_2+MT_Trade)%>%
  mutate_at(5:6, round,3)

## 6. GOAL 
# Calculating Apparent Consumption 2: Linear Regression of 2004 value. 
#
# PROCESS FLOW 
# 1. Set Up
# 2. Import
# 3. Tidy
# 4. **Transform**
# 5. Visualize
# 6. Export

print("== 4b / 6: Transform (Consumption 2) ==")


# Make a long form data set 
#############################################################
#############################################################
#    End of Section - Eol Line 42                          ##
# In this section we predict what year 2004 might have been given the past data ## 
#############################################################
#############################################################

#levels(G6.2$Region)

# Make a long form data set 
H0 <- expand.grid(
  Year = c(2004),
  Region = c("China","NAFTA","EU30","RoW"),
  Major_Market_Sector = c("Transportation","Packaging","Building_Construction","Electrical_Electronic",
                          "Household_Leisure_Sports","Textile","Agriculture","Others")
)


# Just making sure I leave C6.2 intact 
H.1<-G9 %>% select(Year, Region, Major_Market_Sector, PC_Sector)
H.1$Major_Market_Sector<- factor(H.1$Major_Market_Sector, levels = 
                                  c("Transportation", 
                                    "Packaging", 
                                    "Building_Construction", 
                                    "Electrical_Electronic", 
                                    "Household_Leisure_Sports", 
                                    "Agriculture",
                                    "Textiles",
                                    "Others"))
levels(H.1$Region)
H.1$Region<-as.factor(H.1$Region)

# This is the linear regression for predicting year 2004 
H2<-H.1 %>%
  group_by(Region, Major_Market_Sector) %>%         #.groups = "drop"
  summarize(PC_Sector = predict(lm(PC_Sector ~ Year), list(Year=2004))) %>%
  ungroup %>%
  select(Region, Major_Market_Sector, PC_Sector)%>% 
  mutate_at(3, round, 2)

# Stitch it together with HO so that I have a data frame 
H3<-H2%>% inner_join(H0, 
              by=c("Region"="Region", "Major_Market_Sector"="Major_Market_Sector"))

H4<-H3 %>% 
  select(Year,Region,Major_Market_Sector,PC_Sector)

H5<-rbind(H4,H.1)

# H5 is now the master data set 

## 7. GOAL 
# Now we are doing imputations for China and NAFTA to be able to apply back wards on to H5 to get what the waste was since 1950. In the spread sheet this section is 1950~2004. Question for Roland has bee answered as to additives - they have already been included above. 
# 
# PROCESS FLOW 
# 1. Set Up
# 2. Import
# 3. Tidy
# 4. **Transform**
# 5. Visualize
# 6. Export

print("== 4c / 6: Transform (Imputation) ==")

# 1950~2004 in Eol Spreadsheet 
#HH1<-read.csv("01_Data_Raw/14_1950-2004.csv") already fed in above. 
options(digits = 2)
China_2<-na_interpolation(AHH1$China)
NAFTA_lbs<-na_interpolation(AHH1$NAFTA_lbs)

China_2<-as.data.frame(China_2)
HH5<-cbind.data.frame(China_2,NAFTA_lbs)

HH5<-HH5 %>% 
  mutate(NAFTA=NAFTA_lbs/2204.62)

HH6<-cbind(HH5,AHH1) %>% 
  select(Year, China_2, NAFTA, EU30, RoW)

HH6$China_2<- round(HH6$China_2, 2)

# 1950~2004 in Eol Spreadsheet 
colnames(HH6)[colnames(HH6) == 'China_2'] <- 'China'

# making it long form
HH7<-HH6 %>% pivot_longer(cols=c("China","NAFTA","EU30","RoW"),
                                             names_to='Region',values_to='Total_MT_Plastic')

# Make a long form data set 
H0.1<- expand.grid(
  Year = c(1950:2004),
  Region = c("China","NAFTA","EU30","RoW"),
  Major_Market_Sector = c("Transportation","Packaging","Building_Construction","Electrical_Electronic",
                          "Household_Leisure_Sports","Textiles","Agriculture","Others")
)

HH8<-H0.1%>% right_join(HH7, 
                     by=c("Region"="Region", "Year"="Year"))
HH8<-as.data.frame(HH8)
# Making sure H5 goes untouched!
H6<-H5
HH8$Region<-as.factor(HH8$Region)

HH9.2<-H4 %>% full_join(HH8,
                       by=c("Year"="Year","Region"="Region","Major_Market_Sector"="Major_Market_Sector"))

#colnames(HH9.2)[colnames(HH9.2) == 'PC'] <- 'PC_Sector'

#write.csv(HH9.2, "/Users/dr.nivedita/Library/CloudStorage/OneDrive-Personal/04_Post-Doc/01.R_Code/06_Output\\HH9.2.csv")


# 8. GOAL 
# Getting historic values for all regions and all sectors and all years.
# 
# PROCESS FLOW 
# 1. Set Up
# 2. Import
# 3. Tidy
# 4. **Transform**
# 5. Visualize
# 6. Export

print("== 4d / 6: Transform (Historic) ==")

# Removing textiles 

HH9.3<-HH9.2 %>% filter(Major_Market_Sector!="Textiles")
H7<-H6%>% filter(Major_Market_Sector!="Textiles")

# With Carl's new Code! 

IA1<-HH9.3 %>% 
  arrange(desc(Major_Market_Sector), desc(Region), desc(Year)) %>%
  group_by(Region, Major_Market_Sector) %>%
  mutate(PC_Sector = Total_MT_Plastic * PC_Sector[[1]] / Total_MT_Plastic[[1]] )%>%
  ungroup()

################################################################################
################################################################################
#
#.    Combine 2020 + 2004  
#
################################################################################
################################################################################

IA2<-IA1 %>% select(-Total_MT_Plastic)
#colnames(IA2)[colnames(IA2) == 'PC_Sector'] <- 'PC'

IA3<-rbind(IA2,H7)

################################################################################
################################################################################
 # IA3 is now the most important data frame KD_6_2050
################################################################################
################################################################################

# Textiles was tricky. This will be separately added from V12. 

# this is textiles which will be separately added 
IA3.1<-HH9.2 %>% filter(Major_Market_Sector=="Textiles")
IA3.2<-H6%>% filter(Major_Market_Sector=="Textiles")

# 9. GOAL 
# Life time distribution section starts here. Since we do not have direct waste generation data, we use the LTD for each sector to know when plastic become waste. For Packageing the LTD is short - just 3 years. For Transportation the LTD is long - meaning that plastic used in the construction sector stays in the use phase for an average or 11 years. This is essentially calculating when something becomes waste.  
#
# PROCESS FLOW 
# 1. Set Up
# 2. Import
# 3. Tidy
# 4. **Transform**
# 5. Visualize
# 6. Export

print("== 4e / 6: Transform (Waste) ==")

##   Make the Lifetime Distributions set up:  

options(digits = 9)

# Percentage that you might need later 
#I_01<-read.csv("01_Data_Raw/11_Sectors.csv") Already fed in above.

# Feeding in Lifetime Distribution 
#I_02<-read.csv("01_Data_Raw/12_Lifetime.csv")Already fed in above. 

# Make lifetime variables average and varience 
I_03<-AI_02 %>% mutate(mu=log(Average/sqrt(Variance/Average^2+1))) %>% 
  mutate(sigma=sqrt(log(Variance/Average^2+1)))

I_04_Year<- c(0.1, seq(1, 71, by=1))


#############################################################
#############################################################
##              LTD Transportation                         ##
#############################################################
#############################################################

options(digits = 2)
# Calculate the PDF using plnorm()
I_05_TransportationPDF<-plnorm(I_04_Year, meanlog = I_03$mu[1], 
                               sdlog = I_03$sigma[1])

J_A_PDF_Transport<-data.frame(I_04_Year,I_05_TransportationPDF) %>%
  mutate(Transportation= I_05_TransportationPDF - lag(I_05_TransportationPDF, 
                              default = first(I_05_TransportationPDF)))

G1Trans<-J_A_PDF_Transport[[3]][2:72]

#############################################################
#############################################################
#               LTD Packaging                              ##
#############################################################
#############################################################

options(digits = 2)
# Calculate the PDF using plnorm()
I_06_PackagingPDF<-plnorm(I_04_Year, meanlog = I_03$mu[2], 
                               sdlog = I_03$sigma[2])

J_B_PDF_Packaging<-data.frame(I_04_Year,I_06_PackagingPDF) %>%
  mutate(Packaging= I_06_PackagingPDF - lag(I_06_PackagingPDF, 
                                     default = first(I_06_PackagingPDF)))

G2Packaging<-J_B_PDF_Packaging[[3]][2:72]

#############################################################
#############################################################
#               Building and Construction                  ##
#############################################################
#############################################################

options(digits = 2)
# Calculate the PDF using plnorm()
I_07_BuildingPDF<-plnorm(I_04_Year, meanlog = I_03$mu[3], 
                          sdlog = I_03$sigma[3])

J_C_PDF_BuildingCons<-data.frame(I_04_Year,I_07_BuildingPDF) %>%
  mutate(Building_Construction= I_07_BuildingPDF - lag(I_07_BuildingPDF, 
                                  default = first(I_07_BuildingPDF)))

G3Building<-J_C_PDF_BuildingCons[[3]][2:72]

#############################################################
#############################################################
#               Electrical_Electronic                      ##
#############################################################
#############################################################

options(digits = 2)
# Calculate the PDF using plnorm()
I_08_ElectricPDF<-plnorm(I_04_Year, meanlog = I_03$mu[4], 
                         sdlog = I_03$sigma[4])

J_D_PDF_Electric<-data.frame(I_04_Year,I_08_ElectricPDF) %>%
  mutate(Electrical_Electronic= I_08_ElectricPDF - lag(I_08_ElectricPDF, 
                                    default = first(I_08_ElectricPDF)))

G4Electric<-J_D_PDF_Electric[2:72,3]

#############################################################
#############################################################
#               Household_Leisure                          ##
#############################################################
#############################################################

options(digits = 2)
# Calculate the PDF using plnorm()
I_09_HouseholdLeis<-plnorm(I_04_Year, meanlog = I_03$mu[5], 
                         sdlog = I_03$sigma[5])

J_E_PDF_Household<-data.frame(I_04_Year,I_09_HouseholdLeis) %>%
  mutate(Household_Leisure_Sports= I_09_HouseholdLeis - lag(I_09_HouseholdLeis, 
                                default = first(I_09_HouseholdLeis)))

G5House<-J_E_PDF_Household[2:72,3]

#############################################################
#############################################################
#               Agriculture.                               ##
#############################################################
#############################################################

options(digits = 2)
# Calculate the PDF using plnorm()
I_10_Agricul<-plnorm(I_04_Year, meanlog = I_03$mu[6], 
                           sdlog = I_03$sigma[6])

J_F_PDF_Agri<-data.frame(I_04_Year,I_10_Agricul) %>%
  mutate(Agriculture = I_10_Agricul - lag(I_10_Agricul, 
                                  default = first(I_10_Agricul)))

G6Agri<-J_F_PDF_Agri[[3]][2:72]

#############################################################
#############################################################
#               Others.                                    ##
#############################################################
#############################################################

options(digits = 2)
# Calculate the PDF using plnorm()
I_11_Othersl<-plnorm(I_04_Year, meanlog = I_03$mu[7], 
                     sdlog = I_03$sigma[7])

J_G_PDF_Other<-data.frame(I_04_Year,I_11_Othersl) %>%
  mutate(Others = I_11_Othersl - lag(I_11_Othersl, 
                                             default = first(I_11_Othersl)))

G7Others<-J_G_PDF_Other[[3]][2:72]

#############################################################
#############################################################
#               Textiles                                   ##
#############################################################
#############################################################

options(digits = 2)
# Calculate the PDF using plnorm()
I_12_Textiles<-plnorm(I_04_Year, meanlog = I_03$mu[8], 
                     sdlog = I_03$sigma[8])

J_H_Texti<-data.frame(I_04_Year,I_12_Textiles) %>%
  mutate(PDF_Textiles = I_12_Textiles - lag(I_12_Textiles, 
                                             default = first(I_12_Textiles)))

G8Textiles<-J_H_Texti[[3]][2:72]

# 10. GOAL 
# Applying LTD to apparent consumption to get waste generation, by year, by sector, by region. 
#
# PROCESS FLOW 
# 1. Set Up
# 2. Import
# 3. Tidy
# 4. **Transform**
# 5. Visualize
# 6. Export

print("== 4f / 6: Transform (Dimensions) ==")

################################################################################
################################################################################
#
#    Preparing to Calculate Waste Generation 
#
################################################################################
################################################################################
# you need to add textile from 2004 - 2020 
# for forgot to import it, so for now just do it without textiles but add it in later 

KA1<-IA3 %>% filter(Major_Market_Sector!="Textile")

################################################################################
################################################################################
### After Meeting with Roland and Implementing Carl's Solution !! 
################################################################################
################################################################################

# Adding a data frame that extends the years out to KA1 
KA2 <- expand.grid(
  Year = c(1949:1900),
  Region = c("China","NAFTA","EU30","RoW"),
  Major_Market_Sector = c("Transportation","Packaging","Building_Construction","Electrical_Electronic",
                          "Household_Leisure_Sports","Textile","Agriculture","Others"),
  PC_Sector=c(0)
)
KA2

KA3<-bind_rows(KA1,KA2)
################################################################################
################################################################################
#
#    Calculating Waste Generation 
#
################################################################################
################################################################################

KB_1Transport<-KA3%>% 
 arrange(desc(Year)) %>% arrange((Region)) %>% arrange(desc(Major_Market_Sector)) %>% 
 group_by(Region) %>%
 filter(Major_Market_Sector=="Transportation") %>% 
 mutate(WasteGen= RcppRoll::roll_sum(PC_Sector, n=50, weights = G1Trans, align="left", 
                              na.rm = TRUE, normalize = FALSE, fill=NA))

KB_2Packaging<-KA3%>% 
  arrange(desc(Year)) %>% arrange((Region)) %>% arrange(desc(Major_Market_Sector)) %>% 
  group_by(Region) %>%
  filter(Major_Market_Sector=="Packaging") %>% 
  mutate(WasteGen= RcppRoll::roll_sum(PC_Sector, n=50, weights = G2Packaging, align="left", 
                                            na.rm = TRUE, normalize = FALSE, fill=NA))

KB_3Building<-KA3%>% 
  arrange(desc(Year)) %>% arrange((Region)) %>% arrange(desc(Major_Market_Sector)) %>% 
  group_by(Region) %>%
  filter(Major_Market_Sector=="Building_Construction") %>% 
  mutate(WasteGen= RcppRoll::roll_sum(PC_Sector, n=50, weights = G3Building, align="left", 
                                            na.rm = TRUE, normalize = FALSE, fill=NA))

KB_4Electric<-KA3%>% 
  arrange(desc(Year)) %>% arrange((Region)) %>% arrange(desc(Major_Market_Sector)) %>% 
  group_by(Region) %>%
  filter(Major_Market_Sector=="Electrical_Electronic") %>% 
  mutate(WasteGen= RcppRoll::roll_sum(PC_Sector, n=50, weights = G4Electric, align="left", 
                                            na.rm = TRUE, normalize = FALSE, fill=NA))

KB_5House<-KA3%>% 
  arrange(desc(Year)) %>% arrange((Region)) %>% arrange(desc(Major_Market_Sector)) %>% 
  group_by(Region) %>%
  filter(Major_Market_Sector=="Household_Leisure_Sports") %>% 
  mutate(WasteGen= RcppRoll::roll_sum(PC_Sector, n=50, weights = G5House, align="left", 
                                            na.rm = TRUE, normalize = FALSE, fill=NA))

KB_6Agri<-KA3%>% 
  arrange(desc(Year)) %>% arrange((Region)) %>% arrange(desc(Major_Market_Sector)) %>% 
  group_by(Region) %>%
  filter(Major_Market_Sector=="Agriculture") %>% 
  mutate(WasteGen= RcppRoll::roll_sum(PC_Sector, n=50, weights = G6Agri, align="left", 
                                            na.rm = TRUE, normalize = FALSE, fill=NA))

KB_7Other<-KA3%>% 
  arrange(desc(Year)) %>% arrange((Region)) %>% arrange(desc(Major_Market_Sector)) %>% 
  group_by(Region) %>%
  filter(Major_Market_Sector=="Others") %>% 
  mutate(WasteGen= RcppRoll::roll_sum(PC_Sector, n=50, weights = G7Others, align="left", 
                                            na.rm = TRUE, normalize = FALSE, fill=NA))

# 11. GOAL 
# Preparing Waste Generation data-frame for End-of-Life (EOL). 
# Ignore the warnings here. 
#
# PROCESS FLOW 
# 1. Set Up
# 2. Import
# 3. Tidy
# 4. **Transform**
# 5. Visualize
# 6. Export

print("== 4g / 6: Transform (EOL) ==")

################################################################################
################################################################################
#
#    Waste Gen to EOL 
#
################################################################################
################################################################################


KC_1Transport<-KB_1Transport %>%
  group_by(Region) %>% 
  mutate(Year_2=c(2021:1900)) %>% 
  select(Year_2, Region, Major_Market_Sector,WasteGen) %>% 
  group_by(Region) %>% 
  filter(Year_2==(2021:1970))


KC_2Packaging<-KB_2Packaging %>%
  group_by(Region) %>% 
  mutate(Year_2=c(2021:1900)) %>% 
  select(Year_2, Region, Major_Market_Sector,WasteGen) %>% 
  group_by(Region) %>% 
  filter(Year_2==(2021:1970))

KC_3Building<-KB_3Building %>%
  group_by(Region) %>% 
  mutate(Year_2=c(2021:1900)) %>% 
  select(Year_2, Region, Major_Market_Sector, WasteGen) %>% 
  group_by(Region) %>% 
  filter(Year_2==(2021:1970))

KC_4Electric<-KB_4Electric %>%
  group_by(Region) %>% 
  mutate(Year_2=c(2021:1900)) %>% 
  select(Year_2, Region, Major_Market_Sector,WasteGen) %>% 
  group_by(Region) %>% 
  filter(Year_2==(2021:1970))

KC_5House<-KB_5House %>%
  group_by(Region) %>% 
  mutate(Year_2=c(2021:1900)) %>% 
  select(Year_2, Region, Major_Market_Sector,WasteGen) %>% 
  group_by(Region) %>% 
  filter(Year_2==(2021:1970))

KC_6Agri<-KB_6Agri%>%
  group_by(Region) %>% 
  mutate(Year_2=c(2021:1900)) %>% 
  select(Year_2, Region, Major_Market_Sector,WasteGen) %>% 
  group_by(Region) %>% 
  filter(Year_2==(2021:1970))

KC_7Other<-KB_7Other%>%
  group_by(Region) %>% 
  mutate(Year_2=c(2021:1900)) %>% 
  select(Year_2, Region, Major_Market_Sector,WasteGen) %>% 
  group_by(Region) %>% 
  filter(Year_2==(2021:1970))

KD<-rbind(KC_1Transport,KC_2Packaging,KC_3Building,
          KC_4Electric,KC_5House,KC_6Agri,KC_6Agri,KC_7Other)

# 11. GOAL 
# Extending the curve by sector and region to 2050 
# 
# PROCESS FLOW 
# 1. Set Up
# 2. Import
# 3. Tidy
# 4. Transform
# 5. **Visualize**
# 6. Export

print("== 5 / 6: Visualization ==")
 
### WASTE GENERATION FORCASTED TO 2050 

# So, I modeled this just for CHINA and the TRANSPORTATION SECTOR:

KD_1<-KD %>% 
  filter(Region=="China"& Major_Market_Sector=="Transportation") %>% 
  select(Year_2, WasteGen)

KD_1_Model<- loess(WasteGen ~ Year_2, 
                     data = KD_1, 
                     control = loess.control(surface = "direct"))

KD_1_2050 <- data.frame(Year_2 = 1970:2050)
KD_1_2050$WasteGen <- predict(KD_1_Model, KD_1_2050, se = TRUE)$fit
KD_1_2050

ggplot() + 
geom_line(data=KD_1_2050, aes(x=Year_2, y=WasteGen), color='blue') + 
geom_line(data=KD_1, aes(x=Year_2, y=WasteGen), color='red')+
xlab("Year")+
ylab("Plastic Waste Generation in China Transportation")+
ggtitle("China Transportation Plastic Waste")
ggsave(glue("{data_dir}/output_r/china_transport.png"))

# Ciera and Sam - it is very obvious now that its the same code chunk repeated for each Region and each market sector. 
# Is there a way to loop this? Perhaps using apply mapply lapply functions? 


# China + Packaging

KD_2<-KD %>% 
  filter(Region=="China"& Major_Market_Sector=="Packaging") %>% 
  select(Year_2, WasteGen)

KD_2_Model<- loess(WasteGen ~ Year_2, 
                     data = KD_2, 
                     control = loess.control(surface = "direct"))

KD_2_2050 <- data.frame(Year_2 = 1970:2050)
KD_2_2050$WasteGen <- predict(KD_2_Model, KD_1_2050, se = TRUE)$fit
KD_2_2050

ggplot() + 
geom_line(data=KD_2_2050, aes(x=Year_2, y=WasteGen), color='blue') + 
geom_line(data=KD_2, aes(x=Year_2, y=WasteGen), color='red')+
xlab("Year")+
ylab("Plastic Waste Generation in China Packaging")+
ggtitle("China Packaging Plastic Waste")
ggsave(glue("{data_dir}/output_r/china_packaging.png"))

# China + Building & Construction

KD_3<-KD %>% 
  filter(Region=="China" & Major_Market_Sector=="Building_Construction") %>% 
  select(Year_2, WasteGen)

levels(KD$Major_Market_Sector)

KD_3_Model<- loess(WasteGen ~ Year_2, 
                     data = KD_3, 
                     control = loess.control(surface = "direct"))

KD_3_2050 <- data.frame(Year_2 = 1970:2050)
KD_3_2050$WasteGen <- predict(KD_3_Model, KD_3_2050, se = TRUE)$fit
KD_3_2050

ggplot() + 
geom_line(data=KD_3_2050, aes(x=Year_2, y=WasteGen), color='blue') + 
geom_line(data=KD_3, aes(x=Year_2, y=WasteGen), color='red')+
xlab("Year")+
ylab("Plastic Waste Generation in China Building & Construction")+
ggtitle("China Building & Construction Plastic Waste")
ggsave(glue("{data_dir}/output_r/china_building.png"))

# China + Electrical & Electric

KD_4<-KD %>% 
  filter(Region=="China" & Major_Market_Sector=="Electrical_Electronic") %>% 
  select(Year_2, WasteGen)

levels(KD$Major_Market_Sector)

KD_4_Model<- loess(WasteGen ~ Year_2, 
                     data = KD_4, 
                     control = loess.control(surface = "direct"))

KD_4_2050 <- data.frame(Year_2 = 1970:2050)
KD_4_2050$WasteGen <- predict(KD_4_Model, KD_4_2050, se = TRUE)$fit
KD_4_2050

ggplot() + 
geom_line(data=KD_4_2050, aes(x=Year_2, y=WasteGen), color='blue') + 
geom_line(data=KD_4, aes(x=Year_2, y=WasteGen), color='red')+
xlab("Year")+
ylab("Plastic Waste Generation in China Electric & Electrical")+
ggtitle("China Electric & Electronic Plastic Waste")
ggsave(glue("{data_dir}/output_r/china_electrical.png"))

KD_5<-KD %>% 
  filter(Region=="China" & Major_Market_Sector=="Household_Leisure_Sports") %>% 
  select(Year_2, WasteGen)

KD_5_Model<- loess(WasteGen ~ Year_2, 
                     data = KD_5, 
                     control = loess.control(surface = "direct"))

KD_5_2050 <- data.frame(Year_2 = 1970:2050)
KD_5_2050$WasteGen <- predict(KD_5_Model, KD_5_2050, se = TRUE)$fit
KD_5_2050

ggplot() + 
geom_line(data=KD_5_2050, aes(x=Year_2, y=WasteGen), color='blue') + 
geom_line(data=KD_5, aes(x=Year_2, y=WasteGen), color='red')+
xlab("Year")+
ylab("Plastic Waste Generation in China Household")+
ggtitle("China Houseshold Leisure & Sport Plastic Waste")
ggsave(glue("{data_dir}/output_r/china_sport_leisure.png"))

# China Agriculture Plastic Waste

KD_6<-KD %>% 
  filter(Region=="China" & Major_Market_Sector=="Agriculture") %>% 
  select(Year_2, WasteGen)

KD_6_Model<- loess(WasteGen ~ Year_2, 
                     data = KD_6, 
                     control = loess.control(surface = "direct"))

KD_6_2050 <- data.frame(Year_2 = 1970:2050)
KD_6_2050$WasteGen <- predict(KD_6_Model, KD_6_2050, se = TRUE)$fit

ggplot() + 
geom_line(data=KD_6_2050, aes(x=Year_2, y=WasteGen), color='blue') + 
geom_line(data=KD_6, aes(x=Year_2, y=WasteGen), color='red')+
xlab("Year")+
ylab("Plastic Waste Generation in China Agriculture")+
ggtitle("China Agriculture Plastic Waste")
ggsave(glue("{data_dir}/output_r/china_agriculture.png"))

# China Others Plastic Waste

KD_7<-KD %>% 
  filter(Region=="China" & Major_Market_Sector=="Others") %>% 
  select(Year_2, WasteGen)

KD_7_Model<- loess(WasteGen ~ Year_2, 
                     data = KD_7, 
                     control = loess.control(surface = "direct"))

KD_7_2050 <- data.frame(Year_2 = 1970:2050)
KD_7_2050$WasteGen <- predict(KD_7_Model, KD_7_2050, se = TRUE)$fit
KD_7_2050

ggplot() + 
geom_line(data=KD_6_2050, aes(x=Year_2, y=WasteGen), color='blue') + 
geom_line(data=KD_6, aes(x=Year_2, y=WasteGen), color='red')+
xlab("Year")+
ylab("Plastic Waste Generation in China Others")+
ggtitle("China Others Plastic Waste")
ggsave(glue("{data_dir}/output_r/china_other.png"))

# 11. GOAL 
# Extending the curve by sector and region to 2050 
# 
# PROCESS FLOW 
# 1. Set Up
# 2. Import
# 3. Tidy
# 4. Transform
# 5. Visualize
# 6. **Export**

print("== 6 / 6: Export ==")
write.csv(F05, file=glue("{root_dir}/output_r/F05.csv"))
write.csv(G4, file=glue("{root_dir}/output_r/G4.csv"))
write.csv(G7, file=glue("{root_dir}/output_r/G7.csv"))
write.csv(G9, file=glue("{root_dir}/output_r/G9.csv"))
write.csv(H.1, file=glue("{root_dir}/output_r/H.1.csv"))
write.csv(H2, file=glue("{root_dir}/output_r/H2.csv"))
write.csv(H5, file=glue("{root_dir}/output_r/H5.csv"))
write.csv(H6, file=glue("{root_dir}/output_r/H6.csv"))
write.csv(HH8, file=glue("{root_dir}/output_r/HH8.csv"))
write.csv(HH9.2, file=glue("{root_dir}/output_r/HH9.2.csv"))
write.csv(IA2, file=glue("{root_dir}/output_r/IA2.csv"))
write.csv(IA3, file=glue("{root_dir}/output_r/IA3.csv"))
write.csv(KA3, file=glue("{root_dir}/output_r/KA3.csv"))
write.csv(KD, file=glue("{root_dir}/output_r/KD.csv"))
write.csv(KD_1_2050, file=glue("{root_dir}/output_r/KD_1_2050.csv"))
write.csv(KD_2_2050, file=glue("{root_dir}/output_r/KD_2_2050.csv"))
write.csv(KD_3_2050, file=glue("{root_dir}/output_r/KD_3_2050.csv"))
write.csv(KD_4_2050, file=glue("{root_dir}/output_r/KD_4_2050.csv"))
write.csv(KD_5_2050, file=glue("{root_dir}/output_r/KD_5_2050.csv"))
write.csv(KD_6_2050, file=glue("{root_dir}/output_r/KD_6_2050.csv"))
write.csv(KD_7_2050, file=glue("{root_dir}/output_r/KD_7_2050.csv"))
