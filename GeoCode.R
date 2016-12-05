# R Script - dplyr predominant
# Author: leerssej
# Date: Updated 20 NOV 2016
# Desc: Slicing appropriate size chunk of records to send for GeoCoding data
# Desc: Assemble concatenated address data properly relative to each country
# Desc: Send data through ggmap API and Google Maps Geocoding API

options(stringsAsFactors = FALSE)
library(magrittr) 
library(tidverse)
library(ggmap)

###### 32. SliceR, ConcatAssembleR & GeoCodeR ####
load("BuggedPanpoplyv6")
# v6 bugged section only
#### a. SliceR
## Section designation
# Name the file
runNum <- 1
##Set the slice start and stop and name it
geocodeQueryCheck()
# Set this group's start and stop records
sectionSize <- 2500
counterstart <- 1
counterstop <- counterstart + sectionSize - 1

# Uniquely name the group that we sliced -section per date - so that we can bind all rows together properly later.
SectionName <- paste0(as.character(counterstart),"-to-", as.character(counterstop), "_", as.character(Sys.Date()))

# Slice out section we will be sending off
UndoneSection <- slice(Missing, counterstart:counterstop)# %>%  select(ConcatAddr) %>% collect %>%
# Load and return it back in without any f*ing factors gdmit
write.csv(UndoneSection, "UndoneSection.csv", na = "", row.names = F)
UndoneSectionReady2Send <- read.csv("UndoneSection.csv", stringsAsFactors = F)

### b. ConcatAssemblR
UndoneSectionReady2Send %<>% 
    mutate(cnCtAddr =   ifelse(CNTRY %in% c("DZA", "DOM", "ECU", "GBR", "GRC", "IDN", "IRL",
                                            "LTU", "NZL", "PAK", "PER","SAU", "THA", "URY"), 
                                paste0(PrefNmStrStTyp, ", ", stdCity, " ", stdPstlCd, ", ", stdCountry),
                                ifelse(CNTRY == "AUS", paste0(PrefNmStrStTyp, ", ", stdCity, " ", stdState, " ", stdPstlCd, ", ", stdCountry),
                                ifelse(CNTRY == "LVA", paste0(PrefNmStrStTyp, ", ", stdCity, ", ", "lv-", stdPstlCd, ", ", stdCountry),
                                ifelse(CNTRY == "CHL", paste0(PrefNmStrStTyp, ", ", stdCity, ", ", "region de ", stdRegion, ", ", stdCountry),
                                ifelse(CNTRY == "KAZ", paste0(PrefNmStrStTyp, ", ", stdCity, ", ", stdCountry),  ## Kazakhstan
                                ifelse(CNTRY == "IND", paste0(PrefNmStrStTyp, ", ", stdCity, ", ", stdState, ", ", stdCountry),  ## India
                                ifelse(CNTRY %in% c("EGY", "GEO", "ISR", "KWT", "MAR", "VNM"), 
                                                paste0(PrefNmStrStTyp, ", ", stdCity, ", ", stdCountry), 
                                ifelse(CNTRY == "TWN", paste0(PrefNmStrStTyp, ", ", stdCity, ", ", stdCountry, " ", stdPstlCd),
                                ifelse(CNTRY %in% c("CAN", "PAN", "ZAF"), paste0(PrefNmStrStTyp, ", ", stdCity, ", ", stdPstlCd, ", ", stdCountry),
                                ifelse(CNTRY == "UKR", paste0(PrefNmStrStTyp, ", ", stdCity, ", ", stdState, "'ka oblast", ", ", stdCountry),
                                ifelse(CNTRY == "RUS", 
                                       ifelse(is.na(stdState), 
                                              paste0(PrefNmStrStTyp, ", ", stdCity, ", ", stdCountry, ", ", stdPstlCd),     paste0(PrefNmStrStTyp, ", ", stdCity, ", ", stdState, " oblast'", ", ", stdCountry, ", ", stdPstlCd)),
                                ifelse(CNTRY %in% c("COL", "KOR"), paste0(PrefNmStrStTyp, ", ", stdCity, ", ", stdState, ", ", stdCountry),
                                ifelse(CNTRY == "SGP", paste0(PrefNmStrStTyp, ", ", stdCountry, " ", stdPstlCd),
                                ifelse(CNTRY == "ITA", ifelse(is.na(stdCAP), paste0(PrefNmStrStTyp, ", ", stdPstlCd, " ", stdCity, ", ", stdCountry), 
                                                paste0(PrefNmStrStTyp, ", ", stdPstlCd, " ", stdCity, " ", stdCAP, ", ", stdCountry)),
                                ifelse(CNTRY == "POL", paste0(PrefNmStrStTyp, ", ", stdPstlCd, " ", stdCity, " ", stdCountry),
                                ifelse(CNTRY %in% c("MYS", "ESP"), paste0(PrefNmStrStTyp, ", ", stdPstlCd, " ", stdCity, ", ", stdState, ", ", stdCountry),
                                ifelse(CNTRY %in% c("AUT", "BEL", "BGR", "HRV", "CZE", "DNK", "EST", "FIN", "FRA", "DEU", "LUX",
                                                    "NLD", "NOR", "PHL", "POR", "ROU", "SVK", "SVN", "SWE", "CHE", "TUR"),
                                                paste0(PrefNmStrStTyp, ", ", stdPstlCd, " ", stdCity, ", ", stdCountry),
                                ifelse(CNTRY %in% c("ARG", "MEX"), paste0(PrefNmStrStTyp, ", ", stdPstlCd, " ", stdCity, ", ", stdState, ", ", stdCountry),
                                ifelse(CNTRY %in% c("CHN", "HKG"), paste0(PrefNmStrStTyp, ", ", stdState, ", ", stdCountry, ", ", stdPstlCd),
                                ifelse(CNTRY == "VEN", paste0(PrefNmStrStTyp, ", ", stdCity, ", ", stdCountry),
                                ifelse(CNTRY == "HUN", paste0(stdCity, ", ", PrefNmStrStTyp, ", ", stdPstlCd, " ", stdCountry),
                                ifelse(CNTRY %in% c("USA", "JAP"), paste0(PrefNmStrStTyp, ", ", CTY, ", ", ST, " ", PSTL_CD, ", ", stdCountry),
                                paste0(PrefNmStrStTyp, ", ", CTY, ", ", ST, " ", PSTL_CD, ", ", stdCountry))))))))))))))))))))))))
UndoneSectionReady2Send$cnCtAddr %<>% gsub("(^|\\s+)NA,\\s+", "", .)
UndoneSectionReady2Send$cnCtAddr %<>% gsub(", , ", ", ", .)
UndoneSectionReady2Send$cnCtAddr %<>% gsub("^, ", "", .)
UndoneSectionReady2Send$cnCtAddr %<>% gsub("oblast' oblast'", "oblast'", .)
glimpse(UndoneSectionReady2Send)

###### c. GeoCodeR
# prep to use GGMAP
# Checkif GeoCoding Available
geocodeQueryCheck()
glimpse(UndoneSectionReady2Send)
#LogStart time
startTime <- Sys.time()
#send call to google and get reply
geo_reply = mutate_geocode(UndoneSectionReady2Send, cnCtAddr, output = 'more', source = 'google')
# Sample API call from MA
stopTime <- Sys.time()
##write the organized file to collection
# Check dir
dir()
#write the Filename with its slice and time date stamp in
write.csv(geo_reply, file = paste0("GeoReplyCT3GC4MutateGrp_Run", runNum , "_from", SectionName, "_NewGeoCodes.csv"), row.names = F)
#check to see that it got written
dir()
names(geo_reply)
