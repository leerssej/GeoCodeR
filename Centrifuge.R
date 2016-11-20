# R Script - dplyr predominant
# Author: leerssej
# Date: Updated 20 NOV 2016
# Desc: Centrifuge up the errors encountered in previous address validation and geocoding runs.
# Desc: Slice off good data and bad data  
# Desc: Bad will be reverified and coded with GeoCodeR and Google Maps API. 
# Desc: Afterwards it will be re-rowBound with good addressData collection.
# Desc: Review and Error identification analysis begins anew - culminating in another round of 
# Desc: centrifuging and isolation of errors for reverification

options(stringsAsFactors = FALSE)
library(magrittr) 
library(tidverse)

###### 31. Centrifuge up the Bugs for Redo7 - commas, oblastTwice, NA's######
#### ### ### Remember there are 50949 records in this file ### ### ###
## This run will be to gather errors made in the Cnctnt - the next will be retrying holes w/ AddrLn1 & ln2
Panoplyv6 <- read.csv("Panoplyv6.csv", stringsAsFactors = F, na.strings = c("", " ", "NA"))
glimpse(Panoplyv6)
### The NAfull found in CT3GC4 ###
# Add COlumns for presence of NA, double oblasts, excess and starter commas
BuggedPanpoplyv6 <- 
    Panoplyv6 %>% 
    mutate(flgNAfull = 1*grepl("(^|\\s+)NA,\\s+", cnCtAddr),
           flg2oblast = 1*grepl("oblast' oblast'", cnCtAddr),
           flg2comma = 1*grepl(", , ", cnCtAddr),
           flgStartComma = 1*grepl("^, ", cnCtAddr)
           ) %>%
    select(-Rcs) %>% 
    filter(flgNAfull == 1 | flg2oblast == 1 | flg2comma == 1 | flgStartComma == 1) %>% 
    arrange(stdCountry, stdCity, swpAddrLn1, swpAddrLn2)
glimpse(BuggedPanpoplyv6)
save(BuggedPanpoplyv6, file = "BuggedPanpoplyv6")

UnBuggedPanpoplyv6 <- 
    Panoplyv6 %>% 
    mutate(flgNAfull = 1*grepl("(^|\\s+)NA,\\s+", cnCtAddr),
           flg2oblast = 1*grepl("oblast' oblast'", cnCtAddr),
           flg2comma = 1*grepl(", , ", cnCtAddr),
           flgStartComma = 1*grepl("^, ", cnCtAddr)
           ) %>%
    select(-Rcs) %>% 
    filter(flgNAfull != 1 & flg2oblast != 1 & flg2comma != 1 & flgStartComma != 1) %>% 
    arrange(stdCountry, stdCity, swpAddrLn1, swpAddrLn2)
glimpse(UnBuggedPanpoplyv6)
save(UnBuggedPanpoplyv6, file = "UnBuggedPanpoplyv6")
