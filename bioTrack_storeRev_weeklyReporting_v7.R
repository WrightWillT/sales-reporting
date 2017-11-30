########################################################################################################################
# NOTES
########################################################################################################################
# Purpose: This script generates the data for a weekly sales report

# Methodology: 
#   The BioTrack application includes important data not provided by the API (which only reports basic data).
#   This being the case, data is pulled from the BioTrack application (a data provider licensed by the state to report sales),
#     then cleaned in the "Read an Manipulate Data" section. 
#   Next, the license numbers are obtained from the API via SQL extractions in "Append License Number Using BioTrack API".
#   After that, a "last updated" field (for when the [censored company] menu was last updated) is appended by joining on 
#     LCBID between the menu and the data from BioTrack.
#   Finally, once written to .csv, this data is dropped into Excel for presentation.

# VERSION NOTES:
# V1-3: standard getting set-up going through versions quickly as save-points
# V4: acknowledges that I can't get productname via the API at all.  Basically, I need to do an export from BioTrackTHC,
#   then run a macro to reformat, and next do all the data manipulation here before exporting and dropping into an Excel
#   report.  The data manipulation that needs to happen is to append lcb_id and weekPeriod.  
# V5: removes dupes.  The total revenue doesn't match biotrack exactly because of the dupe trim.
#   As I later learned, this creates dupes because 6.01564*10^x doesn't capture the unique last digits so I gotta turn this off
# V6: adds in a date for menu last modified so I can flag stores that are out of business more easily
# V7: Added "FF" and "lot" to the brand list

########################################################################################################################
# LOAD PACKAGES AND SET OPTIONS
########################################################################################################################
library(anytime)
library(RODBC)

options(scipen = 999)

########################################################################################################################
# READ AND MANIPULATE DATA
########################################################################################################################

# read historical data from BioTrackTHC application (the reformatted export report)
histDat <- read.csv("biotrack_inventoryTransfers_reformatted.csv")
# rename
colnames(histDat) <- c("productname","strain","inventoryid","quantity","dispensary","price","transfer","date")
# reorganize
histDat$brand <- NA
histDat <- histDat[,c(8,3,5,9,1,2,4,6)]

histDat$date <- as.Date(histDat$date)

# append weekOfYear, year, and weekPeriod (weeks since jan 1 2015)
histDat <- tbl_df(histDat) %>%
  mutate(weekOfYear = as.numeric(format(date, format = "%W")), 
         year = as.numeric(format(date,format = "%Y")),
         weekPeriod = (year-2015)*52+weekOfYear)

# append brand using unique tokens that only appear for the brand

for(i in 1:nrow(histDat)){
  if(grepl("MN",histDat$productname[i])==T) histDat$brand[i] <- "Marley Natural"
  if(grepl("HL",histDat$productname[i])==T) histDat$brand[i] <- "Headlight"
  if(grepl("DY",histDat$productname[i])==T) histDat$brand[i] <- "Dutchy"
  if(grepl("FF",histDat$productname[i])==T) histDat$brand[i] <- "Final Frontier"
  if(grepl("\\blot\\b",histDat$productname[i])==T) histDat$brand[i] <- "Bulk Flower"
}

# remove vendor samples (rows with price of 0)
histDat <- histDat[histDat$price!=0,] #7971 rows

# convert histDat$inventory id from factor to numeric by getting rid of spaces
histDat$inventoryid <- as.numeric(gsub(" ","",as.character(histDat$inventoryid))) #8029 rows

# group by inventory id
histDatBoiled <- tbl_df(histDat) %>%
  group_by(inventoryid) %>%
  summarize(date = first(date), dispensary = first(dispensary), brand = first(brand), productname = first(productname),
            strain = first(strain), quantity = sum(quantity), price = sum(price), weekOfYear = first(weekOfYear),
            year = first(year), weekPeriod = first(weekPeriod))


########################################################################################################################
# APPEND LICENSE NUMBER USNIG BIOTRACK API
########################################################################################################################

# get license number from BioTrack using manifestid in manifest_stop_data, which you can get from inventoryid

# connect to PH_staging (aka Biotrack API)
channel <- odbcConnect("[censored database]", uid="will.wright", pwd="[censored password]") # connect by entering credentials

# grab manifest stop data with SQL
manifestStopQuery <- "SELECT * FROM `sync_manifest.manifest_stop_data`"
manifest_stop_data <- sqlQuery(channel,manifestStopQuery)
manifest_stop_data <- mutate(manifest_stop_data, lookupid = paste0(manifestid, stopnumber))

# get stop items since the inventory id from the BioTrackTHC export is the only thing that links to a table
# in this case, we get manifestid from manifest_stop_items and then can get license_number from manifest_stop_data
manifestItemQuery <- "SELECT * FROM `sync_manifest.manifest_stop_items`"
manifest_stop_items <- sqlQuery(channel,manifestItemQuery)
manifest_stop_items <- manifest_stop_items[manifest_stop_items$deleted==0,] # removing deleted items
manifest_stop_items <- mutate(manifest_stop_items, lookupid = paste0(manifestid, stopnumber)) # creating lookupid (for the manifest)
manifest_stop_items$sessiontime <- anydate(manifest_stop_items$sessiontime) # convert sessiontime to a date

# join on inventoryid to get lookupid (manifestid + stopnumber because manifestid isn't unique by itself)
histDatLookup <- merge(histDat, manifest_stop_items[,c(7,12)], by.x = "inventoryid", by.y = "inventoryid") #8172 rows with 2153296 in rev

# remove duplicates by grouping on inventoryid and dispensary, then summarizing everything else
histDatLookupBoiled <- tbl_df(histDatLookup) %>%
  group_by(inventoryid,dispensary, date) %>%
  summarize(brand = first(brand), productname = first(productname), strain = first(strain), 
            quantity = first(quantity), price = first(price), weekOfYear = first(weekOfYear), year = first(year),
            weekPeriod = first(weekPeriod), lookupid = first(lookupid))

# join on lookupid to get license_number
histDatMerged <- merge(histDatLookupBoiled, manifest_stop_data[,c(7,19)], by.x = "lookupid", by.y = "lookupid")

# reorder
histDatMerged <- histDatMerged[with(histDatMerged,order(-histDatMerged$weekPeriod)),]

# write to csv for reporting
curDate <- Sys.Date()
# write.csv(histDatMerged,paste0("storeRevSummary_",curDate,".csv"), row.names = F)

########################################################################################################################
# APPEND LAST UPDATED BY USING [censored company] MENU AND LCBID
########################################################################################################################
# generate a table of LCBID and last updated from the leafy menu

# read the menu if it isn't in memory
# menu<-read.csv("menu_wip8_2017-08-30.csv")

# need to append LCB_ID from the lookup to the menu first
lcbLookup <- read.csv("stores_censoredCompany_502_lookup_v6.csv")
menuLcb <- merge(menu, lcbLookup[,c(1,2,4)], by.x = "dispensary_id", by.y = "censoredCompany_id")

#convert created to date
menuLcb$created <- anydate(menuLcb$created)
menuLastUpdated <- tbl_df(menuLcb) %>%
  group_by(lcb_id) %>%
  summarize(last_updated = max(created), sales_2017 = first(sales_2017))


# append last updated
histDatMergedLcb <- merge(histDatMerged, menuLastUpdated, by.x = "license_number", by.y = "lcb_id", all.x = T)
# create flag for "menu updated within the past month"
histDatMergedLcb$menuOutOfDate <- NA
for(i in 1:nrow(histDatMergedLcb)){
  if(is.na(histDatMergedLcb$last_updated[i])==F){
    if(histDatMergedLcb$last_updated[i]<(curDate-30)) histDatMergedLcb$menuOutOfDate[i] <- 1
  }
}

write.csv(histDatMergedLcb,paste0("storeRevSummary_",curDate,".csv"), row.names = F)

