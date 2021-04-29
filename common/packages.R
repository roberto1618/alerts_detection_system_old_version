# Instalacion de paquetes si no son encontrados
if("emayili" %in% rownames(installed.packages()) == FALSE) {install.packages("emayili")}
if("magrittr" %in% rownames(installed.packages()) == FALSE) {install.packages("magrittr")}
if("googleAnalyticsR" %in% rownames(installed.packages()) == FALSE) {install.packages("googleAnalyticsR")}
if("data.table" %in% rownames(installed.packages()) == FALSE) {install.packages("data.table")}
if("tseries" %in% rownames(installed.packages()) == FALSE) {install.packages("tseries")}
if("lubridate" %in% rownames(installed.packages()) == FALSE) {install.packages("lubridate")}
if("forecast" %in% rownames(installed.packages()) == FALSE) {install.packages("forecast")}
if("BSDA" %in% rownames(installed.packages()) == FALSE) {install.packages("BSDA")}
if("googleAuthR" %in% rownames(installed.packages()) == FALSE) {install.packages("googleAuthR")}
if("zoo" %in% rownames(installed.packages()) == FALSE) {install.packages("zoo")}
if("shinythemes" %in% rownames(installed.packages()) == FALSE) {install.packages("shinythemes")}
if("dplyr" %in% rownames(installed.packages()) == FALSE) {install.packages("dplyr")}
if("uroot" %in% rownames(installed.packages()) == FALSE) {install.packages("uroot")}
if("ggplot2" %in% rownames(installed.packages()) == FALSE) {install.packages("ggplot2")}
if("googlesheets" %in% rownames(installed.packages()) == FALSE) {install.packages("googlesheets")}
if("googlesheets4" %in% rownames(installed.packages()) == FALSE) {install.packages("googlesheets4")}
if("gsheet" %in% rownames(installed.packages()) == FALSE) {install.packages("gsheet")}
if("readr" %in% rownames(installed.packages()) == FALSE) {install.packages("readr")}
if("bigrquery" %in% rownames(installed.packages()) == FALSE) {install.packages("bigrquery")}
if("stringr" %in% rownames(installed.packages()) == FALSE) {install.packages("stringr")}
if("httpuv" %in% rownames(installed.packages()) == FALSE) {install.packages("httpuv")}
if("RGoogleAnalytics" %in% rownames(installed.packages()) == FALSE) {install.packages("RGoogleAnalytics")}
if("bigQueryR" %in% rownames(installed.packages()) == FALSE) {install.packages("bigQueryR")}
if("rjson" %in% rownames(installed.packages()) == FALSE) {install.packages("rjson")}
if("prophet" %in% rownames(installed.packages()) == FALSE) {install.packages("prophet")}
if("Hmisc" %in% rownames(installed.packages()) == FALSE) {install.packages("Hmisc")}
if("logging" %in% rownames(installed.packages()) == FALSE) {install.packages("logging")}
if("optparse" %in% rownames(installed.packages()) == FALSE) {install.packages("optparse")}
if("prophet" %in% rownames(installed.packages()) == FALSE) {install.packages("prophet", type="source")}
if("openxlsx" %in% rownames(installed.packages()) == FALSE) {install.packages("openxlsx")}
if("writexl" %in% rownames(installed.packages()) == FALSE) {install.packages("writexl")}
if("httr" %in% rownames(installed.packages()) == FALSE) {install.packages("httr")}
if("BBmisc" %in% rownames(installed.packages()) == FALSE) {install.packages("BBmisc")}

# Paquetes necesarios
library(emayili)
library(magrittr)
library(googleAnalyticsR)
library(data.table)
library(tseries)
library(lubridate)
library(forecast)
library(BSDA)
library(googleAuthR)
library(zoo)
library(shinythemes)
library(dplyr)
library(uroot)
library(ggplot2)
library(googlesheets)
library(googlesheets4)
library(gsheet)
library(readr)
library(bigrquery)
library(stringr)
library(httpuv)
library(RGoogleAnalytics)
library(bigQueryR)
library(rjson)
library(prophet)
library(Hmisc)
library(logging)
library(optparse)
library(openxlsx)
library(writexl)
library(httr)
library(BBmisc)