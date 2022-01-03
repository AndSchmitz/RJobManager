#
#Example script to be run with RJobManager
#


#init-----
#Do some initialization if desired
# rm(list=ls()) #clear workspace
# graphics.off() #turn off any open graphics
# options(
#   warnPartialMatchDollar = T, #warn if incomplete variable names match existing variables
#   dplyr.summarise.inform=F #Disable unnecessary info messages
# )

#library(tidyverse) #load some libraries if desired


#Get working directory from RJobManager
WorkDir <- commandArgs(trailingOnly = TRUE)[1]

#Preparations----
#Load helping functions and input data if required.
#Must be located in WorkDir (or absolute path names must be known)
#source(file.path(WorkDir,"HelpingFunctions.R"))
#Set input/output directories
#InDir <- file.path(WorkDir,"Input")
#InputTable <- read.table(..)
#Create output directory if not existing
OutDir <- file.path(WorkDir,"Output")
dir.create(OutDir,showWarnings = F)

#Run calculations-----
DummyOutput <- data.frame(
  x = rnorm(n = 100),
  y = rnorm(n = 100)
)
write.table(
  x = DummyOutput,
  file = file.path(OutDir,"MyOutput.csv"),
  sep = ";",
  row.names = F
)

print("Done.")
