#2021-12-11 Andreas Schmitz
#R script that acts as a job manager for other R scripts.
#Each job must consists of a job directory (e.g. "Job1") in folder "Todo" and
#a file "Main.R" in its job folder. The job can read/write in its job directory.
#The Main.R must contain a line "WorkDir <- commandArgs(trailingOnly = TRUE)[1]"
#to recieve the absolute path to its job direcory (=WorkDir) from the job manager
#at runtime.


#init-----
rm(list=ls())
graphics.off()
options(
  warnPartialMatchDollar = T,
  stringsAsFactors = F,
  dplyr.summarise.inform = F
)

library(tidyverse)

nParallelJobsMax <- 2
PathToSystemCommandRscript <- "/usr/local/bin/Rscript"
RootDir <- "/mnt/ExternePlatte/Other/WorkTemp/RJobs"

#Prepare directory structure------
ToDoDir <- file.path(RootDir,"Todo")
SuccessDir <- file.path(RootDir,"Success")
InProgressDir <- file.path(RootDir,"InProgress")
FailureDir <- file.path(RootDir,"Failure")
LogFilePath <- file.path(RootDir,"LogFile.txt")
if ( !dir.exists(ToDoDir) ) {
  dir.create(ToDoDir, showWarnings = F)
}
if ( !dir.exists(SuccessDir) ) {
  dir.create(SuccessDir, showWarnings = F)
}
if ( !dir.exists(InProgressDir) ) {
  dir.create(InProgressDir, showWarnings = F)
}
if ( !dir.exists(FailureDir) ) {
  dir.create(FailureDir, showWarnings = F)
}

#Help funs-----
GetTimeStamp <- function() {
  return(format(Sys.time(), format = "%Y-%m-%d %H-%M-%S"))
}
Log <- function(Text) {
  LogFileHandle <- file(
    description = LogFilePath,
    open = "a"
  )
  writeLines(
    text = paste0(GetTimeStamp(),"\t",Text),
    con = LogFileHandle
  )
  close(LogFileHandle)
}


#Check for new job existing------
Log("JobManager started.")
NewJobs <- data.frame(
  FullPath = list.dirs(
    path = ToDoDir,
    recursive = F,
    full.names = T
  )
)
if ( nrow(NewJobs) == 0 ) {
  Log("No jobs todo. Quitting.")
  quit()
}


#Check for running jobs---
JobsRunning <- list.dirs(
  path = InProgressDir,
  recursive = F
)
nJobsRunning <- length(JobsRunning)
if ( nJobsRunning >= nParallelJobsMax ) {
  Log(paste("Already",nJobsRunning,"jobs running. Quitting."))
  quit()
}


#Identify oldest job in queu-----
NewJobs <- NewJobs %>%
  rowwise() %>%
  mutate(
    TimeStamp = file.info(FullPath)$ctime
  ) %>%
  ungroup() %>%
  arrange(TimeStamp) %>%
  mutate(
    JobName = basename(FullPath)
  )
CurrentJobName <- NewJobs$JobName[1]
Log(paste("Found Job:", CurrentJobName))

#Move Job to InProgressDir-----
CurrentJobDirBasename <- paste0(GetTimeStamp()," ",CurrentJobName)
file.rename(
  from = file.path(ToDoDir, CurrentJobName),
  to = file.path(InProgressDir, CurrentJobDirBasename)
)

#Check for Main.R------
RScriptPath <- file.path(InProgressDir, CurrentJobDirBasename,"Main.R")
if ( !file.exists(RScriptPath)) {
  file.rename(
    from = file.path(InProgressDir, CurrentJobDirBasename),
    to = file.path(FailureDir, CurrentJobDirBasename)
  )
  Log(paste("No Main.R found for Job:", CurrentJobName," Quitting."))
  quit()
}

#Call Main.R-----
#Hand over CurrentJobDir as workdir
Log(paste("Starting Job:",CurrentJobName))
Success <- F
tryCatch(
  expr = {
    ErrorCode <- system(paste0(PathToSystemCommandRscript," \"",RScriptPath,"\" \"",file.path(InProgressDir, CurrentJobDirBasename),"\""))
    if ( ErrorCode == 0 ) {
      Success <<- T
    } else {
      Success <<- F
    }
  },
  error = function(ErrorCondition) {
    Success <<- F
  }
)
if ( Success ) {
  Log(paste("Job:",CurrentJobName,"successful."))
  file.rename(
    from = file.path(InProgressDir, CurrentJobDirBasename),
    to = file.path(SuccessDir, CurrentJobDirBasename)
  )
} else {
  Log(paste("Job:",CurrentJobName,"failed."))
  file.rename(
    from = file.path(InProgressDir, CurrentJobDirBasename),
    to = file.path(FailureDir, CurrentJobDirBasename)
  )
}

#eof
