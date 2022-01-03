#2021-12-11 Andreas Schmitz
#R script that acts as a job manager for other R scripts.
#Use case: Schedule a number of R scripts that potentially take long to finish
#on a remote machine and automatically run them one after another. The maximum
#number of jobs running in parallel can be controlled by parameter
#"nParallelJobsMax" (see below).
#
#This script be called for example once per minute (e.g. by cron). It looks for
#a new R job (must be located in subfolder "Todo") and executes it, if the
#maximum number of jobs running in parallel is not exceeded. Each instance of
#this script handles one R job and quits on success, on failure or if no job
#to process was found.
#
#Each job must consist of a job folder (e.g. "Job1") in a "Todo"-folder and
#a file "Main.R" in the job folder (see below). Each R job (Main.R) can
#read/write in its job directory. The Main.R must contain a line
#"WorkDir <- commandArgs(trailingOnly = TRUE)[1]" to recieve the absolute path
#to its job direcory (=WorkDir) from the job manager at runtime.
#See Main.R for an example.
#
#Directory structure:
#/RJobManagerPath/RJobManager.R
#/RJobManagerPath/Todo/Job1/Main.R
#/RJobManagerPath/Todo/Job1/SomeOptionalInputDirectories
#/RJobManagerPath/Todo/Job1/SomeOptionalHelpFuns.R
#/RJobManagerPath/Todo/Job2/Main.R
#/RJobManagerPath/Todo/Job2/SomeOptionalInputDirectories
#/RJobManagerPath/Todo/Job2/SomeOptionalHelpFuns.R
#etc.

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
RJobManagerPath <- "/path/to/RJobManagerDir"

#No changes required below------

#Prepare directory structure------
ToDoDir <- file.path(RJobManagerPath,"Todo")
SuccessDir <- file.path(RJobManagerPath,"Success")
InProgressDir <- file.path(RJobManagerPath,"InProgress")
FailureDir <- file.path(RJobManagerPath,"Failure")
LogFilePath <- file.path(RJobManagerPath,"LogFile.txt")
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

MoveFolder <- function(
  MoveFrom,
  MoveTo
) {
  #A simple rename() does not always work across discs (external and internal disk)
  dir.create(
    path = MoveTo,
    recursive = T,
    showWarnings = F
  )
  file.copy(
    from = list.files(MoveFrom, full.names = TRUE), 
    to = MoveTo,
    recursive = TRUE
  )
  unlink(
    x = MoveFrom,
    recursive = T
  )
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
MoveFrom = file.path(ToDoDir, CurrentJobName)
MoveTo = file.path(InProgressDir, CurrentJobDirBasename)
MoveFolder(
  MoveFrom = MoveFrom,
  MoveTo = MoveTo
)


#Check for Main.R------
RScriptPath <- file.path(InProgressDir, CurrentJobDirBasename,"Main.R")
if ( !file.exists(RScriptPath)) {
  MoveFolder(
    MoveFrom = file.path(InProgressDir, CurrentJobDirBasename),
    MoveTo = file.path(FailureDir, CurrentJobDirBasename)
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
  MoveFolder(
    MoveFrom = file.path(InProgressDir, CurrentJobDirBasename),
    MoveTo = file.path(SuccessDir, CurrentJobDirBasename)
  )
} else {
  Log(paste("Job:",CurrentJobName,"failed."))
  MoveFolder(
    MoveFrom = file.path(InProgressDir, CurrentJobDirBasename),
    MoveTo = file.path(FailureDir, CurrentJobDirBasename)
  )
}

#eof
