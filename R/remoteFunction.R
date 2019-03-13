#  remoteFunctionSSH - execute R functions remotely via SSH
#  Copyright (C) 2019  Georg Schnabel
#  
#  remoteFunctionSSH is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  
#  remoteFunctionSSH is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>

#' Initiate SSH
#'
#' Initiates the SSH connection and returns an object
#' to define and execute functions remotely
#'
#' @param login login in the form user@host
#' @param password password for the ssh connection
#' @param pwfile file whose first line contains password (overrides \code{password})
#' @param tempdir.loc directory on local machine to store files
#' @param tempdir.rem directory on remote machine to store files
#' @param Rscript path to the Rscript executable on the remote machine
#' @param timeout.con duration after which a connection attempt is considered as timed out
#' @param timeout.cmd duration after which a bash command is considered timed out
#' @param verbosity verbosity of the status messages
#' @param delay waiting time between connection attempts (lower limit)
#' @param delay2 waiting time between connection attempts (upper limit)
#' @param use.exist should functions and results already available in \code{tempdir.loc} be used
#'
#' @details
#' The returned list contains the following functions:
#' \tabular{ll}{
#' \code{createRemoteFunction(fun, fun.name=NULL,
#' cache=TRUE, transfer=TRUE, timeout=NULL,
#' show.output=TRUE)}\tab
#' Defines a function \code{fun} with name \code{fun.name}.
#' Returns a function with the same arguments as \code{fun} that remotely evaluates \code{fun}.
#' The flag \code{cache} determines whether inputs and results should be cached on the local machine.
#' The maximal allowed duration of execution in seconds is specified by \code{timeout}.
#' The flag \code{show.output} determines whether output to \code{stdout} on remote machine should be
#' mirrored on the local machine. If \code{transfer=FALSE}, the function is not uploaded automatically
#' and function \code{updateAndTransfer} has to be called before execution. \cr\cr
#' \code{updateAndTransfer} \tab Batch uploads all functions created by \code{createRemoteFunction}.
#' If the directory \code{tempdir.loc} was also used in previous sessions, then also functions defined
#' in these previous sessions will be uploaded.\cr\cr
#' \code{uploadPackage(package)}\tab Uploads a package to the remote machine\cr\cr
#' \code{removePackage(package)}\tab Removes an uploaded package from the remote machine\cr\cr
#' \code{uploadGlobalSource(text=NULL,file=NULL)}\tab Uploads R code either given as file or as character string
#' that is sourced before every function execution.
#' If both \code{text} and \code{file} are specified, \code{text} will be used.\cr\cr
#' \code{uploadGlobal(...)}\tab Uploads objects to the remote machine. Syntax is the same as for \code{save}.\cr\cr
#' \code{removeGlobal()}\tab Remove all global objects and sources from the remote machine.\cr\cr\cr
#' \code{getFun(name)}\tab returns the source code of the function with the specified \code{name}\cr\cr
#' \code{listFuns()}\tab returns a character vector with the names of all registered functions.\cr\cr
#' \code{execFun(name)}\tab returns the function wrapper to execute the respective function remotely.
#' If \code{fun(arg1, arg2, ...)}, then the function can be called by \code{execFun("fun")(arg1,arg2,...)}\cr\cr
#' \code{getFunDataDir(name)} \tab returns the data directory of the function named \code{name} on the local machine\cr\cr
#' \code{getRemFunDataDir(name)} \tab returns the data directory of the function named \code{name} on the remote machine\cr\cr
#' \code{uploadFunData(name,files=NULL)} \tab uploads files from the local to the remote data directory\cr\cr
#' \code{downloadFunData(name,files=NULL)} \tab downloads files from the remote to the local data directory\cr\cr
#' \code{clearFunData(name,files=NULL,del.rem=TRUE, del.loc=TRUE)} \tab deletes data from the local and/or remote machine\cr\cr
#' \code{closeCon()}\tab Closes the connection and cleans up files (but does not delete functions and their inputs and outputs)
#' }
#'
#' The result list contains beside these functions also the objects \code{ssh} and \code{rsync}.
#' These objects are the result of calls to \code{initInteractiveSSH} and \code{initRsync} of the packages
#' \code{interactiveSSH} and \code{rsyncFacility}.
#'
#' @return
#' A list with functions to define and execute R functions, see \code{details}.
#' @export
#'
#' @import digest interactiveSSH rsyncFacility
initSSH <- function(login, password=NULL, pwfile=NULL, port=22, PS1="remFunPrompt>>>", regexPS1=NULL,
                    tempdir.loc=NULL, tempdir.rem=NULL, Rscript=NULL,
                    timeout.con=20, timeout.cmd=20, verbosity=1,
                    delay=c(1,1,1,5,5,5),delay2=delay*1.5,
                    use.exist=TRUE) {

  defaults <- list(timeout.con=timeout.con, timeout.cmd=timeout.cmd,
                   delay = delay, delay2 = delay2, verbosity=verbosity)

  loadDefaults <- function() {
    parfrm <- parent.frame()
    nm <- names(defaults)
    for (i in seq_along(nm))
      if (is.null(parfrm[[ nm[i] ]]))
        assign(nm[i],defaults[[i]],parfrm)
  }

  # global variables

  funList <- list()

  sshCon <- NULL
  rsyncObj <- NULL

  remPackageDir <- NULL
  funContainerDir <- NULL
  remFunContainerDir <- NULL
  globalDir <- NULL
  remGlobalDir <- NULL
  wrapFile <- NULL
  remWrapFile <- NULL

  # give status information

  printInfo <- function(msg,verb) {
    loadDefaults()
    if (verb<=verbosity)
      cat(paste0(msg,"\n"))
  }

  # small function database

  getFun <- function(name) {
    stopifnot(name %in% names(funList))
    funList[[name]]$fun
  }

  listFuns <- function() {
    names(funList)
  }

  execFun <- function(name) {
    stopifnot(name %in% names(funList))
    funList[[name]]$wrap
  }

  getFunDataDir <- function(name) {
    stopifnot(name %in% names(funList))
    file.path(funContainerDir,name,"data")
  }

  getRemFunDataDir <- function(name) {
    stopifnot(name %in% names(funList))
    file.path(remFunContainerDir,name,"data")
  }

  getRemPackageDir <- function() {
    remPackageDir
  }

  getRemGlobalDir <- function() {
    remGlobalDir
  }

  uploadFunData <- function(name, files=NULL) {
    stopifnot(is.null(files) || is.character(files),
              name %in% names(funList))
    funDataDir <- getFunDataDir(name)
    other.args <- character(0)
    if (!is.null(files)) {
      stopifnot(all(file.exists(file.path(funDataDir,files))))
      tmpfile <- tempfile()
      write(files,tmpfile)
      other.args <- paste0("--files-from '",tmpfile,"'")
    }
    rsyncObj$upSyncDir(paste0(funDataDir,"/"),getRemFunDataDir(name),
                    other.args=other.args)
    if (!is.null(files))
      file.remove(tmpfile)
    TRUE
  }

  downloadFunData <- function(name, files=NULL) {
    stopifnot(is.null(files) || is.character(files),
              name %in% names(funList))
    other.args=character(0)
    if (!is.null(files)) {
      tmpfile <- tempfile()
      write(files,tmpfile)
      other.args <- paste0("--files-from '",tmpfile,"'")
    }
    rsyncObj$downSyncDir(paste0(getRemFunDataDir(name),"/"),getFunDataDir(name),
                    other.args=other.args)
    if (!is.null(files))
      file.remove(tmpfile)
    TRUE
  }

  clearFunData <- function(name, files=NULL, del.rem=TRUE, del.loc=TRUE) {
    funDataDir <- getFunDataDir(name)
    remFunDataDir <- getRemFunDataDir(name)
    if (is.null(files)) {
      if (isTRUE(del.loc)) {
        files <- list.files(funDataDir, full.names = TRUE)
        unlink(files,recursive=TRUE)
      }
      if (isTRUE(del.rem)) {
        sshCon$execBash(c(paste0("rm -r '",remFunDataDir,"'"),
                          paste0("mkdir '",remFunDataDir,"'")))
      }
    } else {
      if (isTRUE(del.loc)) {
        unlink(file.path(funDataDir,files),recursive=TRUE)
      }
      if (isTRUE(del.rem)) {
        quotedFiles <- paste0("'",file.path(remFunDataDir,files),"'")
        sshCon$execBash(paste0("rm -r ",paste0(quotedFiles,collapse=" ")))
      }
    }
    TRUE
  }

  objectId <- function(obj) {
    digest(obj,algo="sha1")
  }

  # handler to deal with function on remote side

  wrap.remote <- function(argsHash) {

    # make this method invisible
    remove("wrap",pos=.GlobalEnv)
    # implement handling of warnings and errors
    stackvarname <- paste0("stackinfo_",argsHash)
    conditionFun <- function(cond) {
      stacktrace <- sys.calls()
      stacktraceChar <- sapply(stacktrace,deparse)
      startPos <- match("do.call(fun, funArgs)",stacktraceChar) + 1
      endPos <- length(stacktrace)-2
      stacktrace <- stacktrace[startPos:endPos]
      #stacktrace[[endPos]][[1]] <- as.name(get("funAttrs",.GlobalEnv)$fun.name)
      assign(stackvarname, list(condition=cond, stack=stacktrace), .GlobalEnv)
      return(cond)
    }
    funFile <- "fun.RData"
    argsFile <- paste0("history/input_",argsHash,".RData")
    load(funFile,.GlobalEnv) # contains fun and funAttrs
    funArgs <- readRDS(argsFile)
    .libPaths(c(normalizePath("../../packages"),.libPaths()))
    workDir <- getwd()
    ### source and load global entities ###
    globalDir <- "../../global"
    file.sources <- list.files(globalDir,pattern="*.R",full.names = TRUE)
    data.sources <- list.files(globalDir,pattern="*.rda",full.names = TRUE)
    sapply(data.sources,load,.GlobalEnv)
    sapply(file.sources,source,.GlobalEnv)
    ### function call ###
    setwd("data")
    resObj <- list()
    resObj[["res"]] <- tryCatch(withCallingHandlers(do.call(fun,funArgs),
                                        error = conditionFun),
                    error=function(e) e)
    ### restore working dir ###
    setwd(workDir)
    # in case an error happens, change the result structure
    if (exists(stackvarname,.GlobalEnv)) {
      resObj[["condition"]] <- get(stackvarname, .GlobalEnv)$condition
      resObj[["stack"]] <- get(stackvarname, .GlobalEnv)$stack
    }

    ### end of function call ###
    outFile <- paste0("history/output_",argsHash,".RData")
    outFilePart <- paste0(outFile,".part")
    saveRDS(resObj,file=outFilePart) # achieve atomic write operation with mv
    system(paste0("mv '",outFilePart,"' '",outFile,"'"))
    #cat(paste0("\nFINISHED - ",argsHash,"\n"))
  }

  # unwrap the output obtained from remote machine

  prepareRemoteOutput <- function(outFile,argsHash) {
    resObj <- readRDS(outFile)
    if (!is.null(resObj[["condition"]])) {
          if (all(c("error","condition") %in% class(resObj[["condition"]]))) {
            cat("### STACK TRACE ###\n")
            print(rev(resObj$stack))
            stop(resObj$condition)
          }
          else if (all(c("warning","condition") %in% class(result$condition))) {
            warning(resObj$condition)
          }
        }
    resObj$res
  }

  # create a function on the remote side

  createRemoteFunction <- function(fun, fun.name=NULL, cache=TRUE,
                                   transfer=TRUE, timeout=NULL, show.output=TRUE) {

    # check preconditions
    stopifnot(is.function(fun),is.logical(cache),
              is.null(fun.name) || is.character(fun.name))

    # load defaults
    if (is.null(timeout)) timeout <- defaults[["timeout"]]

    # local wrapper for function
    wrap.local <- function() {
      # transfer data
      funArgs <- as.list(environment())
      if ("..." %in% names(fargs))
        funArgs <- c(funArgs,list(...))

      argsHash <- objectId(funArgs)
      argsFile <- file.path(inpDir,paste0("input_",argsHash,".RData"))
      outFile <- file.path(outDir,paste0("output_",argsHash,".RData"))
      remArgsFile <- file.path(remInpDir,basename(argsFile))
      remOutFile <- file.path(remOutDir,basename(outFile))

      if (file.exists(outFile) && isTRUE(cache)) {
        return(prepareRemoteOutput(outFile,argsHash))
      }

      # test if connection alive
      sshCon$execBash("echo ping")
      rsyncObj$setSocketFile(sshCon$getSocketFile())

      printInfo("Upload input arguments to remote machine...",2)
      saveRDS(funArgs,file=argsFile)
      rsyncObj$upSyncFile(argsFile,remArgsFile)

      printInfo("Execute function on remote machine...",2)
      cmdstr <- paste0(Rscript,' --vanilla ',
                       '-e "setwd(\\"',remFunDir,'\\")" ',
                       '-e "assign(\\"Rscript\\",\\"',Rscript,'\\", .GlobalEnv)" ',
                       '-e "wrap <- readRDS(\\"',remWrapFile,'\\")" ',
                       '-e "wrap(\\"',argsHash,'\\")"')
      printInfo(paste0("\n--- EXECUTE ON REMOTE ---\n",
                       cmdstr,"\n-------------------"),2)

      remConsole <- paste0(sshCon$execBash(cmdstr, retry=TRUE, timeout.cmd=timeout)[[1]],collapse="\n")
      rsyncObj$setSocketFile(sshCon$getSocketFile())

      if (nchar(remConsole)>0 && show.output) {
        if (verbosity>=2)
          printInfo(paste0("\n--- REMOTE OUTPUT --- \n",remConsole,"\n---------------\n"),2)
        else
          cat(remConsole,"\n")
      }


      printInfo("Retrieve result from remote machine...",2)
      if (inpDir==outDir) {
        # faster if input and output dir coincide
        rsyncObj$downMoveFile(c(remArgsFile,remOutFile),inpDir)
      } else {
        rsyncObj$downMoveFile(remArgsFile,inpDir)
        rsyncObj$downMoveFile(remOutFile,outDir)
      }

      # transfer files back
      result <- prepareRemoteOutput(outFile,argsHash)
      if (!isTRUE(cache)) {
        file.remove(outFile,argsFile)
      }
      result
    }

    fargs <- formals(fun)
    formals(wrap.local) <- fargs
    # determine the name of the function
    funPat <- "^([[:alpha:]]|\\.[[:alpha:]])[[:alnum:]_.]*$"
    if (is.null(fun.name)) {
      fun.name <- deparse(substitute(fun))
      if (!grepl(funPat,fun.name))
        fun.name <- paste0("fun_",objectId(fun))
    }
    # check character name
    stopifnot(grepl(funPat,fun.name))

    # define the paths for this remote function
    funDir <- file.path(funContainerDir, fun.name)
    funFile <- file.path(funDir,"fun.RData")
    histDir <- file.path(funDir,"history")
    dataDir <- file.path(funDir,"data")
    # if this is changed, also change in wrap.remote function!!!
    inpDir <- histDir   # inpDir and outDir should be identical!
    outDir <- histDir
    remFunDir <- file.path(remFunContainerDir,basename(funDir))
    remFunFile <- file.path(remFunDir,basename(funFile))
    remHistDir <- file.path(remFunDir,basename(histDir))
    remInpDir <- remHistDir
    remOutDir <- remHistDir
    remDataDir <- file.path(remFunDir,basename(dataDir))

    # create all local and remote files
    if (isTRUE(transfer))
      printInfo("Prepare files on local and remote machine...",1)
    dir.create(funDir,showWarnings = FALSE)
    dir.create(outDir,showWarnings = FALSE)
    dir.create(dataDir,showWarnings = FALSE)

    funAttrs <- list(fun.name=fun.name,cache=cache)
    environment(fun) <- .GlobalEnv # remove missing namespace issue
    save(fun,funAttrs,file=funFile)

    #delete history if function changed
    if (fun.name %in% listFuns()) {
      if (!isTRUE(all.equal(getFun(fun.name),fun))) {
        files <- list.files(histDir)
        file.remove(file.path(histDir,files))
      }
    }

    # if not transferred here, it has to be done manually via function
    if (isTRUE(transfer)) {
      sshCon$execBash(c(paste0("mkdir '",remFunDir,"'"),
                        paste0("mkdir '",remInpDir,"'"),
                        paste0("mkdir '",remOutDir,"'"),
                        paste0("mkdir '",remDataDir,"'")),retry=TRUE)
      rsyncObj$setSocketFile(sshCon$getSocketFile())
      rsyncObj$upSyncFile(funFile,remFunFile)
    }

    funList[[fun.name]] <<- list(fun=fun,wrap=wrap.local)
    invisible(wrap.local)
  }

  updateAndTransfer <- function() {
    # update
    funFiles <- file.path(list.files(funContainerDir,pattern="[_.[:alnum:]]+$",full.names=TRUE),"fun.RData")
    funFiles <- funFiles[file.exists(funFiles)]
    tmpenv <- new.env()
    for (curFile in funFiles) {
      load(curFile,tmpenv)
      createRemoteFunction(tmpenv[["fun"]],fun.name=tmpenv[["funAttrs"]]$fun.name,
                           cache=tmpenv[["funAttrs"]]$cache,transfer=FALSE)
    }
    # transfer
    funDirs <- dirname(funFiles)
    rsyncObj$upSyncDir(funContainerDir,tempdir.rem,
                       other.args=c("--include '*/'",
                                    "--include 'fun.RData'",
                                    "--exclude '*'"))
  }

  uploadPackage <- function(package) {
    dirs <- find.package(package)
    rsyncObj$upSyncDir(dirs,remPackageDir)
  }

  removePackage <- function(package) {
    quotedPackage <- paste0("'",package,"'")
    sshCon$prepare()
    cmdstr <- paste0("cd ",remPackageDir," && ","rm -r ",paste(quotedPackage,collapse=" "))
    sshCon$execBash(cmdstr, retry=TRUE)
    rsyncObj$setSocketFile(sshCon$getSocketFile())
  }

  uploadGlobalSource <- function(text=NULL, file=NULL) {
    stopifnot(is.character(text) || is.character(file))
    while (file.exists(tempf <- file.path(globalDir,paste0("source_",
                                                 paste0(sample(letters,10,replace=TRUE),collapse=""),
                                                 ".R")))) {}
    remtempf <- file.path(remGlobalDir,basename(tempf))
    if (!is.null(text)) {
      writeLines(text,tempf)
    } else {
      file.copy(file,tempf)
    }
    rsyncObj$upSyncFile(tempf,remtempf)
  }

  uploadGlobal <- function(..., list = character(), envir = parent.frame()) {
    while (file.exists(tempf <- file.path(globalDir,
                                          paste0("source_",
                                                 paste0(sample(letters,10,replace=TRUE),collapse=""),
                                                 ".rda")))) {}
    remtempf <- file.path(remGlobalDir,basename(tempf))
    save(...,list=list, envir=envir, file=tempf)
    rsyncObj$upSyncFile(tempf,remtempf)
  }

  removeGlobal <- function() {
    cmdstr <- paste0("cd ",remGlobalDir," && ","rm source_*")
    sshCon$execBash(cmdstr,retry=TRUE)
    rsyncObj$setSocketFile(sshCon$getSocketFile())
    files <- list.files(globalDir,pattern = "^source_[[:alnum:]]+\\.(R|rda)$")
    file.remove(file.path(globalDir,files))
  }

  initCon <- function() {

    # prepare local infrastructure
    if (is.null(tempdir.loc))
      tempdir.loc <<- system("mktemp -d",intern=TRUE)
    dir.create(tempdir.loc, showWarnings = FALSE)
    tempdir.loc <<- normalizePath(tempdir.loc)

    sshDir <- file.path(tempdir.loc,"ssh")
    dir.create(sshDir,showWarnings=FALSE)


    # prepare communication with other computer
    sshCon <<- initInteractiveSSH(login=login,password=password,pwfile=pwfile, port=port, share=TRUE,
                                  tempdir.loc=sshDir, verbosity=verbosity, PS1=PS1, regexPS1=regexPS1,
                                  timeout.con=timeout.con, timeout.cmd=timeout.cmd)
                                  

    rsyncObj <<- initRsync(login=login,pwfile=sshCon$getPwfile(),socket = sshCon$getSocketFile(),
                           tempdir.loc=sshDir, verbosity=verbosity,
                           timeout.con=timeout.con,delay=delay,delay2=delay2)

    # prepare remote infrastructure
    if (is.null(tempdir.rem)) {
      tempdir.rem <<- sshCon$execBash("mktemp -d")$ret
      rsyncObj$setSocketFile(sshCon$getSocketFile())
    } else {
      sshCon$execBash(paste0("mkdir '",tempdir.rem,"'"))
    }

    if (is.null(Rscript))
      Rscript <<- "Rscript"

    funContainerDir <<- file.path(tempdir.loc,"functions")
    globalDir <<- file.path(tempdir.loc,"global")
    remGlobalDir <<- file.path(tempdir.rem,basename(globalDir))
    remFunContainerDir <<- file.path(tempdir.rem,basename(funContainerDir))
    remPackageDir <<- file.path(tempdir.rem,"packages")
    dir.create(globalDir,showWarnings=FALSE)
    dir.create(funContainerDir,showWarnings=FALSE)
    sshCon$execBash(c(paste0("mkdir '",remPackageDir,"'"),
                    paste0("mkdir '",remGlobalDir,"'"),
                    paste0("mkdir '",remFunContainerDir,"'")))
    rsyncObj$setSocketFile(sshCon$getSocketFile())
    wrapFile <<- file.path(tempdir.loc,"wrap.RData")
    remWrapFile <<- file.path(tempdir.rem,basename(wrapFile))

    # remove a warning message about the environment missing
    tempFun <- wrap.remote
    environment(tempFun) <- .GlobalEnv
    saveRDS(tempFun,wrapFile)
    rsyncObj$upSyncFile(wrapFile,remWrapFile)

    # load functions from directories if requested
    if (use.exist) {
      updateAndTransfer()
    }
    TRUE
  }

  closeCon <- function() {
    sshCon$closeCon()
    rsyncObj$closeCon()
  }

  # INITIALIZATION
  initCon()

  # functions that require already initialized connection
  remFileExists <- createRemoteFunction(function(files) {
    stopifnot(is.character(files))
    file.exists(files)
  }, fun.name="remFun_existsFunData", cache=FALSE)

  existsRemFunData <- function(name, files) {
    stopifnot(is.character(name),is.character(files))
    remFunDataDir <- getRemFunDataDir(name)
    remFileExists(file.path(remFunDataDir, files))
  }

  existsFunData <- function(name, files) {
    stopifnot(is.character(name),is.character(files))
    funDataDir <- getFunDataDir(name)
    file.exists(file.path(funDataDir,files))
  }

  # return access functions
  list(createRemoteFunction = createRemoteFunction,
       updateAndTransfer = updateAndTransfer,
       #updateAndTransfer = updateAndTransfer,
       uploadPackage = uploadPackage,
       removePackage = removePackage,
       uploadGlobalSource = uploadGlobalSource,
       uploadGlobal = uploadGlobal,
       removeGlobal = removeGlobal,
       getFun = getFun, listFuns = listFuns, exec = execFun,
       getFunDataDir=getFunDataDir, getRemFunDataDir=getRemFunDataDir,
       getRemGlobalDir=getRemGlobalDir, getRemPackageDir=getRemPackageDir,
       downloadFunData=downloadFunData, uploadFunData=uploadFunData,
       existsRemFunData=existsRemFunData, existsFunData=existsFunData,
       clearFunData=clearFunData,
       closeCon = closeCon,ssh = sshCon,rsync = rsyncObj,
       tempdir.loc = tempdir.loc,tempdir.rem = tempdir.rem, Rscript=Rscript)
}



