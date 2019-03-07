# remoteFunctionSSH - R package 

This package enables the definition of function in a
local R session that can be executed transparently 
on a remote machine over SSH.

## Requirements

This package depends on the R packages 
`interactiveSSH` and `rsyncFacility`,
see section *Installation* how they can be installed.
The package relies on the `ssh` command and in addition
`sshpass` if the password for the SSH login is provided
as an argument to the function `initSSH`.
As the package makes use of named pipes, it works
only under Linux.

## Installation

To install this package and the other two required packages
use the following commands in a terminal:
```
mkdir installDir
cd installDir
git clone https://github.com/gschnabel/interactiveSSH.git
git clone https://github.com/gschnabel/rsyncFacility.git
git clone https://github.com/gschnabel/remoteFunctionSSH.git
R CMD INSTALL interactiveSSH
R CMD INSTALL rsyncFacility
R CMD INSTALL remoteFunctionSSH
```

## Basic usage

First, setup the SSH connection to the remote machine:
```
remFun <- initSSH("user@host", "password", tempdir.loc="tempdir_loc", tempdir.rem="tempdir_rem")
```
Replace `user@host` and `password` by your SSH login credentials.
The argument `tempdir.loc` specifies a directory on the local machine that stores 
connection related information and function definitions.
The argument `tempdir.rem` specifies a directory on the remote machine containing the same information.

A remote function can now be defined by:
```
addFun <- remFun$createRemoteFunction( function(x, y) {
  cat("I am so remote!\n")
  x+y
}, fun.name="addFun", show.output=TRUE)
```

The remote function can now be conveniently executed from the local R session:
```
addFun(2,4)
# Output:  
# I am so remote!
# [1] 6
```

The function name as specified by `fun.name` is used create a directory `<tempdir.loc>/function/<fun.name>' and
contains files with the code of the function and potentially the call history of the function.
The name does not have to coincide with the name of the local object associated with the remote function.
For example, the following specification is also completely fine:
```
addRemFun <- remFun$createRemoteFunction( function(x, y) {
  x+y
}, fun.name="addFun")
```

For more information on the creation of remote functions consult the R help page, e.g. by typing `?initSSH` on the R prompt.

