# Project structure

```
vcluster/
  |
  +---- cmd
  +---- vendor
  +---- vclusterops
  +---- commands
  +---- vlog
  +---- vconfig

CLI Structure

       vcluster
       /      \
      /        \
  vconfig --- commands ---- vlog
                |
               vclusterops

```

## cmd
This directory contains code for launching the command line program, vcluster.
There should be minimal code here to setup and invoke the main entry point
for the CLI program. Most of the code should be the other directories
within the project.

## vendor
This is the standard directory for third-party libraries. The directory
is managed by the Go module management system. Vertica code shouldn't
be put in this directory.

## vclusterops
This directory contains the library code for providing high-level cluster
operation functions, like CreateDB, StopDB, StartDB. It also contains
all of the code for executing the steps to complete those functions.
We expect that other projects will want to use this library without
using the other pieces of this project.

### vclusterops: no dependencies
Code in vclusterops should not depend on code in commands, vconfig, or vlog.

### vclusterops: confluence
https://confluence.verticacorp.com/display/~jslaunwhite/Cluster+Operations+High+Level+Functions+Blueprint

## commands
This directory contains code for parsing command line options. The options
from the command line are translated into arguments for the high-level
function in vclusterops.

## vconfig
This directory contains code for reading/writing a configuration file and
turning it into structured data types. The commands layer consumes the structured
types to make calls to the high-level opertion functions. The commands layer
produces updated structured types to be written back by functions in vconfig

## vlog
This directory contains utilities for setting up logging. The logging
will be used by commands and vconfig
