# Introduction

This repository contains the Go library and CLI to administer a vertica cluster using various HTTP RESTful interfaces. These interfaces are exposed in two services: node managemend agent and the embedded https service that runs in the vertica server. This will stitch together the REST calls to provide a coherent Go interface to do admin level operations such as: creating a database, scaling up/down, and restarting ad stoping the cluster.

Tradionally, the kind of commands have been available through admintools. However, the architecture of this application is not suitable for running a containerized environment. The chief complaint is the requirement on using SSH for communication and a state file (admintools.conf) that must be preserved and kept in-sync at each vertica host.

The first consumer of this library is the [vertica operator](https://github.com/vertica/vertica-kubernetes).

# Project structure

```
vcluster/
  |
  +---- cmd
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

## vclusterops
This directory contains the library code for providing high-level cluster
operation functions, like CreateDB, StopDB, StartDB. It also contains
all of the code for executing the steps to complete those functions.
We expect that other projects will want to use this library without
using the other pieces of this project.

### vclusterops: no dependencies
Code in vclusterops should not depend on code in commands, vconfig, or vlog.

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
