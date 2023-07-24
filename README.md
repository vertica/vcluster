# vcluster

[![Go Reference](https://pkg.go.dev/badge/github.com/vertica/vcluster.svg)](https://pkg.go.dev/github.com/vertica/vcluster)

This repository contains the Go library and command-line interface (CLI) to
administer a Vertica cluster with HTTP RESTful interfaces. These interfaces are
exposed by the following services:
- Node Management Agent (NMA)
- Embedded HTTPS service

This CLI tool combines REST calls to provide a coherent Go interface to perform
administrator-level operations, including: creating a database, scaling
up/down, restarting the cluster, and stopping the cluster.

Traditionally, these operations were completed with
[admintools](https://docs.vertica.com/latest/en/admin/using-admin-tools/admin-tools-reference/writing-admin-tools-scripts/).
However, admintools is not suitable for containerized environments because it
relies on SSH for communications and maintains a state file (admintools.conf)
on each Vertica host.

The [VerticaDB operator](https://github.com/vertica/vertica-kubernetes) uses
this library to perform database actions on Vertica on Kubernetes.

## Prerequisites
- [Go version 1.20](https://go.dev/doc/install) and higher


## Repository contents overview

```
vcluster/
├── cmd
│   └── vcluster
│       └── main.go
├── commands
└── vclusterops
    ├── test_data
    ├── util
    └── vlog
```

- `/cmd/vcluster`: The `/cmd` directory contains executable code. The
  `/vcluster` directory contains minimal code that sets up and invokes the
  entry point for the vcluster CLI.
- `/commands`: Code for parsing command line options. These options are
  translated into arguments for the high-level functions in `/vclusterops`.
- `/vclusterops`: Library code for high-level operations such as CreateDB,
  StopDB, and StartDB. It also contains all code for executing the steps that
complete these functions. Code in this library should not depend on other
directories in this project.
  External projects import this library to build custom CLI tools.
- `/vclusterops/testdata`: Contains code and files for testing purposes. By
  convention, the go tool ignores the `/testdata` directory.
  This directory contains a YAML file that defines a simple three-node [Eon Mode cluster](https://docs.vertica.com/latest/en/architecture/eon-concepts/) for testing.
- `/vclusterops/util`: Code that is used by more than one library in this
  project and does not fit logically into an existing package.
- `/vclusterops/vlog`: Sets up a logging utility that writes to
  `/opt/vertica/log/vcluster.log`.
