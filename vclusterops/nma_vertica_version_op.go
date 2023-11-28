/*
 (c) Copyright [2023] Open Text.
 Licensed under the Apache License, Version 2.0 (the "License");
 You may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package vclusterops

import (
	"errors"
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	NoVersion = "NO_VERSION"
	DefaultSC = "default_subcluster"
)

type hostVersionMap map[string]string

type nmaVerticaVersionOp struct {
	opBase
	IsEon              bool
	RequireSameVersion bool
	HasIncomingSCNames bool
	SCToHostVersionMap map[string]hostVersionMap
	vdb                *VCoordinationDatabase
}

func makeHostVersionMap() hostVersionMap {
	return make(hostVersionMap)
}

func makeSCToHostVersionMap() map[string]hostVersionMap {
	return make(map[string]hostVersionMap)
}

// makeNMAVerticaVersionOp is used when db has not been created
func makeNMAVerticaVersionOp(logger vlog.Printer, hosts []string, sameVersion, isEon bool) nmaVerticaVersionOp {
	op := nmaVerticaVersionOp{}
	op.name = "NMAVerticaVersionOp"
	op.logger = logger.WithName(op.name)
	op.hosts = hosts
	op.RequireSameVersion = sameVersion
	op.IsEon = isEon
	op.SCToHostVersionMap = makeSCToHostVersionMap()
	return op
}

// makeNMAVerticaVersionOpWithoutHosts is used when db is down
func makeNMAVerticaVersionOpWithoutHosts(logger vlog.Printer, sameVersion bool) nmaVerticaVersionOp {
	// We set hosts to nil and isEon to false temporarily, and they will get the correct value from execute context in prepare()
	return makeNMAVerticaVersionOp(logger, nil /*hosts*/, sameVersion, false /*isEon*/)
}

// makeNMAVerticaVersionOpWithVDB is used when db is up
func makeNMAVerticaVersionOpWithVDB(logger vlog.Printer, sameVersion bool, vdb *VCoordinationDatabase) nmaVerticaVersionOp {
	// We set hosts to nil temporarily, and it will get the correct value from vdb in prepare()
	op := makeNMAVerticaVersionOp(logger, nil /*hosts*/, sameVersion, vdb.IsEon)
	op.vdb = vdb
	return op
}

func (op *nmaVerticaVersionOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.buildNMAEndpoint("vertica/version")
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *nmaVerticaVersionOp) prepare(execContext *opEngineExecContext) error {
	/*
		 *	 Initialize SCToHostVersionMap in three cases:
		 *	 - when db is up, we initialize SCToHostVersionMap using vdb content (from Vertica https service)
		 *   - when db is down, we initialize SCToHostVersionMap using nmaVDatabase (from NMA /catalog/database) in execute context
		 *   - when db has not been created, we initialize SCToHostVersionMap using op.hosts (from user input)
		 *   An example of initialized SCToHostVersionMap:
		    {
				"default_subcluster" : {"192.168.0.101": "", "192.168.0.102": ""},
				"subcluster1" : {"192.168.0.103": "", "192.168.0.104": ""},
				"subcluster2" : {"192.168.0.105": "", "192.168.0.106": ""},
			}
		 *
	*/
	if len(op.hosts) == 0 {
		if op.vdb != nil {
			// db is up
			op.HasIncomingSCNames = true
			for host, vnode := range op.vdb.HostNodeMap {
				op.hosts = append(op.hosts, host)
				sc := vnode.Subcluster
				// Update subcluster of new nodes that will be assigned to default subcluster.
				// When we created vdb in db_add_node without specifying subcluster, we did not know the default subcluster name
				// so new nodes is using "" as their subclusters. Below line will correct node nodes' subclusters.
				if op.vdb.IsEon && sc == "" && execContext.defaultSCName != "" {
					op.vdb.HostNodeMap[host].Subcluster = execContext.defaultSCName
					sc = execContext.defaultSCName
				}

				// initialize the SCToHostVersionMap with empty versions
				if op.SCToHostVersionMap[sc] == nil {
					op.SCToHostVersionMap[sc] = makeHostVersionMap()
				}
				op.SCToHostVersionMap[sc][host] = ""
			}
		} else {
			// db is down
			op.HasIncomingSCNames = true
			if execContext.nmaVDatabase.CommunalStorageLocation != "" {
				op.IsEon = true
			}
			for host, vnode := range execContext.nmaVDatabase.HostNodeMap {
				op.hosts = append(op.hosts, host)
				// initialize the SCToHostVersionMap with empty versions
				sc := vnode.Subcluster.Name
				if op.SCToHostVersionMap[sc] == nil {
					op.SCToHostVersionMap[sc] = makeHostVersionMap()
				}
				op.SCToHostVersionMap[sc][host] = ""
			}
		}
	} else {
		// When creating a db, the subclusters of all nodes will be the same so set it to a fixed value.
		sc := DefaultSC
		// initialize the SCToHostVersionMap with empty versions
		op.SCToHostVersionMap[sc] = makeHostVersionMap()
		for _, host := range op.hosts {
			op.SCToHostVersionMap[sc][host] = ""
		}
	}

	execContext.dispatcher.setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *nmaVerticaVersionOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *nmaVerticaVersionOp) finalize(_ *opEngineExecContext) error {
	return nil
}

type nmaVerticaVersionOpResponse map[string]string

func (op *nmaVerticaVersionOp) parseAndCheckResponse(host, resultContent string) error {
	// each result is a pair {"vertica_version": <vertica version string>}
	// example result:
	// {"vertica_version": "Vertica Analytic Database v12.0.3"}
	var responseObj nmaVerticaVersionOpResponse
	err := util.GetJSONLogErrors(resultContent, &responseObj, op.name, op.logger)
	if err != nil {
		return err
	}

	version, ok := responseObj["vertica_version"]
	// missing key "vertica_version"
	if !ok {
		return errors.New("Unable to get vertica version from host " + host)
	}

	op.logger.Info("JSON response", "host", host, "responseObj", responseObj)
	// update version for the host in SCToHostVersionMap
	for sc, hostVersionMap := range op.SCToHostVersionMap {
		if _, exists := hostVersionMap[host]; exists {
			op.SCToHostVersionMap[sc][host] = version
		}
	}
	return nil
}

func (op *nmaVerticaVersionOp) logResponseCollectVersions() error {
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.isPassing() {
			errStr := fmt.Sprintf("[%s] result from host %s summary %s, details: %+v\n",
				op.name, host, FailureResult, result)
			return errors.New(errStr)
		}

		err := op.parseAndCheckResponse(host, result.content)
		if err != nil {
			return fmt.Errorf("[%s] fail to parse result on host %s, details: %w", op.name, host, err)
		}
	}
	return nil
}

func (op *nmaVerticaVersionOp) logCheckVersionMatch() error {
	/*   An example of SCToHostVersionMap:
	    {
			"default_subcluster" : {"192.168.0.101": "Vertica Analytic Database v24.1.0", "192.168.0.102": "Vertica Analytic Database v24.1.0"},
			"subcluster1" : {"192.168.0.103": "Vertica Analytic Database v24.0.0", "192.168.0.104": "Vertica Analytic Database v24.0.0"},
			"subcluster2" : {"192.168.0.105": "Vertica Analytic Database v24.0.0", "192.168.0.106": "Vertica Analytic Database v24.0.0"},
		}
	*/
	var versionStr string
	for sc, hostVersionMap := range op.SCToHostVersionMap {
		versionStr = NoVersion
		for host, version := range hostVersionMap {
			op.logger.Info("version check", "host", host, "version", version)
			if version == "" {
				if op.IsEon && op.HasIncomingSCNames {
					return fmt.Errorf("[%s] No version collected for host [%s] in subcluster [%s]", op.name, host, sc)
				}
				return fmt.Errorf("[%s] No version collected for host [%s]", op.name, host)
			} else if versionStr == NoVersion {
				// first time seeing a valid version, set it as the versionStr
				versionStr = version
			} else if version != versionStr && op.RequireSameVersion {
				if op.IsEon && op.HasIncomingSCNames {
					return fmt.Errorf("[%s] Found mismatched versions: [%s] and [%s] in subcluster [%s]", op.name, versionStr, version, sc)
				}
				return fmt.Errorf("[%s] Found mismatched versions: [%s] and [%s]", op.name, versionStr, version)
			}
		}
		// no version collected at all
		if versionStr == NoVersion {
			if op.IsEon && op.HasIncomingSCNames {
				return fmt.Errorf("[%s] No version collected for all hosts in subcluster [%s]", op.name, sc)
			}
			return fmt.Errorf("[%s] No version collected for all hosts", op.name)
		}
	}
	return nil
}

func (op *nmaVerticaVersionOp) processResult(_ *opEngineExecContext) error {
	err := op.logResponseCollectVersions()
	if err != nil {
		return err
	}
	return op.logCheckVersionMatch()
}
