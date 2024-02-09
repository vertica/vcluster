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
	"sort"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	SandboxCmd = iota
	StartNodeCommand
	StopDBCmd
	ScrutinizeCmd
	DBAddSubclusterCmd
	InstallPackageCmd
)

type CommandType int

type httpsGetUpNodesOp struct {
	opBase
	opHTTPSBase
	DBName      string
	noUpHostsOk bool
	cmdType     CommandType
}

func makeHTTPSGetUpNodesOp(logger vlog.Printer, dbName string, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string, cmdType CommandType,
) (httpsGetUpNodesOp, error) {
	op := httpsGetUpNodesOp{}
	op.name = "HTTPSGetUpNodesOp"
	op.logger = logger.WithName(op.name)
	op.hosts = hosts
	op.useHTTPPassword = useHTTPPassword
	op.DBName = dbName
	op.cmdType = cmdType

	if useHTTPPassword {
		err := util.ValidateUsernameAndPassword(op.name, useHTTPPassword, userName)
		if err != nil {
			return op, err
		}
		op.userName = userName
		op.httpsPassword = httpsPassword
	}
	return op, nil
}

func (op *httpsGetUpNodesOp) allowNoUpHosts() {
	op.noUpHostsOk = true
}

func (op *httpsGetUpNodesOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.buildHTTPSEndpoint("nodes")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsGetUpNodesOp) prepare(execContext *opEngineExecContext) error {
	execContext.dispatcher.setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsGetUpNodesOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

/* httpsNodeStateResponse example:
   {'details':[]
	'node_list':[{ 'name': 'v_test_db_running_node0001',
	               'node_id':'45035996273704982',
		           'address': '192.168.1.101',
		           'state' : 'UP'
		           'database' : 'test_db',
		           'is_primary' : true,
		           'is_readonly' : false,
		           'catalog_path' : "\/data\/test_db\/v_test_db_node0001_catalog\/Catalog"
		           'subcluster_name' : ''
		           'last_msg_from_node_at':'2023-01-23T15:18:18.44866"
		           'down_since' : null
		           'build_info' : "v12.0.4-7142c8b01f373cc1aa60b1a8feff6c40bfb7afe8"
	}]}
     or an rfc error if the endpoint does not return a well-structured JSON, an example:
    {
    "type": "https:\/\/integrators.vertica.com\/rest\/errors\/unauthorized-request",
    "title": "Unauthorized-request",
    "detail": "Local node has not joined cluster yet, HTTP server will accept connections when the node has joined the cluster\n",
    "host": "0.0.0.0",
    "status": 401
    }
*/

func (op *httpsGetUpNodesOp) processResult(execContext *opEngineExecContext) error {
	var allErrs error
	upHosts := make(map[string]struct{})
	upScInfo := make(map[string]string)
	exceptionHosts := []string{}
	downHosts := []string{}

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
		}

		// We assume all the hosts are in the same db cluster
		// If any of the hosts reject the request, other hosts will reject the request too
		// Do not try other hosts when we see a http failure
		if result.isFailing() && result.isHTTPRunning() {
			exceptionHosts = append(exceptionHosts, host)
			continue
		}

		if !result.isPassing() {
			downHosts = append(downHosts, host)
			continue
		}

		nodesStates := nodesStateInfo{}
		err := op.parseAndCheckResponse(host, result.content, &nodesStates)
		if err != nil {
			err = fmt.Errorf(`[%s] fail to parse result on host %s, details: %w`, op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		// collect all the up hosts
		for _, node := range nodesStates.NodeList {
			if node.Database != op.DBName {
				err = fmt.Errorf(`[%s] database %s is running on host %s, rather than database %s`, op.name, node.Database, host, op.DBName)
				allErrs = errors.Join(allErrs, err)
				break
			}
			if node.State == util.NodeUpState {
				upHosts[node.Address] = struct{}{}
				upScInfo[node.Address] = node.Subcluster
			}
		}
		if len(upHosts) > 0 && op.cmdType != SandboxCmd {
			break
		}
	}

	ignoreErrors := op.processHostLists(upHosts, upScInfo, exceptionHosts, downHosts, execContext)
	if ignoreErrors {
		return nil
	}

	return errors.Join(allErrs, fmt.Errorf("no up nodes detected"))
}

func (op *httpsGetUpNodesOp) finalize(_ *opEngineExecContext) error {
	return nil
}

// processHostLists stashes the up hosts, and if there are no up hosts, prints and logs
// down or erratic hosts.  Additionally, it determines if the op should fail or not.
func (op *httpsGetUpNodesOp) processHostLists(upHosts map[string]struct{}, upScInfo map[string]string,
	exceptionHosts, downHosts []string,
	execContext *opEngineExecContext) (ignoreErrors bool) {
	execContext.upScInfo = upScInfo
	if len(upHosts) > 0 {
		for host := range upHosts {
			execContext.upHosts = append(execContext.upHosts, host)
		}
		// sorting the up hosts will be helpful for picking up the initiator in later instructions
		sort.Strings(execContext.upHosts)
		return true
	}

	if len(exceptionHosts) > 0 {
		op.logger.PrintError(`[%s] fail to call https endpoint of database %s on hosts %s`, op.name, op.DBName, exceptionHosts)
	}

	if len(downHosts) > 0 {
		op.logger.PrintError(`[%s] did not detect database %s running on hosts %v`, op.name, op.DBName, downHosts)
	}

	return op.noUpHostsOk
}
