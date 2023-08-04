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

type HTTPSGetUpNodesOp struct {
	OpBase
	OpHTTPBase
	DBName string
}

func MakeHTTPSGetUpNodesOp(opName string, dbName string, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string) HTTPSGetUpNodesOp {
	httpsGetUpNodesOp := HTTPSGetUpNodesOp{}
	httpsGetUpNodesOp.name = opName
	httpsGetUpNodesOp.hosts = hosts
	httpsGetUpNodesOp.useHTTPPassword = useHTTPPassword
	httpsGetUpNodesOp.DBName = dbName

	if useHTTPPassword {
		util.ValidateUsernameAndPassword(useHTTPPassword, userName)
		httpsGetUpNodesOp.userName = userName
		httpsGetUpNodesOp.httpsPassword = httpsPassword
	}
	return httpsGetUpNodesOp
}

func (op *HTTPSGetUpNodesOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildHTTPSEndpoint("nodes")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPSGetUpNodesOp) Prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *HTTPSGetUpNodesOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

/* HTTPNodeStateResponse example:
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
	or a message if the endpoint does not return a well-structured JSON, an example:
	{'message': 'Local node has not joined cluster yet, HTTP server will accept connections when the node has joined the cluster\n'}
*/

func (op *HTTPSGetUpNodesOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error
	// golang does not have set data structure, use a map to simulate it
	upHosts := make(map[string]struct{})
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
		if result.isFailing() && result.IsHTTPRunning() {
			exceptionHosts = append(exceptionHosts, host)
			continue
		}

		if !result.isPassing() {
			downHosts = append(downHosts, host)
			continue
		}

		nodesStateInfo := NodesStateInfo{}
		err := op.parseAndCheckResponse(host, result.content, &nodesStateInfo)
		if err != nil {
			err = fmt.Errorf(`[%s] fail to parse result on host %s, details: %w`, op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		// collect all the up hosts
		for _, node := range nodesStateInfo.NodeList {
			if node.Database != op.DBName {
				err = fmt.Errorf(`[%s] database %s is running on host %s, rather than database %s`, op.name, node.Database, host, op.DBName)
				allErrs = errors.Join(allErrs, err)
				break
			}
			if node.State == util.NodeUpState {
				upHosts[node.Address] = struct{}{}
			}
		}

		if len(upHosts) > 0 {
			break
		}
	}

	if len(upHosts) > 0 {
		for host := range upHosts {
			execContext.upHosts = append(execContext.upHosts, host)
		}
		// sorting the up hosts will be helpful for picking up the initiator in later instructions
		sort.Strings(execContext.upHosts)
		return nil
	}

	if len(exceptionHosts) > 0 {
		vlog.LogPrintError(`[%s] fail to call https endpoint of database %s on hosts %s`, op.name, op.DBName, exceptionHosts)
	}

	if len(downHosts) > 0 {
		vlog.LogPrintError(`[%s] did not detect database %s running on hosts %v`, op.name, op.DBName, downHosts)
	}

	return errors.Join(allErrs, fmt.Errorf("no up nodes detected"))
}

func (op *HTTPSGetUpNodesOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}
