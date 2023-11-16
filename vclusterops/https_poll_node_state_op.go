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
	"strconv"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// Timeout set to 30 seconds for each GET /v1/nodes/{node} call.
// 30 seconds is long enough for normal http request.
// If this timeout is reached, it might imply that the target IP is unreachable
const httpRequestTimeoutSeconds = 30
const (
	StartDBCmd CmdType = iota
	RestartNodeCmd
)

type CmdType int

func (cmd CmdType) String() string {
	switch cmd {
	case StartDBCmd:
		return "start_db"
	case RestartNodeCmd:
		return "restart_node"
	}
	return "unknown_operation"
}

type HTTPSPollNodeStateOp struct {
	OpBase
	OpHTTPSBase
	currentHost string
	timeout     int
	cmdType     CmdType
}

func makeHTTPSPollNodeStateOpHelper(logger vlog.Printer, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string) (HTTPSPollNodeStateOp, error) {
	httpsPollNodeStateOp := HTTPSPollNodeStateOp{}
	httpsPollNodeStateOp.name = "HTTPSPollNodeStateOp"
	httpsPollNodeStateOp.logger = logger.WithName(httpsPollNodeStateOp.name)
	httpsPollNodeStateOp.hosts = hosts
	httpsPollNodeStateOp.useHTTPPassword = useHTTPPassword

	err := util.ValidateUsernameAndPassword(httpsPollNodeStateOp.name, useHTTPPassword, userName)
	if err != nil {
		return httpsPollNodeStateOp, err
	}
	httpsPollNodeStateOp.userName = userName
	httpsPollNodeStateOp.httpsPassword = httpsPassword

	return httpsPollNodeStateOp, nil
}

func makeHTTPSPollNodeStateOpWithTimeoutAndCommand(logger vlog.Printer, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string,
	timeout int, cmdType CmdType) (HTTPSPollNodeStateOp, error) {
	op, err := makeHTTPSPollNodeStateOpHelper(logger, hosts, useHTTPPassword, userName, httpsPassword)
	if err != nil {
		return op, err
	}
	op.timeout = timeout
	op.cmdType = cmdType
	return op, nil
}

func makeHTTPSPollNodeStateOp(logger vlog.Printer, hosts []string,
	useHTTPPassword bool, userName string,
	httpsPassword *string) (HTTPSPollNodeStateOp, error) {
	httpsPollNodeStateOp, err := makeHTTPSPollNodeStateOpHelper(logger, hosts, useHTTPPassword, userName, httpsPassword)
	if err != nil {
		return httpsPollNodeStateOp, err
	}
	timeoutSecondStr := util.GetEnv("NODE_STATE_POLLING_TIMEOUT", strconv.Itoa(StartupPollingTimeout))
	timeoutSecond, err := strconv.Atoi(timeoutSecondStr)
	if err != nil {
		return HTTPSPollNodeStateOp{}, err
	}
	httpsPollNodeStateOp.timeout = timeoutSecond
	return httpsPollNodeStateOp, nil
}

func (op *HTTPSPollNodeStateOp) getPollingTimeout() int {
	return op.timeout
}

func (op *HTTPSPollNodeStateOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.Timeout = httpRequestTimeoutSeconds
		httpRequest.buildHTTPSEndpoint("nodes/" + host)
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *HTTPSPollNodeStateOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *HTTPSPollNodeStateOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPSPollNodeStateOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

func (op *HTTPSPollNodeStateOp) processResult(execContext *OpEngineExecContext) error {
	vlog.PrintWithIndent("[%s] expecting %d up host(s)", op.name, len(op.hosts))

	err := pollState(op, execContext)
	if err != nil {
		// show the host that is not UP
		msg := fmt.Sprintf("Cannot get the correct response from the host %s after %d seconds, details: %s",
			op.currentHost, op.timeout, err)
		op.logger.PrintError(msg)
		return errors.New(msg)
	}
	return nil
}

// the following structs only hosts necessary information for this op
type NodeInfo struct {
	Address string `json:"address"`
	// vnode name, e.g., v_dbname_node0001
	Name        string `json:"name"`
	State       string `json:"state"`
	CatalogPath string `json:"catalog_path"`
}

type NodesInfo struct {
	NodeList []NodeInfo `json:"node_list"`
}

func (op *HTTPSPollNodeStateOp) shouldStopPolling() (bool, error) {
	upNodeCount := 0

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.currentHost = host

		// when we get timeout error, we know that the host is unreachable/dead
		if result.isTimeout() {
			return true, fmt.Errorf("[%s] cannot connect to host %s, please check if the host is still alive", op.name, host)
		}

		// VER-88185 vcluster start_db - password related issues
		// We don't need to wait until timeout to determine if all nodes are up or not.
		// If we find the wrong password for the HTTPS service on any hosts, we should fail immediately.
		// We also need to let user know to wait until all nodes are up
		if result.isPasswordAndCertificateError(op.logger) {
			switch op.cmdType {
			case StartDBCmd, RestartNodeCmd:
				op.logger.PrintError("[%s] The credentials are incorrect. 'Catalog Sync' will not be executed.",
					op.name)
				return true, fmt.Errorf("[%s] wrong password/certificate for https service on host %s, but the nodes' startup have been in progress."+
					"Please use vsql to check the nodes' status and manually run sync_catalog vsql command 'select sync_catalog()'", op.name, host)
			}
			return true, fmt.Errorf("[%s] wrong password/certificate for https service on host %s",
				op.name, host)
		}
		if result.isPassing() {
			// parse the /nodes/{node} endpoint response
			nodesInfo := NodesInfo{}
			err := op.parseAndCheckResponse(host, result.content, &nodesInfo)
			if err != nil {
				op.logger.PrintError("[%s] fail to parse result on host %s, details: %s",
					op.name, host, err)
				return true, err
			}

			// check whether the node is up
			// the node list should only have one node info
			if len(nodesInfo.NodeList) == 1 {
				nodeInfo := nodesInfo.NodeList[0]
				if nodeInfo.State == util.NodeUpState {
					upNodeCount++
				}
			} else {
				// if NMA endpoint cannot function well on any of the hosts, we do not want to retry polling
				return true, fmt.Errorf("[%s] expect one node's information, but got %d nodes' information"+
					" from NMA /v1/nodes/{node} endpoint on host %s",
					op.name, len(nodesInfo.NodeList), host)
			}
		}
	}

	if upNodeCount < len(op.hosts) {
		vlog.PrintWithIndent("[%s] %d host(s) up", op.name, upNodeCount)
		return false, nil
	}

	vlog.PrintWithIndent("[%s] All nodes are up", op.name)

	return true, nil
}
