/*
 (c) Copyright [2023-2024] Open Text.
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
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
)

type httpsPollComputeNodeStateOp struct {
	opBase
	opHTTPSBase
	// Map of compute hosts to be added to whether or not they are the desired status yet
	computeHostStatus map[string]bool
	// The timeout for the entire operation (polling)
	timeout int
	// The timeout for each http request. Requests will be repeated if timeout hasn't been exceeded.
	httpRequestTimeout int
	// poll for nodes down: Set to true if nodes need to be polled to be down
	checkDown bool
}

func makeHTTPSPollComputeNodeStateOpHelper(hosts, computeHosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string) (httpsPollComputeNodeStateOp, error) {
	op := httpsPollComputeNodeStateOp{}
	op.name = "HTTPSPollComputeNodeStateOp"
	op.hosts = hosts // should be 1+ hosts capable of retrieving accurate node states, e.g. primary up hosts
	if len(op.hosts) < 1 {
		return op, errors.New("polling compute node state requires at least one primary up host")
	}
	op.computeHostStatus = make(map[string]bool, len(computeHosts))
	for _, computeHost := range computeHosts {
		op.computeHostStatus[computeHost] = false
	}
	op.useHTTPPassword = useHTTPPassword
	op.httpRequestTimeout = defaultHTTPSRequestTimeoutSeconds
	err := util.ValidateUsernameAndPassword(op.name, useHTTPPassword, userName)
	if err != nil {
		return op, err
	}
	op.userName = userName
	op.httpsPassword = httpsPassword
	return op, nil
}

func makeHTTPSPollComputeNodeStateOp(hosts, computeHosts []string,
	useHTTPPassword bool, userName string,
	httpsPassword *string, timeout int) (httpsPollComputeNodeStateOp, error) {
	op, err := makeHTTPSPollComputeNodeStateOpHelper(hosts, computeHosts, useHTTPPassword, userName, httpsPassword)
	if err != nil {
		return op, err
	}
	if timeout == 0 {
		// using default value
		op.timeout = util.GetEnvInt("NODE_STATE_POLLING_TIMEOUT", StartupPollingTimeout)
	} else {
		op.timeout = timeout
	}
	op.checkDown = false // poll for COMPUTE state (UP equivalent)
	op.description = fmt.Sprintf("Wait for %d compute node(s) to reach COMPUTE state", len(computeHosts))
	return op, err
}

//nolint:unused // for NYI stop node
func makeHTTPSPollComputeNodeStateDownOp(hosts, computeHosts []string,
	useHTTPPassword bool, userName string,
	httpsPassword *string) (httpsPollComputeNodeStateOp, error) {
	op, err := makeHTTPSPollComputeNodeStateOpHelper(hosts, computeHosts, useHTTPPassword, userName, httpsPassword)
	if err != nil {
		return op, err
	}
	op.timeout = util.GetEnvInt("NODE_STATE_POLLING_TIMEOUT", StartupPollingTimeout)
	op.checkDown = true
	op.description = fmt.Sprintf("Wait for %d compute node(s) to go DOWN", len(hosts))
	return op, nil
}

func (op *httpsPollComputeNodeStateOp) getPollingTimeout() int {
	return util.Max(op.timeout, 0)
}

func (op *httpsPollComputeNodeStateOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.Timeout = op.httpRequestTimeout
		httpRequest.buildHTTPSEndpoint(util.NodesEndpoint)
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsPollComputeNodeStateOp) prepare(execContext *opEngineExecContext) error {
	execContext.dispatcher.setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsPollComputeNodeStateOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsPollComputeNodeStateOp) finalize(_ *opEngineExecContext) error {
	return nil
}

func (op *httpsPollComputeNodeStateOp) checkStatusToString() string {
	if op.checkDown {
		return strings.ToLower(util.NodeDownState)
	}
	return "up (compute)"
}

func (op *httpsPollComputeNodeStateOp) getRemainingHostsString() string {
	var remainingHosts []string
	for host, statusOk := range op.computeHostStatus {
		if statusOk {
			remainingHosts = append(remainingHosts, host)
		}
	}
	return strings.Join(remainingHosts, ",")
}

func (op *httpsPollComputeNodeStateOp) processResult(execContext *opEngineExecContext) error {
	op.logger.PrintInfo("[%s] expecting %d %s host(s)", op.name, len(op.hosts), op.checkStatusToString())

	err := pollState(op, execContext)
	if err != nil {
		// show the hosts that are not COMPUTE or DOWN
		msg := fmt.Sprintf("the hosts [%s] are not in %s state after %d seconds, details: %s",
			op.getRemainingHostsString(), op.checkStatusToString(), op.timeout, err)
		op.logger.PrintError(msg)
		return errors.New(msg)
	}
	return nil
}

func (op *httpsPollComputeNodeStateOp) shouldStopPolling() (bool, error) {
	if op.checkDown {
		return op.shouldStopPollingForDown()
	}

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		// when we get timeout error, we know that the host is unreachable/dead
		if result.isTimeout() {
			return true, fmt.Errorf("[%s] cannot connect to host %s, please check if the host is still alive", op.name, host)
		}

		// We don't need to wait until timeout to determine if all nodes are up or not.
		// If we find the wrong password for the HTTPS service on any hosts, we should fail immediately.
		// We also need to let user know to wait until all nodes are up
		if result.isPasswordAndCertificateError(op.logger) {
			op.logger.PrintError("[%s] The credentials are incorrect. 'Catalog Sync' will not be executed.",
				op.name)
			return false, makePollNodeStateAuthenticationError(op.name, host)
		}
		if result.isPassing() {
			// parse the /nodes endpoint response for all nodes, then look for the new ones
			nodesInformation := nodesInfo{}
			err := op.parseAndCheckResponse(host, result.content, &nodesInformation)
			if err != nil {
				op.logger.PrintError("[%s] fail to parse result on host %s, details: %s",
					op.name, host, err)
				return true, err
			}

			// check which nodes have COMPUTE status
			upNodeCount := 0
			for _, nodeInfo := range nodesInformation.NodeList {
				_, ok := op.computeHostStatus[nodeInfo.Address]
				if !ok {
					// skip unrelated nodes
					continue
				}
				if nodeInfo.State == util.NodeComputeState {
					upNodeCount++
					op.computeHostStatus[nodeInfo.Address] = true
				} else {
					// it would be weird for a previously COMPUTE node to change status while we're still
					// polling, but no reason not to use the updated value in case it differs.
					op.computeHostStatus[nodeInfo.Address] = false
				}
			}
			if upNodeCount == len(op.computeHostStatus) {
				op.logger.PrintInfo("[%s] All nodes are %s", op.name, op.checkStatusToString())
				op.updateSpinnerStopMessage("all nodes are %s", op.checkStatusToString())
				return true, nil
			}
			// try the next host's result
			op.logger.PrintInfo("[%s] %d host(s) up (compute)", op.name, upNodeCount)
			op.updateSpinnerMessage("%d host(s) up (compute), expecting %d up (compute) host(s)", upNodeCount, len(op.computeHostStatus))
		}
	}
	// no host returned all new compute nodes as status COMPUTE, so keep polling
	return false, nil
}

func (op *httpsPollComputeNodeStateOp) shouldStopPollingForDown() (bool, error) {
	// for NYI stop node
	return true, fmt.Errorf("NYI")
}
