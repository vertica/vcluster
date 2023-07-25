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
	"strconv"
	"time"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type HTTPSPollNodeStateOp struct {
	OpBase
	OpHTTPBase
	allHosts   map[string]interface{}
	upHosts    map[string]interface{}
	notUpHosts []string
}

func MakeHTTPSPollNodeStateOp(hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string) HTTPSPollNodeStateOp {
	httpsPollNodeStateOp := HTTPSPollNodeStateOp{}
	httpsPollNodeStateOp.name = "HTTPSPollNodeStateOp"
	httpsPollNodeStateOp.hosts = hosts
	httpsPollNodeStateOp.useHTTPPassword = useHTTPPassword

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)
	httpsPollNodeStateOp.userName = userName
	httpsPollNodeStateOp.httpsPassword = httpsPassword

	httpsPollNodeStateOp.upHosts = make(map[string]interface{})
	httpsPollNodeStateOp.allHosts = make(map[string]interface{})
	for _, h := range hosts {
		httpsPollNodeStateOp.allHosts[h] = struct{}{}
	}

	return httpsPollNodeStateOp
}

func (op *HTTPSPollNodeStateOp) setupClusterHTTPRequest(hosts []string) {
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

func (op *HTTPSPollNodeStateOp) Prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *HTTPSPollNodeStateOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPSPollNodeStateOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

func (op *HTTPSPollNodeStateOp) processResult(execContext *OpEngineExecContext) error {
	startTime := time.Now()
	timeoutSecondStr := util.GetEnv("NODE_STATE_POLLING_TIMEOUT", strconv.Itoa(StartupPollingTimeout))
	timeoutSecond, err := strconv.Atoi(timeoutSecondStr)
	if err != nil {
		return fmt.Errorf("invalid timeout value %s: %w", timeoutSecondStr, err)
	}

	duration := time.Duration(timeoutSecond) * time.Second
	count := 0
	for endTime := startTime.Add(duration); ; {
		if time.Now().After(endTime) {
			break
		}

		if count > 0 {
			time.Sleep(PollingInterval * time.Second)
		}

		shouldStopPoll, err := op.shouldStopPolling()
		if err != nil {
			return err
		}

		if shouldStopPoll {
			return nil
		}

		if err := op.execute(execContext); err != nil {
			return err
		}

		count++
	}

	// show the hosts that are not UP
	sort.Strings(op.notUpHosts)
	msg := fmt.Sprintf("The following hosts are not up after %d seconds: %v",
		timeoutSecond, op.notUpHosts)
	vlog.LogPrintError(msg)
	return errors.New(msg)
}

// the following structs only hosts necessary information for this op
type NodeInfo struct {
	Address string `json:"address"`
	// vnode name, e.g., v_dbname_node0001
	Name  string `json:"name"`
	State string `json:"state"`
}

type NodesInfo struct {
	NodeList []NodeInfo `json:"node_list"`
}

func (op *HTTPSPollNodeStateOp) shouldStopPolling() (bool, error) {
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		// VER-88185 vcluster start_db - password related issues
		// We don't need to wait until timeout to determine if all nodes are up or not.
		// If we find the wrong password for the HTTPS service on any hosts, we should fail immediately."
		if result.IsPasswordandCertificateError() {
			vlog.LogPrintError("[%s] Database is UP, but user has provided wrong credentials so unable to perform further operations",
				op.name)
			return false, fmt.Errorf("[%s] wrong password/certificate for https service on host %s",
				op.name, host)
		}
		if result.isPassing() {
			// parse the /nodes endpoint response
			nodesInfo := NodesInfo{}
			err := op.parseAndCheckResponse(host, result.content, &nodesInfo)
			if err != nil {
				vlog.LogPrintError("[%s] fail to parse result on host %s, details: %s",
					op.name, host, err)
				return false, err
			}

			// check whether all nodes are up
			for _, n := range nodesInfo.NodeList {
				if n.State == util.NodeUpState {
					op.upHosts[n.Address] = struct{}{}
				}
			}

			// the HTTPS /nodes endpoint will return the states of all nodes
			// we only need to read info from one responding node
			break
		}
	}

	op.notUpHosts = util.MapKeyDiff(op.allHosts, op.upHosts)
	if len(op.notUpHosts) == 0 {
		vlog.LogPrintInfoln("All nodes are up")
		return true, nil
	}

	return false, nil
}
