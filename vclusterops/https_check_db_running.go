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
	"time"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	OneSecond             = 1
	OneMinute             = 60 * OneSecond
	StopDBTimeout         = 5 * OneMinute
	StartupPollingTimeout = 5 * OneMinute
	PollingInterval       = 3 * OneSecond
)

type OpType int

const (
	CreateDB OpType = iota
	StopDB
	StartDB
)

func (op OpType) String() string {
	switch op {
	case CreateDB:
		return "Create DB"
	case StopDB:
		return "Stop DB"
	case StartDB:
		return "Start DB"
	}
	return "unknown operation"
}

type HTTPCheckRunningDBOp struct {
	OpBase
	OpHTTPBase
	opType OpType
}

func MakeHTTPCheckRunningDBOp(opName string, hosts []string,
	useHTTPPassword bool, userName string,
	httpsPassword *string, opType OpType) HTTPCheckRunningDBOp {
	runningDBChecker := HTTPCheckRunningDBOp{}
	runningDBChecker.name = opName
	runningDBChecker.hosts = hosts
	runningDBChecker.useHTTPPassword = useHTTPPassword

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)
	runningDBChecker.userName = userName
	runningDBChecker.httpsPassword = httpsPassword
	runningDBChecker.opType = opType
	return runningDBChecker
}

func (op *HTTPCheckRunningDBOp) setupClusterHTTPRequest(hosts []string) {
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

func (op *HTTPCheckRunningDBOp) logPrepare() {
	vlog.LogInfo("[%s] Prepare() called for operation %s \n", op.name, op.opType)
}

func (op *HTTPCheckRunningDBOp) Prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

/* HTTPNodeStateResponse example:
   {'details':[]
    'node_list':[{'name': 'v_test_db_running_node0001',
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
*/
//
// or a message if the endpoint doesn't return a well-structured JSON, examples:
// {'message': 'Local node has not joined cluster yet, HTTP server will accept connections when the node has joined the cluster\n'}
// {"message": "Wrong password\n"}
type HTTPNodeStateResponse map[string][]map[string]string

func (op *HTTPCheckRunningDBOp) isDBRunningOnHost(host string,
	responseObj HTTPNodeStateResponse) (running bool, msg string, err error) {
	// parse and log the results
	msg = ""
	nodeList, ok := responseObj["node_list"]
	if !ok {
		// hanging HTTPS service thread
		switch op.opType {
		case CreateDB:
			msg = fmt.Sprintf("[%s] Detected HTTPS service running on host %s, please stop the HTTPS service before creating a new database",
				op.name, host)
		case StopDB, StartDB:
			msg = fmt.Sprintf("[%s] Detected HTTPS service running on host %s", op.name, host)
		}
		return false, msg, nil
	}
	// exception, panic out loudly
	if len(nodeList) == 0 {
		noNodeErr := fmt.Errorf("[%s] Unexpected result from host %s: empty node_list obtained from /nodes endpoint response",
			op.name, host)
		return true, "", noNodeErr
	}

	nodeInfo := nodeList[0]
	runningDBName, ok := nodeInfo["database"]
	// exception, panic out
	if !ok {
		noDBInfoErr := fmt.Errorf("[%s] Unexpected result from host %s: no database name returned from /nodes endpoint response", op.name, host)
		return true, "", noDBInfoErr
	}

	msg = fmt.Sprintf("[%s] Database %s is still running on host %s", op.name, runningDBName, host)
	return true, msg, nil
}

// processResult will look at all of the results that come back from the hosts.
// We don't return an error if all of the nodes are down. Otherwise, an error is
// returned.
func (op *HTTPCheckRunningDBOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error
	// golang doesn't have set data structure,
	// so use maps for caching distinct up and down hosts
	// we have this list of hosts for better debugging info
	upHosts := make(map[string]bool)
	downHosts := make(map[string]bool)
	exceptionHosts := make(map[string]bool)
	// print msg
	msg := ""
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		resSummaryStr := SuccessResult
		// VER-87303: it's possible that there's a DB running with a different password
		if !result.IsHTTPRunning() {
			resSummaryStr = FailureResult
		}
		vlog.LogPrintInfo("[%s] result from host %s summary %s, details: %+v.",
			op.name, host, resSummaryStr, result)

		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
		}
		if result.isFailing() && !result.IsHTTPRunning() {
			downHosts[host] = true
			continue
		} else if result.isException() {
			exceptionHosts[host] = true
			continue
		}

		upHosts[host] = true
		// a passing result means that the db isn't down
		var responseObj HTTPNodeStateResponse
		err := op.parseAndCheckResponse(host, result.content, &responseObj)

		// don't return, as an error here could just mean a node not being up
		if err != nil {
			err = fmt.Errorf("[%s] error happened parsing the response of host %s, error info: %w",
				op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			msg = result.content
			continue
		}
		dbRunning, checkMsg, err := op.isDBRunningOnHost(host, responseObj)
		if err != nil {
			return fmt.Errorf("[%s] error happened during checking DB running on host %s, details: %w",
				op.name, host, err)
		}
		vlog.LogInfo("[%s] DB running on host %s: %s, detail: %s", op.name, host, dbRunning, checkMsg)
		// return at least one check msg to user
		msg = checkMsg
	}

	// log info
	vlog.LogInfo("[%s] check db running results: up hosts %v; down hosts %v; hosts with status unknown %v",
		op.name, upHosts, downHosts, exceptionHosts)
	// no DB is running on hosts, return a passed result
	if len(upHosts) == 0 {
		return nil
	}

	vlog.LogPrintInfoln(msg)

	switch op.opType {
	case CreateDB:
		vlog.LogPrintInfoln("Aborting database creation")
	case StopDB, StartDB:
		vlog.LogPrintInfoln("The database has not been down yet")
	}
	return allErrs
}

func (op *HTTPCheckRunningDBOp) Execute(execContext *OpEngineExecContext) error {
	if op.opType == CreateDB {
		vlog.LogInfo("[%s] Execute() for operation %s", op.name, op.opType)
		return op.checkDBConnection(execContext)
	}
	return op.pollForDBDown(execContext)
}

func (op *HTTPCheckRunningDBOp) pollForDBDown(execContext *OpEngineExecContext) error {
	// start the polling
	startTime := time.Now()
	// for tests
	timeoutSecondStr := util.GetEnv("NODE_STATE_POLLING_TIMEOUT", strconv.Itoa(StopDBTimeout))
	timeoutSecond, err := strconv.Atoi(timeoutSecondStr)
	if err != nil {
		return fmt.Errorf("invalid timeout value %s: %w", timeoutSecondStr, err)
	}

	// do not poll, just return succeed
	if timeoutSecond <= 0 {
		return nil
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
		err = execContext.dispatcher.sendRequest(&op.clusterHTTPRequest)
		if err != nil {
			return fmt.Errorf("fail to dispatch request %v: %w", op.clusterHTTPRequest, err)
		}
		err = op.processResult(execContext)
		// If we get an error, intentionally eat the error so that we send the
		// request again. We are waiting for all nodes to be down, which is a
		// success result from processContext.
		if err != nil {
			vlog.LogInfo("[%s] failure when checking node status: %s", op.name, err)
		} else {
			return nil
		}
		count++
	}
	// timeout
	msg := fmt.Sprintf("the DB is still up after %s seconds", timeoutSecondStr)
	vlog.LogPrintWarning(msg)
	return errors.New(msg)
}

func (op *HTTPCheckRunningDBOp) checkDBConnection(execContext *OpEngineExecContext) error {
	err := execContext.dispatcher.sendRequest(&op.clusterHTTPRequest)
	if err != nil {
		return fmt.Errorf("fail to dispatch request %v: %w", op.clusterHTTPRequest, err)
	}
	return op.processResult(execContext)
}

func (op *HTTPCheckRunningDBOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}
