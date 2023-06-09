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
	"fmt"
	"strconv"
	"time"

	"vertica.com/vcluster/vclusterops/util"
	"vertica.com/vcluster/vclusterops/vlog"
)

const (
	OneSecond             = 1
	OneMinute             = 60 * OneSecond
	StopDBTimeout         = 5 * OneMinute
	StartupPollingTimeout = 5 * OneMinute
	PollingInterval       = 5 * OneSecond
)

type OpType int

const (
	CreateDB OpType = iota
	StopDB
)

func (op OpType) String() string {
	switch op {
	case CreateDB:
		return "Create DB"
	case StopDB:
		return "Stop DB"
	}
	return "unknown operation"
}

type HTTPCheckRunningDBOp struct {
	OpBase
	OpHTTPBase
	opType OpType
}

func MakeHTTPCheckRunningDBOp(name string, hosts []string,
	useHTTPPassword bool, userName string,
	httpsPassword *string, opType OpType) HTTPCheckRunningDBOp {
	runningDBChecker := HTTPCheckRunningDBOp{}
	runningDBChecker.name = name
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

func (op *HTTPCheckRunningDBOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
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
*/
//
// or a message if the endpoint doesn't return a well-structured JSON, an example:
// {'message': 'Local node has not joined cluster yet, HTTP server will accept connections when the node has joined the cluster\n'}
type HTTPNodeStateResponse map[string][]map[string]string

func (op *HTTPCheckRunningDBOp) isDBRunningOnHost(host string,
	responseObj HTTPNodeStateResponse) (running bool, msg string, err error) {
	// parse and log the results
	msg = ""
	nodeList, ok := responseObj["node_list"]
	if !ok {
		// hanging HTTPS service thread
		if op.opType == CreateDB {
			msg = fmt.Sprintf("[%s] Detected HTTPS service running on host %s, please stop the HTTPS service before creating a new database",
				op.name, host)
		} else if op.opType == StopDB {
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

func (op *HTTPCheckRunningDBOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
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
		if !result.isPassing() {
			resSummaryStr = FailureResult
		}
		vlog.LogPrintInfo("[%s] result from host %s summary %s, details %+v.\n",
			op.name, host, resSummaryStr, result)
		if result.isFailing() {
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
			vlog.LogError("[%s] error happened parsing the response of host %s, error info: %s",
				op.name, host, err.Error())
			msg = result.content
			continue
		}
		dbRunning, checkMsg, errCheckDBRunning := op.isDBRunningOnHost(host, responseObj)
		if errCheckDBRunning != nil {
			vlog.LogError("[%s] error happened during checking DB running on host %s, details: %s",
				op.name, host, errCheckDBRunning.Error())
			return MakeClusterOpResultFail()
		}
		vlog.LogInfo("[%s] DB running on host %s: %s, detail: %s", op.name, host, dbRunning, checkMsg)
		// return at least one check msg to user
		msg = checkMsg
	}

	// log info
	vlog.LogInfo("[%s] check db running results: up hosts %v; down hosts %v; hosts with status unknown %v",
		op.name, upHosts, downHosts, exceptionHosts)
	// DB is running
	if len(upHosts) == 0 {
		return MakeClusterOpResultPass()
	}

	vlog.LogPrintInfoln(msg)
	if op.opType == CreateDB {
		vlog.LogPrintInfoln("Aborting database creation")
	} else if op.opType == StopDB {
		vlog.LogPrintInfoln("The database has not been down yet")
	}
	return MakeClusterOpResultFail()
}

func (op *HTTPCheckRunningDBOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if op.opType == CreateDB {
		vlog.LogInfo("[%s] Execute() for operation %s", op.name, op.opType)
		return op.checkDBConnection(execContext)
	}
	return op.pollForDBDown(execContext)
}

func (op *HTTPCheckRunningDBOp) pollForDBDown(execContext *OpEngineExecContext) ClusterOpResult {
	// start the polling
	startTime := time.Now()
	// for tests
	timeoutSecondStr := util.GetEnv("NODE_STATE_POLLING_TIMEOUT", strconv.Itoa(StopDBTimeout))
	timeoutSecond, err := strconv.Atoi(timeoutSecondStr)
	if err != nil {
		vlog.LogPrintError("invalid timeout value %s", timeoutSecondStr)
		return MakeClusterOpResultFail()
	}

	// do not poll, just return succeed
	if timeoutSecond <= 0 {
		return MakeClusterOpResultPass()
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
			vlog.LogError("Fail to dispatch request %v", op.clusterHTTPRequest)
			return MakeClusterOpResultException()
		}
		result := op.processResult(execContext)
		// db not UP, return
		if result.status == SUCCESS {
			return result
		}
		count++
	}
	// timeout
	vlog.LogPrintWarning("the DB is still up after %s seconds", timeoutSecondStr)
	return MakeClusterOpResultFail()
}

func (op *HTTPCheckRunningDBOp) checkDBConnection(execContext *OpEngineExecContext) ClusterOpResult {
	err := execContext.dispatcher.sendRequest(&op.clusterHTTPRequest)
	if err != nil {
		vlog.LogError("Fail to dispatch request %v", op.clusterHTTPRequest)
		return MakeClusterOpResultException()
	}
	return op.processResult(execContext)
}

func (op *HTTPCheckRunningDBOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}
