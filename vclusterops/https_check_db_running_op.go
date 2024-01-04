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

type opType int

const (
	CreateDB opType = iota
	StopDB
	StartDB
	ReviveDB
)

func (op opType) String() string {
	switch op {
	case CreateDB:
		return "Create DB"
	case StopDB:
		return "Stop DB"
	case StartDB:
		return "Start DB"
	case ReviveDB:
		return "Revive DB"
	}
	return "unknown operation"
}

// DBIsRunningError is an error to indicate we found the database still running.
// This is emitted from this op. Callers can do type checking to perform an
// action based on the error.
type DBIsRunningError struct {
	Detail string
}

// Error returns the message details. This is added so that it is compatible
// with the error interface.
func (e *DBIsRunningError) Error() string {
	return e.Detail
}

type httpsCheckRunningDBOp struct {
	opBase
	opHTTPSBase
	opType opType
}

func makeHTTPSCheckRunningDBOp(logger vlog.Printer, hosts []string,
	useHTTPPassword bool, userName string,
	httpsPassword *string, operationType opType,
) (httpsCheckRunningDBOp, error) {
	op := httpsCheckRunningDBOp{}
	op.name = "HTTPCheckDBRunningOp"
	op.logger = logger.WithName(op.name)
	op.hosts = hosts
	op.useHTTPPassword = useHTTPPassword

	err := util.ValidateUsernameAndPassword(op.name, useHTTPPassword, userName)
	if err != nil {
		return op, err
	}

	op.userName = userName
	op.httpsPassword = httpsPassword
	op.opType = operationType
	return op, nil
}

func (op *httpsCheckRunningDBOp) setupClusterHTTPRequest(hosts []string) error {
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

func (op *httpsCheckRunningDBOp) logPrepare() {
	op.logger.Info("prepare() called", "opType", op.opType)
}

func (op *httpsCheckRunningDBOp) prepare(execContext *opEngineExecContext) error {
	execContext.dispatcher.setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

/* httpsNodeStateResponse example:
   {
	"details": null,
    "node_list":[{
				  "name": "v_test_db_running_node0001",
	          	  "node_id": 45035996273704982,
		  		  "address": "192.168.1.101",
		  		  "state" : "UP",
		  		  "database" : "test_db",
		  		  "is_primary" : true,
		  		  "is_readonly" : false,
		  		  "catalog_path" : "\/data\/test_db\/v_test_db_node0001_catalog\/Catalog",
		  		  "data_path": ["\/data\/test_db\/v_test_db_node0001_data"],
      	          "depot_path": "\/data\/test_db\/v_test_db_node0001_depot",
		          "subcluster_name" : "default_subcluster",
		          "last_msg_from_node_at":"2023-01-23T15:18:18.44866",
		          "down_since" : null,
		          "build_info" : "v12.0.4-7142c8b01f373cc1aa60b1a8feff6c40bfb7afe8"
                }]
	}
*/
//
// or a message if the endpoint doesn't return a well-structured JSON, examples:
// {"message": "Local node has not joined cluster yet, HTTP server will accept connections when the node has joined the cluster\n"}
// {"message": "Wrong password\n"}
type nodeList []map[string]any
type httpsNodeStateResponse map[string]nodeList

func (op *httpsCheckRunningDBOp) isDBRunningOnHost(host string,
	responseObj httpsNodeStateResponse) (running bool, msg string, err error) {
	// parse and log the results
	msg = ""
	nodeList, ok := responseObj["node_list"]
	if !ok {
		// hanging HTTPS service thread
		switch op.opType {
		case CreateDB:
			msg = fmt.Sprintf("[%s] Detected HTTPS service running on host %s, please stop the HTTPS service before creating a new database",
				op.name, host)
		case StopDB, StartDB, ReviveDB:
			msg = fmt.Sprintf("[%s] Detected HTTPS service running on host %s", op.name, host)
		}
		return false, msg, nil
	}
	// exception, throw an error
	if len(nodeList) == 0 {
		noNodeErr := fmt.Errorf("[%s] Unexpected result from host %s: empty node_list obtained from /nodes endpoint response",
			op.name, host)
		return true, "", noNodeErr
	}

	nodeInfo := nodeList[0]
	runningDBName, ok := nodeInfo["database"]
	// exception, throw an error
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
func (op *httpsCheckRunningDBOp) processResult(_ *opEngineExecContext) error {
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
		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
		}
		if result.isFailing() && !result.isHTTPRunning() {
			downHosts[host] = true
			continue
		} else if result.isException() {
			exceptionHosts[host] = true
			continue
		}

		upHosts[host] = true
		// a passing result means that the db isn't down
		var responseObj httpsNodeStateResponse
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
		op.logger.Info("DB running", "host", host, "dbRunning", dbRunning, "checkMsg", checkMsg)
		// return at least one check msg to user
		msg = checkMsg
	}

	// log info
	op.logger.Info("check db running results", "up hosts", upHosts, "down hosts", downHosts, "hosts with status unknown", exceptionHosts)
	// no DB is running on hosts, return a passed result
	if len(upHosts) == 0 {
		return nil
	}

	op.logger.Info("Check DB running", "detail", msg)

	switch op.opType {
	case CreateDB:
		op.logger.PrintInfo("Aborting database creation")
	case StopDB:
		op.logger.PrintInfo("The database has not been down yet")
	case StartDB:
		op.logger.PrintInfo("Aborting database start")
	case ReviveDB:
		op.logger.PrintInfo("Aborting database revival")
	}

	// when db is running, append an error to allErrs for stopping VClusterOpEngine
	return errors.Join(allErrs, &DBIsRunningError{Detail: msg})
}

func (op *httpsCheckRunningDBOp) execute(execContext *opEngineExecContext) error {
	op.logger.Info("Execute() called", "opType", op.opType)
	switch op.opType {
	case CreateDB, StartDB, ReviveDB:
		return op.checkDBConnection(execContext)
	case StopDB:
		return op.pollForDBDown(execContext)
	}

	return fmt.Errorf("unknown operation found in HTTPCheckRunningDBOp")
}

func (op *httpsCheckRunningDBOp) pollForDBDown(execContext *opEngineExecContext) error {
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
			op.logger.Info("failure when checking node status", "err", err)
		} else {
			return nil
		}
		count++
	}
	// timeout
	msg := fmt.Sprintf("the DB is still up after %s seconds", timeoutSecondStr)
	op.logger.PrintWarning(msg)
	return errors.New(msg)
}

func (op *httpsCheckRunningDBOp) checkDBConnection(execContext *opEngineExecContext) error {
	err := execContext.dispatcher.sendRequest(&op.clusterHTTPRequest)
	if err != nil {
		return fmt.Errorf("fail to dispatch request %v: %w", op.clusterHTTPRequest, err)
	}
	return op.processResult(execContext)
}

func (op *httpsCheckRunningDBOp) finalize(_ *opEngineExecContext) error {
	return nil
}
