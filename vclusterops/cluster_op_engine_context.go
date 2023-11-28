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

import "github.com/vertica/vcluster/vclusterops/vlog"

type opEngineExecContext struct {
	dispatcher             requestDispatcher
	networkProfiles        map[string]networkProfile
	nmaVDatabase           nmaVDatabase
	upHosts                []string // a sorted host list that contains all up nodes
	nodesInfo              []NodeInfo
	defaultSCName          string // store the default subcluster name of the database
	hostsWithLatestCatalog []string
	startupCommandMap      map[string][]string // store start up command map to restart nodes
	dbInfo                 string              // store the db info that retrieved from communal storage
}

func makeOpEngineExecContext(logger vlog.Printer) opEngineExecContext {
	newOpEngineExecContext := opEngineExecContext{}
	newOpEngineExecContext.dispatcher = makeHTTPRequestDispatcher(logger)

	return newOpEngineExecContext
}
