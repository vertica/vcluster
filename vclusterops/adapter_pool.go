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
	"sync"

	"github.com/vertica/vcluster/vclusterops/vlog"
)

type AdapterPool struct {
	// map from host to HTTPAdapter
	connections map[string]Adapter
}

var (
	poolInstance AdapterPool
	once         sync.Once
)

// return a singleton instance of the AdapterPool
func getPoolInstance() AdapterPool {
	/* if once.Do(f) is called multiple times,
	 * only the first call will invoke f,
	 * even if f has a different value in each invocation.
	 * Reference: https://pkg.go.dev/sync#Once
	 */
	once.Do(func() {
		poolInstance = makeAdapterPool()
	})

	return poolInstance
}

func makeAdapterPool() AdapterPool {
	newAdapterPool := AdapterPool{}
	newAdapterPool.connections = make(map[string]Adapter)
	return newAdapterPool
}

type adapterToRequest struct {
	adapter Adapter
	request HostHTTPRequest
}

func (pool *AdapterPool) sendRequest(clusterHTTPRequest *ClusterHTTPRequest) error {
	vlog.LogInfoln("Adapter pool's sendRequest is called")
	// build a collection of adapter to request
	// we need this step as a host may not be in the pool
	// in that case, we should not proceed
	var adapterToRequestCollection []adapterToRequest
	for host := range clusterHTTPRequest.RequestCollection {
		request := clusterHTTPRequest.RequestCollection[host]
		adapter, ok := pool.connections[host]
		if !ok {
			return fmt.Errorf("host %s is not found in the adapter pool", host)
		}
		ar := adapterToRequest{adapter: adapter, request: request}
		adapterToRequestCollection = append(adapterToRequestCollection, ar)
	}

	hostCount := len(adapterToRequestCollection)

	// result channel to collect result from each host
	resultChannel := make(chan HostHTTPResult, hostCount)

	for i := 0; i < len(adapterToRequestCollection); i++ {
		ar := adapterToRequestCollection[i]
		// send request to the hosts
		// each goroutine will handle one request for one host
		request := ar.request
		go ar.adapter.sendRequest(&request, resultChannel)
	}

	// handle results
	// we expect to receive the same number of results from the channel as the number of hosts
	// before proceeding to the next steps
	clusterHTTPRequest.ResultCollection = make(map[string]HostHTTPResult)
	for i := 0; i < hostCount; i++ {
		result, ok := <-resultChannel
		if ok {
			clusterHTTPRequest.ResultCollection[result.host] = result
		}
	}
	close(resultChannel)

	return nil
}
