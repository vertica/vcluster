package vclusterops

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/vertica/vcluster/vclusterops/vlog"
)

type NMADeleteDirectoriesOp struct {
	OpBase
	hostRequestBodyMap map[string]string
}

type deleteDirParams struct {
	Directories []string `json:"directories"`
	ForceDelete bool     `json:"force_delete"`
	Sandbox     bool     `json:"sandbox"`
}

func MakeNMADeleteDirectoriesOp(
	opName string,
	vdb *VCoordinationDatabase,
	forceDelete bool,
) (NMADeleteDirectoriesOp, error) {
	nmaDeleteDirectoriesOp := NMADeleteDirectoriesOp{}
	nmaDeleteDirectoriesOp.name = opName
	nmaDeleteDirectoriesOp.hosts = vdb.HostList

	err := nmaDeleteDirectoriesOp.buildRequestBody(vdb, forceDelete)
	if err != nil {
		return nmaDeleteDirectoriesOp, err
	}

	return nmaDeleteDirectoriesOp, nil
}

func (op *NMADeleteDirectoriesOp) buildRequestBody(
	vdb *VCoordinationDatabase,
	forceDelete bool,
) error {
	op.hostRequestBodyMap = make(map[string]string)
	for h, vnode := range vdb.HostNodeMap {
		p := deleteDirParams{}
		// directories
		p.Directories = append(p.Directories, vnode.CatalogPath)
		p.Directories = append(p.Directories, vnode.StorageLocations...)

		if vdb.UseDepot {
			dbDepotPath := filepath.Join(vdb.DepotPrefix, vdb.Name)
			p.Directories = append(p.Directories, vnode.DepotPath, dbDepotPath)
		}

		dbCatalogPath := filepath.Join(vdb.CatalogPrefix, vdb.Name)
		dbDataPath := filepath.Join(vdb.DataPrefix, vdb.Name)
		p.Directories = append(p.Directories, dbCatalogPath, dbDataPath)

		// force-delete
		p.ForceDelete = forceDelete

		// TODO: we don't have functionality of sandboxing at this time
		// we will update this once it's available
		p.Sandbox = false

		dataBytes, err := json.Marshal(p)
		if err != nil {
			return fmt.Errorf("[%s] fail to marshal request data to JSON string, detail: %w", op.name, err)
		}
		op.hostRequestBodyMap[h] = string(dataBytes)

		vlog.LogInfo("Host %s delete directory params %+v", h, p)
	}

	return nil
}

func (op *NMADeleteDirectoriesOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildNMAEndpoint("directories/delete")
		httpRequest.RequestData = op.hostRequestBodyMap[host]
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *NMADeleteDirectoriesOp) Prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *NMADeleteDirectoriesOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMADeleteDirectoriesOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

func (op *NMADeleteDirectoriesOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			// the response object will be a map[string]string, for example:
			// {
			//     "/data/test_db": "deleted",
			//     "/data/test_db/v_demo_db_node0001_catalog": "deleted",
			//     "/data/test_db/v_demo_db_node0001_data": "deleted"
			// }
			_, err := op.parseAndCheckMapResponse(host, result.content)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
			}
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	return allErrs
}
