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

package rfc7807

import (
	"net/http"
	"path"
)

// List of all known RFC 7807 problems that vcluster may see. All are exported
// from the package so they can be used by the NMA, vcluster, etc.
//
// Treat each problem's type (URL reference) as immutable. They should never be
// changed across server releases. And they should never be reused as it is used to
// uniquely identify the problem that is hit.
//
// In general, the title should be constant too. The only time we may want to
// relax that is if they are changed for localization purposes.
const errorEndpointsPrefix = "https://integrators.vertica.com/rest/errors/"

var (
	GenericBootstrapCatalogFailure = newProblemID(
		path.Join(errorEndpointsPrefix, "internal-bootstrap-catalog-failure"),
		"Internal error while bootstraping the catalog",
		http.StatusInternalServerError,
	)
	CommunalStorageNotEmpty = newProblemID(
		path.Join(errorEndpointsPrefix, "communal-storage-not-empty"),
		"Communal storage is not empty",
		http.StatusInternalServerError,
	)
	CommunalStoragePathInvalid = newProblemID(
		path.Join(errorEndpointsPrefix, "communal-storage-path-invalid"),
		"Communal storage is not a valid path for the file system",
		http.StatusInternalServerError,
	)
	CommunalRWAccessError = newProblemID(
		path.Join(errorEndpointsPrefix, "communal-read-write-access-error"),
		"Failed while testing read/write access to the communal storage",
		http.StatusInternalServerError,
	)
	CommunalAccessError = newProblemID(
		path.Join(errorEndpointsPrefix, "communal-access-error"),
		"Error accessing communal storage",
		http.StatusInternalServerError,
	)
	GenericLicenseCheckFailure = newProblemID(
		path.Join(errorEndpointsPrefix, "internal-license-check-failure"),
		"Internal error while checking license file",
		http.StatusInternalServerError,
	)
	WrongRequestMethod = newProblemID(
		path.Join(errorEndpointsPrefix, "wrong-request-method"),
		"Wrong request method used",
		http.StatusMethodNotAllowed,
	)
	BadRequest = newProblemID(
		path.Join(errorEndpointsPrefix, "bad-request"),
		"Bad request sent",
		http.StatusBadRequest,
	)
	GenericHTTPInternalServerError = newProblemID(
		path.Join(errorEndpointsPrefix, "http-internal-server-error"),
		"Internal server error",
		http.StatusInternalServerError,
	)
	GenericGetNodeInfoFailure = newProblemID(
		path.Join(errorEndpointsPrefix, "internal-get-node-info-failure"),
		"Internal error while getting node information",
		http.StatusInternalServerError,
	)
	GenericLoadRemoteCatalogFailure = newProblemID(
		path.Join(errorEndpointsPrefix, "internal-load-remote-catalog-failure"),
		"Internal error while loading remote catalog",
		http.StatusInternalServerError,
	)
	GenericSpreadSecurityPersistenceFailure = newProblemID(
		path.Join(errorEndpointsPrefix, "spread-security-persistence-failure"),
		"Internal error while persisting spread encryption key",
		http.StatusInternalServerError,
	)
	SubclusterNotFound = newProblemID(
		path.Join(errorEndpointsPrefix, "subcluster-not-found"),
		"Subcluster is not found",
		http.StatusInternalServerError,
	)
	GenericCatalogEditorFailure = newProblemID(
		path.Join(errorEndpointsPrefix, "internal-catalog-editor-failure"),
		"Internal error while running catalog editor",
		http.StatusInternalServerError,
	)
	GenericVerticaDownloadFileFailure = newProblemID(
		path.Join(errorEndpointsPrefix, "general-vertica-download-file-failure"),
		"General error while running Vertica download file",
		http.StatusInternalServerError,
	)
	InsufficientPrivilege = newProblemID(
		path.Join(errorEndpointsPrefix, "insufficient-privilege"),
		"Insufficient privilege",
		http.StatusInternalServerError,
	)
	UndefinedFile = newProblemID(
		path.Join(errorEndpointsPrefix, "undefined-file"),
		"Undefined file",
		http.StatusInternalServerError,
	)
	DuplicateFile = newProblemID(
		path.Join(errorEndpointsPrefix, "duplicate-file"),
		"Duplicate file",
		http.StatusInternalServerError,
	)
	WrongObjectType = newProblemID(
		path.Join(errorEndpointsPrefix, "wrong-object-type"),
		"Wrong object type",
		http.StatusInternalServerError,
	)
	DiskFull = newProblemID(
		path.Join(errorEndpointsPrefix, "disk-full"),
		"Disk full",
		http.StatusInternalServerError,
	)
	InsufficientResources = newProblemID(
		path.Join(errorEndpointsPrefix, "insufficient-resources"),
		"Insufficient resources",
		http.StatusInternalServerError,
	)
	IOError = newProblemID(
		path.Join(errorEndpointsPrefix, "io-error"),
		"IO error",
		http.StatusInternalServerError,
	)
	QueryCanceled = newProblemID(
		path.Join(errorEndpointsPrefix, "query-canceled"),
		"Query canceled",
		http.StatusInternalServerError,
	)
	InternalVerticaDownloadFileFailure = newProblemID(
		path.Join(errorEndpointsPrefix, "internal-vertica-download-file-failure"),
		"Internal error while running Vertica download file",
		http.StatusInternalServerError,
	)
)
