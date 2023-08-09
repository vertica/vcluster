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
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/vertica/vcluster/rfc7807"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type HTTPAdapter struct {
	host string
}

func MakeHTTPAdapter() HTTPAdapter {
	return HTTPAdapter{}
}

const (
	certPathBase          = "/opt/vertica/config/https_certs"
	nmaPort               = 5554
	httpsPort             = 8443
	defaultRequestTimeout = 300 // seconds
)

type certificatePaths struct {
	certFile string
	keyFile  string
	caFile   string
}

func (adapter *HTTPAdapter) sendRequest(request *HostHTTPRequest, resultChannel chan<- HostHTTPResult) {
	// build query params
	queryParams := buildQueryParamString(request.QueryParams)

	// set up the request URL
	var port int
	if request.IsNMACommand {
		port = nmaPort
	} else {
		port = httpsPort
	}

	requestURL := fmt.Sprintf("https://%s:%d/%s%s",
		adapter.host,
		port,
		request.Endpoint,
		queryParams)
	vlog.LogInfo("Request URL %s\n", requestURL)

	// whether use password (for HTTPS endpoints only)
	usePassword, err := whetherUsePassword(request)
	if err != nil {
		resultChannel <- adapter.makeExceptionResult(err)
		return
	}

	// HTTP client
	client, err := adapter.setupHTTPClient(request, usePassword, resultChannel)
	if err != nil {
		resultChannel <- adapter.makeExceptionResult(err)
		return
	}

	// set up request body
	var requestBody io.Reader
	if request.RequestData == "" {
		requestBody = http.NoBody
	} else {
		requestBody = bytes.NewBuffer([]byte(request.RequestData))
	}

	// build HTTP request
	req, err := http.NewRequest(request.Method, requestURL, requestBody)
	if err != nil {
		err = fmt.Errorf("fail to build request %v on host %s, details %w",
			request.Endpoint, adapter.host, err)
		resultChannel <- adapter.makeExceptionResult(err)
		return
	}

	// set username and password
	// which is only used for HTTPS endpoints
	if usePassword {
		req.SetBasicAuth(request.Username, *request.Password)
	}

	// send HTTP request
	resp, err := client.Do(req)
	if err != nil {
		err = fmt.Errorf("fail to send request %v on host %s, details %w",
			request.Endpoint, adapter.host, err)
		resultChannel <- adapter.makeExceptionResult(err)
		return
	}
	defer resp.Body.Close()

	// generate and return the result
	resultChannel <- adapter.generateResult(resp)
}

func (adapter *HTTPAdapter) generateResult(resp *http.Response) HostHTTPResult {
	bodyString, err := adapter.readResponseBody(resp)
	if err != nil {
		return adapter.makeExceptionResult(err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return adapter.makeSuccessResult(bodyString, resp.StatusCode)
	}
	return adapter.makeFailResult(resp.Header, bodyString, resp.StatusCode)
}

func (adapter *HTTPAdapter) readResponseBody(resp *http.Response) (bodyString string, err error) {
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("fail to read the response body: %w", err)
		return "", err
	}
	bodyString = string(bodyBytes)

	return bodyString, nil
}

// makeSuccessResult is a factory method for HostHTTPResult when a success
// response comes back from a REST endpoints.
func (adapter *HTTPAdapter) makeSuccessResult(content string, statusCode int) HostHTTPResult {
	return HostHTTPResult{
		host:       adapter.host,
		status:     SUCCESS,
		statusCode: statusCode,
		content:    content,
	}
}

// makeExceptionResult is a factory method for HostHTTPResult when an error
// during the process of communicating with a REST endpoint. It won't refer to
// the error received over the wire, but usually some error that occurred in the
// process of communicating.
func (adapter *HTTPAdapter) makeExceptionResult(err error) HostHTTPResult {
	return HostHTTPResult{
		host:   adapter.host,
		status: EXCEPTION,
		err:    err,
	}
}

// makeFailResult is a factory method for HostHTTPResult when an error response
// is received from a REST endpoint.
func (adapter *HTTPAdapter) makeFailResult(header http.Header, respBody string, statusCode int) HostHTTPResult {
	return HostHTTPResult{
		host:       adapter.host,
		status:     FAILURE,
		statusCode: statusCode,
		content:    respBody,
		err:        adapter.extractErrorFromResponse(header, respBody, statusCode),
	}
}

// extractErrorFromResponse is called when we get a failed response from a REST
// call. We will look at the headers and response body to decide what error
// object to create.
func (adapter *HTTPAdapter) extractErrorFromResponse(header http.Header, respBody string, statusCode int) error {
	if header.Get("Content-Type") == rfc7807.ContentType {
		return rfc7807.GenerateErrorFromResponse(respBody)
	}
	return fmt.Errorf("status code %d returned from host %s: %s", statusCode, adapter.host, respBody)
}

func whetherUsePassword(request *HostHTTPRequest) (bool, error) {
	if request.IsNMACommand {
		return false, nil
	}

	// in case that password is provided
	if request.Password != nil {
		return true, nil
	}

	// otherwise, use certs
	// a. use certs in options
	if request.UseCertsInOptions {
		return false, nil
	}

	// b. use certs in local path
	_, err := getCertFilePaths()
	if err != nil {
		// in case that the cert files do not exist
		return false, fmt.Errorf("either TLS certificates or password should be provided")
	}

	return false, nil
}

// this variable is for unit test, be careful to modify it
var getCertFilePathsFn = getCertFilePaths

func (adapter *HTTPAdapter) buildCertsFromFile() (tls.Certificate, *x509.CertPool, error) {
	certPaths, err := getCertFilePathsFn()
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("fail to get paths for certificates, details %w", err)
	}

	cert, err := tls.LoadX509KeyPair(certPaths.certFile, certPaths.keyFile)
	if err != nil {
		return cert, nil, fmt.Errorf("fail to load HTTPS certificates, details %w", err)
	}

	caCert, err := os.ReadFile(certPaths.caFile)
	if err != nil {
		return cert, nil, fmt.Errorf("fail to load HTTPS CA certificates, details %w", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return cert, caCertPool, nil
}

func (adapter *HTTPAdapter) buildCertsFromMemory(key, cert, caCert string) (tls.Certificate, *x509.CertPool, error) {
	certificate, err := tls.X509KeyPair([]byte(cert), []byte(key))
	if err != nil {
		return certificate, nil, fmt.Errorf("fail to load HTTPS certificates, details %w", err)
	}

	caCertPool := x509.NewCertPool()
	ok := caCertPool.AppendCertsFromPEM([]byte(caCert))
	if !ok {
		return certificate, nil, fmt.Errorf("fail to load HTTPS CA certificates")
	}

	return certificate, caCertPool, nil
}

func (adapter *HTTPAdapter) setupHTTPClient(
	request *HostHTTPRequest,
	usePassword bool,
	_ chan<- HostHTTPResult) (*http.Client, error) {
	var client *http.Client

	// set up request timeout
	requestTimeout := time.Duration(defaultRequestTimeout)
	if request.Timeout > 0 {
		requestTimeout = time.Duration(request.Timeout)
	} else if request.Timeout == -1 {
		requestTimeout = time.Duration(0) // a Timeout of zero means no timeout.
	}

	if usePassword {
		// TODO: we have to use `InsecureSkipVerify: true` here,
		//       as password is used
		//nolint:gosec
		client = &http.Client{
			Timeout: time.Second * requestTimeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	} else {
		var cert tls.Certificate
		var caCertPool *x509.CertPool
		var err error
		if request.UseCertsInOptions {
			cert, caCertPool, err = adapter.buildCertsFromMemory(request.Certs.key, request.Certs.cert, request.Certs.caCert)
		} else {
			cert, caCertPool, err = adapter.buildCertsFromFile()
		}
		if err != nil {
			return client, err
		}
		// for both http and nma, we have to use `InsecureSkipVerify: true` here
		// because the certs are self signed at this time
		// TODO: update the InsecureSkipVerify once we start to use non-self-signed certs

		//nolint:gosec
		client = &http.Client{
			Timeout: time.Second * requestTimeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					Certificates:       []tls.Certificate{cert},
					RootCAs:            caCertPool,
					InsecureSkipVerify: true,
				},
			},
		}
	}
	return client, nil
}

func buildQueryParamString(queryParams map[string]string) string {
	var queryParamString string
	if len(queryParams) == 0 {
		return queryParamString
	}

	v := url.Values{}
	for key, value := range queryParams {
		v.Set(key, value)
	}
	queryParamString = "?" + v.Encode()
	return queryParamString
}

func getCertFilePaths() (certPaths certificatePaths, err error) {
	username, err := util.GetCurrentUsername()
	if err != nil {
		return certPaths, err
	}

	certPaths.certFile = path.Join(certPathBase, username+".pem")
	if !util.CheckPathExist(certPaths.certFile) {
		return certPaths, fmt.Errorf("cert file path does not exist")
	}

	certPaths.keyFile = path.Join(certPathBase, username+".key")
	if !util.CheckPathExist(certPaths.keyFile) {
		return certPaths, fmt.Errorf("key file path does not exist")
	}

	certPaths.caFile = path.Join(certPathBase, "rootca.pem")
	if !util.CheckPathExist(certPaths.caFile) {
		return certPaths, fmt.Errorf("ca file path does not exist")
	}

	return certPaths, nil
}
