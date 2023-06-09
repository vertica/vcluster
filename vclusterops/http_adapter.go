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

	"vertica.com/vcluster/vclusterops/util"
	"vertica.com/vcluster/vclusterops/vlog"
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
	queryParams := buildQueryParamString(request.QueryParams, request.IsNMACommand)

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
		resultChannel <- adapter.makeExceptionResult(err.Error())
		return
	}

	// HTTP client
	client, err := adapter.setupHTTPClient(request, usePassword, resultChannel)
	if err != nil {
		adapter.makeExceptionResult(err.Error())
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
		errMessage := fmt.Sprintf("fail to build request %v on host %s, details %s",
			request, adapter.host, err.Error())
		resultChannel <- adapter.makeExceptionResult(errMessage)
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
		errMessage := fmt.Sprintf("fail to send request %v on host %s, details %s",
			request, adapter.host, err.Error())
		resultChannel <- adapter.makeExceptionResult(errMessage)
		return
	}
	defer resp.Body.Close()

	// process result
	adapter.processResult(resp, resultChannel)
}

func (adapter *HTTPAdapter) processResult(resp *http.Response, resultChannel chan<- HostHTTPResult) {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		bodyString, ok := adapter.readResponseBody(resp, resultChannel)
		if ok {
			// save as a successful result to the result channel
			resultChannel <- adapter.makeSucessResult(bodyString, resp.StatusCode)
		}
	} else {
		bodyString, ok := adapter.readResponseBody(resp, resultChannel)
		if ok {
			// save as a failed result to the result channel
			message := fmt.Sprintf("Request failed with code [%d], detail: %s", resp.StatusCode, bodyString)
			resultChannel <- adapter.makeFailResult(message, resp.StatusCode)
		}
	}
}

func (adapter *HTTPAdapter) readResponseBody(
	resp *http.Response,
	resultChannel chan<- HostHTTPResult) (bodyString string, ok bool) {
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		errMessage := fmt.Sprintf("fail to read the response body, details %s", err.Error())
		resultChannel <- adapter.makeExceptionResult(errMessage)
		return "", false
	}
	bodyString = string(bodyBytes)

	return bodyString, true
}

func (adapter *HTTPAdapter) makeSucessResult(content string, statusCode int) HostHTTPResult {
	var result HostHTTPResult
	result.host = adapter.host
	result.status = SUCCESS
	result.statusCode = statusCode
	result.content = content
	return result
}

func (adapter *HTTPAdapter) makeExceptionResult(errMessage string) HostHTTPResult {
	var result HostHTTPResult
	result.host = adapter.host
	result.status = EXCEPTION
	result.errMsg = errMessage
	return result
}

func (adapter *HTTPAdapter) makeFailResult(message string, statusCode int) HostHTTPResult {
	var result HostHTTPResult
	result.host = adapter.host
	result.status = FAILURE
	result.statusCode = statusCode
	result.content = message
	return result
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
	_, err := getCertFilePaths()
	if err != nil {
		// in case that the cert files do not exist
		return false, fmt.Errorf("either TLS certificates or password should be provided")
	}

	return false, nil
}

func (adapter *HTTPAdapter) buildCerts(resultChannel chan<- HostHTTPResult) (tls.Certificate, *x509.CertPool, error) {
	certPaths, err := getCertFilePaths()
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("fail to get username for certificates, details %s", err.Error())
	}

	cert, err := tls.LoadX509KeyPair(certPaths.certFile, certPaths.keyFile)
	if err != nil {
		return cert, nil, fmt.Errorf("fail to load HTTPS certificates, details %s", err.Error())
	}

	caCert, err := os.ReadFile(certPaths.caFile)
	if err != nil {
		return cert, nil, fmt.Errorf("fail to load CA cert, details %s", err.Error())
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return cert, caCertPool, nil
}

func (adapter *HTTPAdapter) setupHTTPClient(
	request *HostHTTPRequest,
	usePassword bool,
	resultChannel chan<- HostHTTPResult) (*http.Client, error) {
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
		cert, caCertPool, err := adapter.buildCerts(resultChannel)
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

func buildQueryParamString(queryParams map[string]string, isNMACommand bool) string {
	var queryParamString string
	if len(queryParams) == 0 {
		return queryParamString
	}

	// HTTPS service doesn't recognize encoded url with special characters, like ',', '%'
	// do not encode for HTTPS service urls
	if isNMACommand {
		v := url.Values{}
		for key, value := range queryParams {
			v.Set(key, value)
		}
		queryParamString = "?" + v.Encode()
	} else {
		queryParamString = ""
		for key, value := range queryParams {
			keyValueStr := key + "=" + value
			if queryParamString == "" {
				queryParamString = keyValueStr
			} else {
				queryParamString += ("&" + keyValueStr)
			}
		}
		queryParamString = "?" + queryParamString
	}
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
