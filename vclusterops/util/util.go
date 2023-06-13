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

package util

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strings"

	"golang.org/x/exp/slices"
	"golang.org/x/sys/unix"

	"github.com/vertica/vcluster/vclusterops/vlog"
)

func GetJSONLogErrors(responseContent string, responseObj any, opName string) error {
	err := json.Unmarshal([]byte(responseContent), responseObj)
	if err != nil {
		opTag := ""
		if opName != "" {
			opTag = fmt.Sprintf("[%s] ", opName)
		}

		vlog.LogError(opTag+"fail to unmarshal the response content, details: %v\n", err)
		return err
	}

	return nil
}

// calculate array diff: m-n
func SliceDiff[K comparable](m, n []K) []K {
	nSet := make(map[K]struct{}, len(n))
	for _, x := range n {
		nSet[x] = struct{}{}
	}

	var diff []K
	for _, x := range m {
		if _, found := nSet[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

// calculate diff of map keys: m-n
func MapKeyDiff[M ~map[K]V, K comparable, V any](m, n M) []K {
	var diff []K

	for k := range m {
		if _, found := n[k]; !found {
			diff = append(diff, k)
		}
	}

	return diff
}

func CheckPathExist(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

func StringInArray(str string, list []string) bool {
	return slices.Contains(list, str)
}

// convert an array to a string by joining the elements in the array
// using the given delimiter
func ArrayToString(arr []string, delimiter string) string {
	return strings.Join(arr, delimiter)
}

func GetCurrentUsername() (string, error) {
	currentUser, err := user.Current()
	if err != nil {
		return "", err
	}
	return currentUser.Username, nil
}

// avoid import strings in every operation
func TrimSpace(str string) string {
	return strings.TrimSpace(str)
}

// have this util function so no need to import file/path
// for every command that needs check file path
func IsAbsPath(path string) bool {
	return filepath.IsAbs(path)
}

func ResolveToAbsPath(path string) (string, error) {
	if !strings.Contains(path, "~") {
		return filepath.Abs(path)
	}
	// needed for resolving '~' in relative paths
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	homeDir := usr.HomeDir

	if path == "~" {
		return homeDir, nil
	} else if strings.HasPrefix(path, "~/") {
		return filepath.Join(homeDir, path[2:]), nil
	} else {
		return "", fmt.Errorf("invalid path")
	}
}

// IP util functions
func IsIPv4(ip string) bool {
	return net.ParseIP(ip).To4() != nil
}

func IsIPv6(ip string) bool {
	return net.ParseIP(ip).To16() != nil
}

func ResolveToIPAddrs(hostname string, ipv6 bool) ([]string, error) {
	// resolve hostname using local resolver
	hostIPs, err := net.LookupHost(hostname)
	if err != nil {
		return nil, err
	}
	if len(hostIPs) < 1 {
		return nil, fmt.Errorf("cannot resolve %s to a valid IP address", hostname)
	}
	var v4Addrs []string
	var v6Addrs []string
	for _, addr := range hostIPs {
		if IsIPv4(addr) {
			v4Addrs = append(v4Addrs, addr)
		} else if IsIPv6(addr) {
			v6Addrs = append(v6Addrs, addr)
		} else {
			return nil, fmt.Errorf("%s is resolved to invalid address %s", hostname, addr)
		}
	}
	if ipv6 {
		return v6Addrs, nil
	}
	return v4Addrs, nil
}

func ResolveToOneIP(hostname string, ipv6 bool) (string, error) {
	// already an IPv4 or IPv6 address
	if !ipv6 && IsIPv4(hostname) {
		return hostname, nil
	}
	// IPv6
	if ipv6 && IsIPv6(hostname) {
		return hostname, nil
	}
	addrs, err := ResolveToIPAddrs(hostname, ipv6)
	// contains the case where the hostname cannot be resolved to be IP
	if err != nil {
		return "", err
	}
	if len(addrs) > 1 {
		return "", fmt.Errorf("%s is resolved to more than one IP addresss: %v", hostname, addrs)
	}
	return addrs[0], nil
}

// replace all '//' to be '/', trim the path string
func GetCleanPath(path string) string {
	if path == "" {
		return path
	}
	cleanPath := strings.TrimSpace(path)
	cleanPath = strings.ReplaceAll(cleanPath, "//", "/")
	return cleanPath
}

func AbsPathCheck(dirPath string) error {
	if !filepath.IsAbs(dirPath) {
		return fmt.Errorf("'%s' is not an absolute path", dirPath)
	}
	return nil
}

func SplitHosts(hosts string) ([]string, error) {
	if strings.TrimSpace(hosts) == "" {
		return []string{}, fmt.Errorf("must specify a host or host list")
	}
	splitRes := strings.Split(strings.ToLower(strings.TrimSpace(hosts)), ",")
	for i, host := range splitRes {
		splitRes[i] = strings.TrimSpace(host)
	}
	return splitRes, nil
}

// O(1) time complexity as we have known limited number of flags
func IsFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

// get env var with a fallback value
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func CheckMissingFields(object any) error {
	var missingFields []string
	v := reflect.ValueOf(object)
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).IsZero() {
			missingFields = append(missingFields, v.Type().Field(i).Name)
		}
	}
	if len(missingFields) > 0 {
		return fmt.Errorf("unexpected or missing fields in response object: %v", missingFields)
	}
	return nil
}

// IsLocalHost decides whether an address or host name is local host
func IsLocalHost(host string) (bool, error) {
	// get local host name
	localHostName, err := os.Hostname()
	if err != nil {
		return false, err
	}

	// get local host addresses
	localHostAddresses, err := net.LookupIP(localHostName)
	if err != nil {
		return false, err
	}

	// check whether the input address matches any local addresses
	for _, localAddr := range localHostAddresses {
		inputAddresses, err := net.LookupIP(host)
		if err != nil {
			return false, err
		}

		for _, inputAddr := range inputAddresses {
			if inputAddr.Equal(localAddr) {
				return true, nil
			}
		}
	}

	return false, nil
}

// when password is given, the user name cannot be empty
func ValidateUsernameAndPassword(useHTTPPassword bool, userName string) {
	if useHTTPPassword && userName == "" {
		panic("[Programmer error] should provide a username for using basic authentication for HTTPS requests")
	}
}

const (
	FileExist    = 0
	FileNotExist = 1
	NoWritePerm  = 2
	// this can be extended
	// if we want to check other permissions
)

// Check whether the directory is read accessible
// by trying to open the file
func CanReadAccessDir(dirPath string) error {
	if _, err := os.Open(dirPath); err != nil {
		return fmt.Errorf("read access denied to path [%s]", dirPath)
	}
	return nil
}

// Check whether the directory is read accessible
func CanWriteAccessDir(dirPath string) int {
	// check whether the path exists
	_, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return FileNotExist
		}
	}

	// check whether the path has write access
	if err := unix.Access(dirPath, unix.W_OK); err != nil {
		log.Printf("Path '%s' is not writable.\n", dirPath)
		return NoWritePerm
	}

	return FileExist
}

// copy file from a source path to a destination path
func CopyFile(sourcePath, destinationPath string, perm fs.FileMode) error {
	fileBytes, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}

	err = os.WriteFile(destinationPath, fileBytes, perm)
	if err != nil {
		return fmt.Errorf("fail to create file at %s", destinationPath)
	}

	return nil
}

// check if an option is passed in
func IsOptionSet(parser *flag.FlagSet, optionName string) bool {
	flagVisitMap := make(map[string]bool)
	parser.Visit(func(f *flag.Flag) {
		flagVisitMap[f.Name] = true
	})
	return flagVisitMap[optionName]
}

// when db name is provided, make sure no special chars are in it
func ValidateDBName(dbName string) error {
	escapeChars := `=<>'^\".@*?#&/-:;{}()[] \~!%+|,` + "`$"
	for _, c := range dbName {
		if strings.Contains(escapeChars, string(c)) {
			return fmt.Errorf("invalid character in database name: %c", c)
		}
	}
	return nil
}

// suppress help message for hidden options
func SetParserUsage(parser *flag.FlagSet, op string) {
	fmt.Printf("Usage of %s:\n", op)
	fmt.Println("Options:")
	parser.VisitAll(func(f *flag.Flag) {
		if f.Usage != SuppressHelp {
			fmt.Printf("  -%s\n\t%s\n", f.Name, f.Usage)
		}
	})
}

func GetOptionalFlagMsg(message string) string {
	return message + " [Optional]"
}

func GetEonFlagMsg(message string) string {
	return "[Eon only] " + message
}
