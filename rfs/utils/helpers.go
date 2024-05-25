package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net/rpc"
	"strings"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/rs/zerolog/log"
)

func Map[T, V comparable](data []T, fn func(v T) V) []V {
	result := []V{}
	for _, dt := range data {
		result = append(result, fn(dt))
	}
	return result
}

func ForEach[T any](data []T, fn func(v T)) {
	for _, dt := range data {
		fn(dt)
	}
}

func ExtractFromMap[K, V comparable](data, result map[K]V, fn func(value V) bool) {
	for k, v := range data {
		if fn(v) {
			result[k] = v
		}
	}
}

func Filter[T comparable](data []T, fn func(v T) bool) []T {
	result := []T{}
	for _, dt := range data {
		if fn(dt) {
			result = append(result, dt)
		}
	}
	return result
}

func GetFileNameAndDirName(p common.Path) (string, string) {
	ps := string(p)
	for i := len(ps) - 1; i > 0; i-- {
		if ps[i] == '/' {
			return ps[:i], ps[i+1:]
		}
	}
	return "", ""
}

func ValidateFilenameStr(filename string, p common.Path) (bool, error) {
	switch filename {
	case "", ".", "..":
		return true, fmt.Errorf("path %s does not have a base file", p)
	default:
		break
	}
	return false, nil
}

func CallRPCServer(addr string, method string, args any, reply any) error {
	log.Info().Msgf("addr=%s method=%s args=%#v ", addr, method, args)
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		if strings.Contains(err.Error(), "dial tcp: missing address") {
			log.Info().Msgf("err=%s addr=%s method=%s args=%#v ", err.Error(), addr, method, args)
		}
		return err
	}
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		return err
	}

	return nil
}

func CallAllRPCServers(addrs []string, method string, args any, reply []any) []error {
	errs := make([]error, 0)
	for i, addr := range addrs {
		err := CallRPCServer(addr, method, args, reply[i])
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func Sample(n, k int) ([]int, error) {
	if n < k {
		return nil, fmt.Errorf("population is not enough for sampling (n = %d, k = %d)", n, k)
	}
	return rand.Perm(n)[:k], nil
}

func ComputeChecksum(content string) string {
	hash := sha256.New()
	hash.Write([]byte(content))
	checksum := hash.Sum(nil)
	return hex.EncodeToString(checksum)
}
