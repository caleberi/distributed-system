package utils

import (
	"fmt"
	"math/rand"
	"net/rpc"

	"github.com/caleberi/distributed-system/rfs/common"
)

func Map[T, V comparable](data []T, fn func(v T) V) []V {
	result := []V{}
	for _, dt := range data {
		result = append(result, fn(dt))
	}
	return result
}

func ForEach[T comparable](data []T, fn func(v T)) {
	for _, dt := range data {
		fn(dt)
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
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
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
