package shared

import (
	"net/rpc"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
)

func UnicastToRPCServer(addr string, method string, args any, reply any) error {
	log.Info().Msgf("addr=%s method=%s", addr, method)
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

func BroadcastToRPCServers(addrs []string, method string, args any, reply []any) []error {
	var (
		forEachIndexed = func(data []string, fn func(v string, idx int)) {
			for i, dt := range data {
				fn(dt, i)
			}
		}
		wg = sync.WaitGroup{}
	)

	errs := make([]error, 0)
	forEachIndexed(addrs, func(addr string, index int) {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			err := UnicastToRPCServer(addr, method, args, reply[index])
			if err != nil {
				errs = append(errs, err)
			}
		}(&wg)
	})
	wg.Wait()
	return errs
}
