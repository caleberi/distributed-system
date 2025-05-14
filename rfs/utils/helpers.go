package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
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

func LoopOverMap[T comparable, V comparable](data map[T]V, fn func(k T, V V)) {
	for k, v := range data {
		fn(k, v)
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

// TransformSlice applies a transformation function to each element of a slice
func TransformSlice[T, V any](slice []T, transform func(T) V) []V {
	result := make([]V, 0, len(slice))
	for _, item := range slice {
		result = append(result, transform(item))
	}
	return result
}

// ForEachInSlice executes a function for each element in a slice
func ForEachInSlice[T any](slice []T, action func(T)) {
	for _, item := range slice {
		action(item)
	}
}

// IterateOverMap executes a function for each key-value pair in a map
func IterateOverMap[K comparable, V any](m map[K]V, action func(K, V)) {
	for k, v := range m {
		action(k, v)
	}
}

// FilterMapToNew creates a new map with key-value pairs that satisfy a predicate
func FilterMapToNew[K, V comparable](m map[K]V, predicate func(V) bool) map[K]V {
	result := make(map[K]V)
	for k, v := range m {
		if predicate(v) {
			result[k] = v
		}
	}
	return result
}

// FilterSlice returns a new slice with elements that satisfy a predicate
func FilterSlice[T any](slice []T, predicate func(T) bool) []T {
	result := make([]T, 0)
	for _, item := range slice {
		if predicate(item) {
			result = append(result, item)
		}
	}
	return result
}

// ReduceSlice reduces a slice to a single value using an accumulator function
func ReduceSlice[T, V any](slice []T, initial V, accumulator func(V, T) V) V {
	result := initial
	for _, item := range slice {
		result = accumulator(result, item)
	}
	return result
}

// FindInSlice returns the first element in a slice that satisfies a predicate
func FindInSlice[T any](slice []T, predicate func(T) bool) (T, bool) {
	for _, item := range slice {
		if predicate(item) {
			return item, true
		}
	}
	var zero T
	return zero, false
}

// GroupByKey groups slice elements by a key derived from each element
func GroupByKey[T any, K comparable](slice []T, keyFunc func(T) K) map[K][]T {
	result := make(map[K][]T)
	for _, item := range slice {
		key := keyFunc(item)
		result[key] = append(result[key], item)
	}
	return result
}

// ChunkSlice splits a slice into chunks of a specified size
func ChunkSlice[T any](slice []T, chunkSize int) [][]T {
	var chunks [][]T
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}
	return chunks
}

// ZipSlices combines elements from two slices into pairs
func ZipSlices[T, U any](slice1 []T, slice2 []U) [][2]any {
	minLen := len(slice1)
	if len(slice2) < minLen {
		minLen = len(slice2)
	}
	result := make([][2]any, minLen)
	for i := 0; i < minLen; i++ {
		result[i] = [2]any{slice1[i], slice2[i]}
	}
	return result
}

func Sum(slice []float64) float64 {
	total := 0.0
	for i := 0; i < len(slice); i++ {
		total += slice[i]
	}
	return total
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
