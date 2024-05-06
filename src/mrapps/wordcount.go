package main

import (
	"fmt"
	"strings"
	"unicode"

	"6.5840/mr"
)

func Map(key string, contents string) []mr.KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}
func Reduce(key string, values []string) string {
	return fmt.Sprintf("%d", len(values))
}
