package wire

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkDecodeBody(b *testing.B) {
	for _, n := range []int{1, MaxBatch} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			data := []byte("[" + strings.Repeat(`{"ack":{"sequence":7}},`, n-1) + `{"ack":{"sequence":7}}]`)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := DecodeBody(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBoundedJSON(b *testing.B) {
	for _, n := range []int{1, MaxBatch, MaxNodes} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			data := []byte(`{"members":[` + strings.Repeat(`{},`, n-1) + `{}]}`)
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				if !boundedJSON(data) {
					b.Fatal("rejected bounded array")
				}
			}
		})
	}
}
