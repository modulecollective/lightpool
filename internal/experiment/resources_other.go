//go:build !linux

package experiment

func peakRSS() (*int64, string) { return nil, "peak resident memory unavailable on this platform" }
