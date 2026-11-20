package experiment

import "syscall"

func peakRSS() (*int64, string) {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		return nil, err.Error()
	}
	n := r.Maxrss * 1024
	return &n, "getrusage(RUSAGE_SELF).Maxrss; process high-water RSS"
}
