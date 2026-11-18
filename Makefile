MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -euc
.DEFAULT_GOAL := test

.PHONY: test cov testrace check lint tidy integ

test integ:
	go test -count=1 ./...

cov:
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out

testrace:
	go test -count=1 -race ./... $(TESTARGS)

check: lint tidy

lint:
	golangci-lint run --build-tags "$(GO_TAGS)"

tidy:
	go mod tidy
	@if test -n "$$(git status --porcelain -- go.mod go.sum)"; then \
		echo 'go.mod or go.sum needs updating'; \
		git --no-pager diff -- go.mod go.sum; \
		exit 1; fi
