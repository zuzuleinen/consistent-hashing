# consistent-hashing

A lightweight and concurrency-safe consistent hashing implementation in Go,
with support for **replication** and **virtual nodes**.

Check this [video](https://www.youtube.com/watch?v=vccwdhfqIrI) to learn about Consistent Hashing.

## Installation

```bash
go get https://github.com/zuzuleinen/consistent-hashing
```

Then use it in your project:

```go
import "github.com/zuzuleinen/consistent-hashing"
```

## Usage 

Simple usage:

```go
	ch := consistenthashing.NewConsistentHashing()

	ch.Add("host-1")
	ch.Add("host-2")
	ch.Add("host-3")

	matchedHosts, err := ch.Get("customer-id-1")
```