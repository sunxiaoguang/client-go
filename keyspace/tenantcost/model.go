package tenantcost

import (
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type RequestUnit float64

const (
	readRequestCost      = 1
	readCostPerMB        = 0.5
	writeRequestCost     = 5
	writeCostPerMB       = 200
	kvCPUMillisecondCost = 1
	podCPUSecondCost     = 1000
)

type Config struct {
	// DryRunMode is a mode that the cost will not be actually consumed.
	DryRunMode bool `toml:"dry-run-mode" json:"dry-run-mode"`

	// KVReadRequest is the baseline cost of a KV read.
	KVReadRequest RequestUnit `toml:"kv-read-request" json:"kv-read-request"`

	// KVReadByte is the per-byte cost of a KV read.
	KVReadByte RequestUnit `toml:"kv-read-byte" json:"kv-read-byte"`

	// KVWriteRequest is the baseline cost of a KV write.
	KVWriteRequest RequestUnit `toml:"kv-write-request" json:"kv-write-request"`

	// KVWriteByte is the per-byte cost of a KV write.
	KVWriteByte RequestUnit `toml:"kv-write-byte" json:"kv-write-byte"`

	// KVCPUMillisecond is the per-millisecond cost of a KV request.
	KVCPUMillisecond RequestUnit `toml:"kv-cpu-millisecond" json:"kv-cpu-millisecond"`

	// PodCPUSecond is the cost of using a CPU second on the SQL pod.
	PodCPUSecond RequestUnit `toml:"pod-cpu-second" json:"pod-cpu-second"`
}

const perMBToPerByte = float64(1) / (1024 * 1024)

// DefaultConfig returns the configuration that corresponds to the default
// setting values.
func DefaultConfig() Config {
	return Config{
		KVReadRequest:    RequestUnit(readRequestCost),
		KVReadByte:       RequestUnit(readCostPerMB * perMBToPerByte),
		KVWriteRequest:   RequestUnit(writeRequestCost),
		KVWriteByte:      RequestUnit(writeCostPerMB * perMBToPerByte),
		KVCPUMillisecond: RequestUnit(kvCPUMillisecondCost),
		PodCPUSecond:     RequestUnit(podCPUSecondCost),
	}
}

// KVReadCost calculates the cost of a KV read operation.
func (c *Config) KVReadCost(bytes int64) RequestUnit {
	return c.KVReadRequest + RequestUnit(bytes)*c.KVReadByte
}

// KVWriteCost calculates the cost of a KV write operation.
func (c *Config) KVWriteCost(bytes int64) RequestUnit {
	return c.KVWriteRequest + RequestUnit(bytes)*c.KVWriteByte
}

func (c *Config) KVCPUCost(milliseconds int64) RequestUnit {
	return c.KVCPUMillisecond * RequestUnit(milliseconds)
}

// RequestCost returns the portion of the cost that can be calculated upfront:
// the per-request cost (for both reads and writes) and the per-byte write cost.
func (c *Config) RequestCost(bri RequestInfo) RequestUnit {
	if isWrite, writeBytes, _ := bri.IsWrite(); isWrite {
		return c.KVWriteCost(writeBytes)
	}
	return c.KVReadRequest
}

// ResponseCost returns the portion of the cost that can only be calculated
// after-the-fact: the per-byte read and per-millisecond kv CPU time cost.
func (c *Config) ResponseCost(bri ResponseInfo) RequestUnit {
	return c.KVReadCost(bri.ReadBytes()) + c.KVCPUCost(bri.CPUTime())
}

// RequestInfo captures the request information that is used (together with
// the cost model) to determine the portion of the cost that can be calculated
// upfront. Specifically: whether it is a read or a write and the write size (if
// it's a write).
type RequestInfo struct {
	writeBytes int64
	kvRequest  int64
}

// MakeRequestInfo extracts the relevant information from a BatchRequest.
func MakeRequestInfo(req *tikvrpc.Request) RequestInfo {
	var writeBytes, kvRequest int64
	if !req.IsTxnWriteRequest() && !req.IsRawWriteRequest() {
		writeBytes = -1
	}

	switch r := req.Req.(type) {
	case *kvrpcpb.PrewriteRequest:
		for _, mutation := range r.Mutations {
			writeBytes += int64(len(mutation.Key) + len(mutation.Value))
		}
		kvRequest += int64(len(r.Mutations) + len(r.Secondaries) + 1)
		//case *kvrpcpb.CommitRequest:
		//writeBytes += int64(unsafe.Sizeof(r.Keys))
		//kvRequest += int64(len(r.Keys))
	}

	return RequestInfo{writeBytes: writeBytes, kvRequest: kvRequest}
}

// IsWrite returns whether the request is a write, and if so the write size in
// bytes.
func (bri RequestInfo) IsWrite() (isWrite bool, writeBytes int64, kvRequest int64) {
	if bri.writeBytes == -1 {
		return false, 0, 0
	}
	return true, bri.writeBytes, bri.kvRequest
}

// TestingRequestInfo creates a RequestInfo for testing purposes.
func TestingRequestInfo(isWrite bool, writeBytes int64) RequestInfo {
	if !isWrite {
		return RequestInfo{writeBytes: -1}
	}
	return RequestInfo{writeBytes: writeBytes}
}

// ResponseInfo captures the BatchResponse information that is used (together
// with the cost model) to determine the portion of the cost that can only be
// calculated after-the-fact. Specifically: the read size (if the request is a
// read).
type ResponseInfo struct {
	cpuTime     int64
	readBytes   int64
	coprCPUTime int64
	kvRequest   int64
}

// MakeResponseInfo extracts the relevant information from a BatchResponse.
func MakeResponseInfo(resp *tikvrpc.Response) ResponseInfo {
	var (
		cpuTime      int64
		readBytes    int64
		detailV2     *kvrpcpb.ExecDetailsV2
		detail       *kvrpcpb.ExecDetails
		respDataSize int64
		isCopr       bool
		coprCPUTime  int64
		kvRequest    int64
	)
	if resp.Resp == nil {
		return ResponseInfo{cpuTime, readBytes, coprCPUTime, kvRequest}
	}
	switch r := resp.Resp.(type) {
	case *coprocessor.Response:
		detailV2 = r.GetExecDetailsV2()
		detail = r.GetExecDetails()
		respDataSize = int64(r.Data.Size())
		isCopr = true
	case *tikvrpc.CopStreamResponse:
		// streaming request returns io.EOF, so the first CopStreamResponse.Response maybe nil.
		if r.Response != nil {
			detailV2 = r.Response.GetExecDetailsV2()
			detail = r.Response.GetExecDetails()
		}
		respDataSize = int64(r.Data.Size())
		isCopr = true
	case *kvrpcpb.GetResponse:
		detailV2 = r.GetExecDetailsV2()
	case *kvrpcpb.ScanResponse:
		readBytes = int64(r.Size())
	}

	if detailV2 != nil {
		cpuTime = int64(detailV2.GetTimeDetail().GetProcessWallTimeMs())
		readBytes = int64(detailV2.GetScanDetailV2().GetProcessedVersionsSize())
		kvRequest = int64(detailV2.GetScanDetailV2().ProcessedVersions)
	} else if detail != nil {
		cpuTime = int64(detail.GetTimeDetail().GetProcessWallTimeMs())
		// readBytes = detail.ScanDetail.Lock.ReadBytes + detail.ScanDetail.Write.ReadBytes + detail.ScanDetail.Write.ReadBytes
		readBytes = respDataSize
		scanDetail := detail.GetScanDetail()
		kvRequest = int64(scanDetail.GetData().Processed +
			scanDetail.GetLock().Processed +
			scanDetail.GetWrite().Processed)
	}

	if isCopr {
		coprCPUTime = cpuTime
	}

	return ResponseInfo{cpuTime, readBytes, coprCPUTime, kvRequest}
}

// CPUTime returns the CPU time in milliseconds.
func (bri ResponseInfo) CPUTime() int64 {
	return bri.cpuTime
}

// ReadBytes returns the bytes read, or 0 if the request was a write.
func (bri ResponseInfo) ReadBytes() int64 {
	return bri.readBytes
}

func (bri ResponseInfo) CoprCPUTime() int64 {
	return bri.coprCPUTime
}

func (bri ResponseInfo) KVRequest() int64 {
	return bri.kvRequest
}

// TestingResponseInfo creates a ResponseInfo for testing purposes.
func TestingResponseInfo(readBytes int64) ResponseInfo {
	return ResponseInfo{readBytes: readBytes}
}

// // Add consumption from the given structure.
// func Add(self *pdpb.Consumption, other *pdpb.Consumption) {
// 	self.RU += other.RU
// 	self.ReadRequests += other.ReadRequests
// 	self.ReadBytes += other.ReadBytes
// 	self.WriteRequests += other.WriteRequests
// 	self.WriteBytes += other.WriteBytes
// 	self.PodsCpuSeconds += other.PodsCpuSeconds
// 	self.KvReadCpuMilliseconds += other.KvReadCpuMilliseconds
// 	self.KvWriteCpuMilliseconds += other.KvWriteCpuMilliseconds
// }

// // Sub subtracts consumption, making sure no fields become negative.
// func Sub(c *pdpb.Consumption, other *pdpb.Consumption) {
// 	if c.RU < other.RU {
// 		c.RU = 0
// 	} else {
// 		c.RU -= other.RU
// 	}

// 	if c.ReadRequests < other.ReadRequests {
// 		c.ReadRequests = 0
// 	} else {
// 		c.ReadRequests -= other.ReadRequests
// 	}

// 	if c.ReadBytes < other.ReadBytes {
// 		c.ReadBytes = 0
// 	} else {
// 		c.ReadBytes -= other.ReadBytes
// 	}

// 	if c.WriteRequests < other.WriteRequests {
// 		c.WriteRequests = 0
// 	} else {
// 		c.WriteRequests -= other.WriteRequests
// 	}

// 	if c.WriteBytes < other.WriteBytes {
// 		c.WriteBytes = 0
// 	} else {
// 		c.WriteBytes -= other.WriteBytes
// 	}

// 	if c.PodsCpuSeconds < other.PodsCpuSeconds {
// 		c.PodsCpuSeconds = 0
// 	} else {
// 		c.PodsCpuSeconds -= other.PodsCpuSeconds
// 	}

// 	if c.KvReadCpuMilliseconds < other.KvReadCpuMilliseconds {
// 		c.KvReadCpuMilliseconds = 0
// 	} else {
// 		c.KvReadCpuMilliseconds -= other.KvReadCpuMilliseconds
// 	}

// 	if c.KvWriteCpuMilliseconds < other.KvWriteCpuMilliseconds {
// 		c.KvWriteCpuMilliseconds = 0
// 	} else {
// 		c.KvWriteCpuMilliseconds -= other.KvWriteCpuMilliseconds
// 	}
// }
