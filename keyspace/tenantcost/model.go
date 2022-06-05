package tenantcost

import (
	"unsafe"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type RequestUnit float64

const (
	readRequestCost  = 1
	readCostPerMB    = 0.5
	writeRequestCost = 5
	writeCostPerMB   = 200
	podCPUSecondCost = 1000
)

type Config struct {
	// KVReadRequest is the baseline cost of a KV read.
	KVReadRequest RequestUnit

	// KVReadByte is the per-byte cost of a KV read.
	KVReadByte RequestUnit

	// KVWriteRequest is the baseline cost of a KV write.
	KVWriteRequest RequestUnit

	// KVWriteByte is the per-byte cost of a KV write.
	KVWriteByte RequestUnit

	// PodCPUSecond is the cost of using a CPU second on the SQL pod.
	PodCPUSecond RequestUnit
}

const perMBToPerByte = float64(1) / (1024 * 1024)

// DefaultConfig returns the configuration that corresponds to the default
// setting values.
func DefaultConfig() Config {
	return Config{
		KVReadRequest:  RequestUnit(readRequestCost),
		KVReadByte:     RequestUnit(readCostPerMB * perMBToPerByte),
		KVWriteRequest: RequestUnit(writeRequestCost),
		KVWriteByte:    RequestUnit(writeCostPerMB * perMBToPerByte),
		PodCPUSecond:   RequestUnit(podCPUSecondCost),
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

// RequestCost returns the portion of the cost that can be calculated upfront:
// the per-request cost (for both reads and writes) and the per-byte write cost.
func (c *Config) RequestCost(bri RequestInfo) RequestUnit {
	if isWrite, writeBytes := bri.IsWrite(); isWrite {
		return c.KVWriteCost(writeBytes)
	}
	return c.KVReadRequest
}

// ResponseCost returns the portion of the cost that can only be calculated
// after-the-fact: the per-byte read cost.
func (c *Config) ResponseCost(bri ResponseInfo) RequestUnit {
	return RequestUnit(bri.ReadBytes()) * c.KVReadByte
}

// RequestInfo captures the request information that is used (together with
// the cost model) to determine the portion of the cost that can be calculated
// upfront. Specifically: whether it is a read or a write and the write size (if
// it's a write).
type RequestInfo struct {
	writeBytes int64
}

// MakeRequestInfo extracts the relevant information from a BatchRequest.
func MakeRequestInfo(req *tikvrpc.Request) RequestInfo {
	if !req.IsTxnWriteRequest() && !req.IsRawWriteRequest() {
		return RequestInfo{writeBytes: -1}
	}

	var writeBytes int64
	switch r := req.Req.(type) {
	case *kvrpcpb.PrewriteRequest:
		writeBytes += int64(r.TxnSize)
	case *kvrpcpb.CommitRequest:
		writeBytes += int64(unsafe.Sizeof(r.Keys))
	}

	return RequestInfo{writeBytes: writeBytes}
}

// IsWrite returns whether the request is a write, and if so the write size in
// bytes.
func (bri RequestInfo) IsWrite() (isWrite bool, writeBytes int64) {
	if bri.writeBytes == -1 {
		return false, 0
	}
	return true, bri.writeBytes
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
	readBytes int64
}

// MakeResponseInfo extracts the relevant information from a BatchResponse.
func MakeResponseInfo(resp *tikvrpc.Response) ResponseInfo {
	var (
		readBytes    int64
		detailV2     *kvrpcpb.ExecDetailsV2
		detail       *kvrpcpb.ExecDetails
		respDataSize int64
	)
	if resp.Resp == nil {
		return ResponseInfo{readBytes}
	}
	switch r := resp.Resp.(type) {
	case *coprocessor.Response:
		detailV2 = r.ExecDetailsV2
		detail = r.ExecDetails
		respDataSize = int64(r.Data.Size())
	case *tikvrpc.CopStreamResponse:
		// streaming request returns io.EOF, so the first CopStreamResponse.Response maybe nil.
		if r.Response != nil {
			detailV2 = r.Response.ExecDetailsV2
			detail = r.Response.ExecDetails
		}
		respDataSize = int64(r.Data.Size())
	case *kvrpcpb.GetResponse:
		detailV2 = r.ExecDetailsV2
	case *kvrpcpb.ScanResponse:
		readBytes = int64(r.Size())
	}

	if detailV2 != nil && detailV2.ScanDetailV2 != nil {
		readBytes = int64(detailV2.ScanDetailV2.GetProcessedVersionsSize())
	} else if detail != nil && detail.ScanDetail != nil {
		// readBytes = detail.ScanDetail.Lock.ReadBytes + detail.ScanDetail.Write.ReadBytes + detail.ScanDetail.Write.ReadBytes
		readBytes = respDataSize
	}

	return ResponseInfo{readBytes: readBytes}
}

// ReadBytes returns the bytes read, or 0 if the request was a write.
func (bri ResponseInfo) ReadBytes() int64 {
	return bri.readBytes
}

// TestingResponseInfo creates a ResponseInfo for testing purposes.
func TestingResponseInfo(readBytes int64) ResponseInfo {
	return ResponseInfo{readBytes: readBytes}
}

// Add consumption from the given structure.
func Add(self *pdpb.Consumption, other *pdpb.Consumption) {
	self.RU += other.RU
	self.ReadRequests += other.ReadRequests
	self.ReadBytes += other.ReadBytes
	self.WriteRequests += other.WriteRequests
	self.WriteBytes += other.WriteBytes
	self.PodsCpuSeconds += other.PodsCpuSeconds
}

// Sub subtracts consumption, making sure no fields become negative.
func Sub(c *pdpb.Consumption, other *pdpb.Consumption) {
	if c.RU < other.RU {
		c.RU = 0
	} else {
		c.RU -= other.RU
	}

	if c.ReadRequests < other.ReadRequests {
		c.ReadRequests = 0
	} else {
		c.ReadRequests -= other.ReadRequests
	}

	if c.ReadBytes < other.ReadBytes {
		c.ReadBytes = 0
	} else {
		c.ReadBytes -= other.ReadBytes
	}

	if c.WriteRequests < other.WriteRequests {
		c.WriteRequests = 0
	} else {
		c.WriteRequests -= other.WriteRequests
	}

	if c.WriteBytes < other.WriteBytes {
		c.WriteBytes = 0
	} else {
		c.WriteBytes -= other.WriteBytes
	}

	if c.PodsCpuSeconds < other.PodsCpuSeconds {
		c.PodsCpuSeconds = 0
	} else {
		c.PodsCpuSeconds -= other.PodsCpuSeconds
	}
}
