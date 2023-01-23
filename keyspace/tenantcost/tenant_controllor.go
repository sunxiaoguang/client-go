package tenantcost

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/metrics"
	"go.uber.org/zap"
)

// mainLoopUpdateInterval is the period at which we collect CPU usage and
// evaluate whether we need to send a new token request.
const mainLoopUpdateInterval = 1 * time.Second

// movingAvgFactor is the weight applied to a new "sample" of RU usage (with one
// sample per mainLoopUpdateInterval).
//
// If we want a factor of 0.5 per second, this should be:
//
//	0.5^(1 second / mainLoopUpdateInterval)
const movingAvgFactor = 0.5

const notifyFraction = 0.1

// If we have less than this many RUs to report, extend the reporting period to
// reduce load on the host cluster.
const consumptionReportingThreshold = 100

// The extended reporting period is this factor times the normal period.
const extendedReportingPeriodFactor = 4

const bufferRUs = 5000

// type TokenBucketProvider interface {
// 	TokenBucket(
// 		ctx context.Context, in *pdpb.TokenBucketRequest,
// 	) (*pdpb.TokenBucketResponse, error)
// }

const initialRquestUnits = 10000
const initialRate = 100

func newTenantSideCostController(
	tenantID uint64,
	//	provider TokenBucketProvider,
	config *Config,
) (*tenantSideCostController, error) {
	c := &tenantSideCostController{
		tenantID: tenantID,
		// provider:        provider,
		// responseChan:    make(chan *pdpb.TokenBucketResponse, 1),
		lowRUNotifyChan: make(chan struct{}, 1),
	}
	c.limiter = NewLimiter(initialRate, initialRquestUnits, c.lowRUNotifyChan)

	if config != nil {
		c.costCfg = *config
	} else {
		c.costCfg = DefaultConfig()
	}
	return c, nil
}

// NewTenantSideCostController creates an object which implements the
// server.TenantSideCostController interface.
func NewTenantSideCostController(
	tenantID uint64, config *Config,
) (*tenantSideCostController, error) {
	return newTenantSideCostController(tenantID, config)
}

type tenantSideCostController struct {
	tenantID            uint64
	limiter             *Limiter
	instanceFingerprint string
	costCfg             Config

	// lowRUNotifyChan is used when the number of available RUs is running low and
	// we need to send an early token bucket request.
	lowRUNotifyChan chan struct{}

	// run contains the state that is updated by the main loop.
	run struct {
		now time.Time
		// cpuUsage is the last CPU usage of the instance returned by UserCPUSecs.
		cpuUsage float64

		// targetPeriod stores the value of the TargetPeriodSetting setting at the
		// last update.
		targetPeriod time.Duration

		// initialRequestCompleted is set to true when the first token bucket
		// request completes successfully.
		initialRequestCompleted bool

		// requestInProgress is true if we are in the process of sending a request;
		// it gets set to false when we process the response (in the main loop),
		// even in error cases.
		requestInProgress bool

		// requestNeedsRetry is set if the last token bucket request encountered an
		// error. This triggers a retry attempt on the next tick.
		//
		// Note: requestNeedsRetry and requestInProgress are never true at the same
		// time.
		requestNeedsRetry bool

		lastRequestTime time.Time

		lastDeadline time.Time
		lastRate     float64

		// avgRUPerSec is an exponentially-weighted moving average of the RU
		// consumption per second; used to estimate the RU requirements for the next
		// request.
		avgRUPerSec float64
		// lastSecRU is the consumption.RU value when avgRUPerSec was last updated.
		avgRUPerSecLastRU float64

		setupNotificationCh        <-chan time.Time
		setupNotificationThreshold float64
		setupNotificationTimer     *time.Timer

		fallbackRate      float64
		fallbackRateStart time.Time
	}
}

// Start is part of multitenant.TenantSideCostController.
func (c *tenantSideCostController) Start(
	ctx context.Context,
	instanceFingerprint string,
) error {
	if len(instanceFingerprint) == 0 {
		return errors.New("invalid SQLInstanceID")
	}
	c.instanceFingerprint = instanceFingerprint

	go c.mainLoop(ctx)
	return nil
}

func (c *tenantSideCostController) initRunState(ctx context.Context) {
	c.run.targetPeriod = 10 * time.Second

	now := time.Now()
	c.run.now = now
	c.run.cpuUsage = UserCPUSecs(ctx)
	c.run.lastRequestTime = now
	c.run.avgRUPerSec = initialRquestUnits / c.run.targetPeriod.Seconds()
}

const CPUUsageAllowance = 10 * time.Millisecond

// updateRunState is called whenever the main loop awakens and accounts for the
// CPU usage in the interim.
func (c *tenantSideCostController) updateRunState(ctx context.Context) {
	c.run.targetPeriod = 10 * time.Second

	newTime := time.Now()
	newCPUUsage := UserCPUSecs(ctx)
	// Update CPU consumption.
	deltaCPU := newCPUUsage - c.run.cpuUsage

	// Subtract any allowance that we consider free background usage.
	if deltaTime := newTime.Sub(c.run.now); deltaTime > 0 {
		deltaCPU -= CPUUsageAllowance.Seconds() * deltaTime.Seconds()
	}
	if deltaCPU < 0 {
		deltaCPU = 0
	}
	ru := deltaCPU * float64(c.costCfg.PodCPUSecond)

	metrics.PodCPURU.Observe(ru)

	c.run.now = newTime
	c.run.cpuUsage = newCPUUsage
	log.Info("[tenant controllor] update run state, use cpu second", zap.Float64("deltaCPU", deltaCPU), zap.Float64("ru", ru), zap.Float64("newCPUUsage", newCPUUsage), zap.Float64("oldCPUUsage", c.run.cpuUsage), zap.Float64("remaining", c.limiter.AvailableTokens(newTime)), zap.Float64("limit", float64(c.limiter.Limit())))
}

// updateAvgRUPerSec is called exactly once per mainLoopUpdateInterval.
func (c *tenantSideCostController) updateAvgRUPerSec() {
}

// shouldReportConsumption decides if it's time to send a token bucket request
// to report consumption.
func (c *tenantSideCostController) shouldReportConsumption() bool {
	return false
}

func (c *tenantSideCostController) mainLoop(ctx context.Context) {
	interval := mainLoopUpdateInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	tickerCh := ticker.C

	c.initRunState(ctx)
	//	c.sendTokenBucketRequest(ctx, "init")

	for {
		select {
		case <-tickerCh:
			c.updateRunState(ctx)
			c.updateAvgRUPerSec()
			// Switch to the fallback rate, if necessary.
			if !c.run.fallbackRateStart.IsZero() && !c.run.now.Before(c.run.fallbackRateStart) {
				log.Info("[tenant controllor] switching to fallback rate", zap.Float64("rate", c.run.fallbackRate))
				c.limiter.SetLimitAt(c.run.now, Limit(c.run.fallbackRate))
				c.run.fallbackRateStart = time.Time{}
			}
			if c.run.requestNeedsRetry || c.shouldReportConsumption() {
				c.run.requestNeedsRetry = false
				//		c.sendTokenBucketRequest(ctx, "report")
			}
		case <-c.run.setupNotificationCh:
			c.run.setupNotificationTimer = nil
			c.run.setupNotificationCh = nil

			c.updateRunState(ctx)
			c.limiter.SetupNotification(c.run.now, float64(c.run.setupNotificationThreshold))

		case <-c.lowRUNotifyChan:
			c.updateRunState(ctx)
			c.run.fallbackRateStart = c.run.now.Add(200 * time.Millisecond)
			if !c.run.requestInProgress {
				log.Warn("[tenant controllor] low RU notification", zap.Float64("threshold", c.run.setupNotificationThreshold), zap.Time("now", c.run.now), zap.Time("rate-start", c.run.fallbackRateStart))
				//	c.sendTokenBucketRequest(ctx, "low_ru")
			}
		case <-ctx.Done():

			return
		}
	}
}

// OnRequestWait is part of the multitenant.TenantSideKVInterceptor
// interface.
func (c *tenantSideCostController) OnRequestWait(
	ctx context.Context, info RequestInfo,
) error {
	// c.limit.WaitN(ctx, c.costCfg.RequestCost(info))
	return nil
}

// OnResponse is part of the multitenant.TenantSideBatchInterceptor interface.
//
// the RequestCost to the bucket).
func (c *tenantSideCostController) OnResponse(
	ctx context.Context, req RequestInfo, resp ResponseInfo,
) {
	var (
		isWrite, writeBytes, kvRequest = req.IsWrite()
		writeRU, readRU, kvCPURU       float64
		readBytes, coprCPUMilliseconds int64
	)
	if isWrite {
		kvCPUMilliseconds := resp.CPUTime()
		writeRU = float64(c.costCfg.KVWriteCost(writeBytes))
		kvCPURU = float64(c.costCfg.KVCPUCost(kvCPUMilliseconds))
	} else {
		readBytes = resp.ReadBytes()
		kvCPUMilliseconds := resp.CPUTime()
		coprCPUMilliseconds = resp.CoprCPUTime()
		readRU = float64(c.costCfg.KVReadCost(readBytes))
		kvCPURU = float64(c.costCfg.KVCPUCost(kvCPUMilliseconds))
		kvRequest = resp.KVRequest()
	}

	if isWrite {
		metrics.TiKVWriteBytes.Add(float64(writeBytes))
		metrics.TiKVWriteKVBatch.Add(float64(1))
		metrics.TiKVWriteKVRequest.Add(float64(kvRequest))
		metrics.WriteByteRU.Observe(writeRU)
		metrics.WriteKVCPURU.Observe(kvCPURU)
	} else {
		metrics.TiKVReadBytes.Add(float64(readBytes))
		metrics.TiKVReadKVBatch.Add(float64(1))
		metrics.TiKVReadKVRequest.Add(float64(kvRequest))
		metrics.TiKVCoprocessorCPUTime.Add(float64(coprCPUMilliseconds) / 1000)
		metrics.ReadByteRU.Observe(readRU)
		metrics.ReadKVCPURU.Observe(kvCPURU)
	}
}
