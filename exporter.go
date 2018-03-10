package resqueExporter

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/redis.v3"
)

const namespace = "resque"

type exporter struct {
	config         *Config
	queues         map[string][]string
	failedQueues   map[string][]string
	mut            sync.Mutex
	scrapeFailures prometheus.Counter
	processed      *prometheus.GaugeVec
	failedQueue    *prometheus.GaugeVec
	failedQueueStatus *prometheus.GaugeVec
	failedTotal    *prometheus.GaugeVec
	queueStatus    *prometheus.GaugeVec
	totalWorkers   *prometheus.GaugeVec
	activeWorkers  *prometheus.GaugeVec
	idleWorkers    *prometheus.GaugeVec
	timer          *time.Timer
}

func newExporter(config *Config) (*exporter, error) {
	e := &exporter{
		config: config,
		queues: make(map[string][]string),
		failedQueues: make(map[string][]string),
		queueStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "jobs_in_queue",
				Help:      "Number of remained jobs in queue",
			},
			[]string{"host", "db", "namespace", "queue_name"},
		),
		failedQueueStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "jobs_in_failed_queue",
				Help:      "Number of failed jobs in queue",
			},
			[]string{"host", "db", "namespace", "queue_name"},
		),
		processed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "processed",
				Help:      "Number of processed jobs",
			},
			[]string{"host", "db", "namespace"},
		),
		failedQueue: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "failed_queue_count",
				Help:      "Number of jobs in the failed queue",
			},
			[]string{"host", "db", "namespace"},
		),
		failedTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "failed",
				Help:      "Number of failed jobs",
			},
			[]string{"host", "db", "namespace"},
		),
		scrapeFailures: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "exporter_scrape_failures_total",
				Help:      "Number of errors while scraping resque.",
			},
		),
		totalWorkers: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "total_workers",
				Help:      "Number of workers",
			},
			[]string{"host", "db", "namespace"},
		),
		activeWorkers: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "active_workers",
				Help:      "Number of active workers",
			},
			[]string{"host", "db", "namespace"},
		),
		idleWorkers: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "idle_workers",
				Help:      "Number of idle workers",
			},
			[]string{"host", "db", "namespace"},
		),
	}

	return e, nil
}

func (e *exporter) Describe(ch chan<- *prometheus.Desc) {
	e.scrapeFailures.Describe(ch)
	e.queueStatus.Describe(ch)
	e.failedQueueStatus.Describe(ch)
	e.processed.Describe(ch)
	e.failedQueue.Describe(ch)
	e.failedTotal.Describe(ch)
	e.totalWorkers.Describe(ch)
	e.activeWorkers.Describe(ch)
	e.idleWorkers.Describe(ch)
}

func (e *exporter) Collect(ch chan<- prometheus.Metric) {
	e.mut.Lock() // To protect metrics from concurrent collects.
	defer e.mut.Unlock()

	if e.timer != nil {
		// guarded
		e.notifyToCollect(ch)
		return
	}

	if err := e.collect(ch); err != nil {
		e.incrementFailures(ch)
	}

	e.timer = time.AfterFunc(time.Duration(e.config.GuardIntervalMillis)*time.Millisecond, func() {
		// reset timer
		e.mut.Lock()
		defer e.mut.Unlock()
		e.timer = nil
	})
}

func (e *exporter) collect(ch chan<- prometheus.Metric) error {
	for i := range e.config.ResqueConfigs {
		resqueConf := e.config.ResqueConfigs[i]
		labels := []string{resqueConf.Host, strconv.FormatInt(resqueConf.DB, 10), resqueConf.Namespace}

		resqueNamespace := resqueConf.Namespace

		redisOpt := &redis.Options{
			Addr:     fmt.Sprintf("%s:%d", resqueConf.Host, resqueConf.Port),
			Password: resqueConf.Password,
			DB:       resqueConf.DB,
		}

		client := redis.NewClient(redisOpt)
		defer client.Close()

		failed, err := client.LLen(fmt.Sprintf("%s:failed", resqueNamespace)).Result()
		if err != nil && err != redis.Nil {
			return err
		}
		e.failedQueue.WithLabelValues(labels...).Set(float64(failed))

		queues, err := client.SMembers(fmt.Sprintf("%s:queues", resqueNamespace)).Result()
		if err != nil && err != redis.Nil {
			return err
		}

		var (
			labelsString = fmt.Sprintf("%s:%s:%s", labels[0], labels[1], labels[2])
			removedQueues = make([]string, 10)
			exporterQueuesIdx = 0
			oldQueues = e.queues[labelsString]
		)
		sort.Strings(queues)

		for _, q := range queues {
			for exporterQueuesIdx < len(oldQueues) {
				oldQueue := oldQueues[exporterQueuesIdx]
				if oldQueue > q { break }
				if oldQueue != q { removedQueues = append(removedQueues, oldQueue) }
				exporterQueuesIdx += 1
			}
			n, err := client.LLen(fmt.Sprintf("%s:queue:%s", resqueNamespace, q)).Result()
			if err != nil && err != redis.Nil {
				return err
			}
			e.queueStatus.WithLabelValues(append(labels, q)...).Set(float64(n))
		}
		removedQueues = append(removedQueues, oldQueues[exporterQueuesIdx:]...)

		for _, q := range removedQueues {
			e.queueStatus.DeleteLabelValues(append(labels, q)...)
		}

		e.queues[labelsString] = queues

		failedQueues, err := client.SMembers(fmt.Sprintf("%s:failed_queues", resqueNamespace)).Result()
		if err != nil && err != redis.Nil {
			return err
		}

		var (
			removedFailedQueues = make([]string, 10)
			exporterFailedQueuesIdx = 0
			oldFailedQueues = e.failedQueues[labelsString]
		)
		sort.Strings(failedQueues)

		for _, q := range failedQueues {
			for exporterFailedQueuesIdx < len(oldFailedQueues) {
				oldQueue := oldFailedQueues[exporterFailedQueuesIdx]
				if oldQueue > q { break }
				if oldQueue != q { removedFailedQueues = append(removedFailedQueues, oldQueue) }
				exporterFailedQueuesIdx += 1
			}
			n, err := client.LLen(fmt.Sprintf("%s:%s", resqueNamespace, q)).Result()
			if err != nil && err != redis.Nil {
				return err
			}
			e.failedQueueStatus.WithLabelValues(append(labels, q)...).Set(float64(n))
		}
		removedFailedQueues = append(removedFailedQueues, oldFailedQueues[exporterFailedQueuesIdx:]...)

		for _, q := range removedFailedQueues {
			e.failedQueueStatus.DeleteLabelValues(append(labels, q)...)
		}

		e.failedQueues[labelsString] = failedQueues

		processed, err := client.Get(fmt.Sprintf("%s:stat:processed", resqueNamespace)).Result()
		if err != nil && err != redis.Nil {
			return err
		}
		processedCnt, _ := strconv.ParseFloat(processed, 64)
		e.processed.WithLabelValues(labels...).Set(processedCnt)

		failedtotal, err := client.Get(fmt.Sprintf("%s:stat:failed", resqueNamespace)).Result()
		if err != nil && err != redis.Nil {
			return err
		}
		failedCnt, _ := strconv.ParseFloat(failedtotal, 64)
		e.failedTotal.WithLabelValues(labels...).Set(failedCnt)

		workers, err := client.SMembers(fmt.Sprintf("%s:workers", resqueNamespace)).Result()
		if err != nil && err != redis.Nil {
			return err
		}
		e.totalWorkers.WithLabelValues(labels...).Set(float64(len(workers)))

		activeWorkers := 0
		idleWorkers := 0
		for _, w := range workers {
			_, err := client.Get(fmt.Sprintf("%s:worker:%s", resqueNamespace, w)).Result()
			if err != nil && err != redis.Nil {
				return err
			} else if err != redis.Nil {
				activeWorkers++
			} else {
				idleWorkers++
			}
		}
		e.activeWorkers.WithLabelValues(labels...).Set(float64(activeWorkers))
		e.idleWorkers.WithLabelValues(labels...).Set(float64(idleWorkers))
	}

	e.notifyToCollect(ch)

	return nil
}

func (e *exporter) incrementFailures(ch chan<- prometheus.Metric) {
	e.scrapeFailures.Inc()
	e.scrapeFailures.Collect(ch)
}

func (e *exporter) notifyToCollect(ch chan<- prometheus.Metric) {
	e.queueStatus.Collect(ch)
	e.failedQueueStatus.Collect(ch)
	e.processed.Collect(ch)
	e.failedQueue.Collect(ch)
	e.failedTotal.Collect(ch)
	e.totalWorkers.Collect(ch)
	e.activeWorkers.Collect(ch)
	e.idleWorkers.Collect(ch)
}
