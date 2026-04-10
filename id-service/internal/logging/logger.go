package logging

import (
	"bytes"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zapio"
)

type asyncTCPWriter struct {
	addr         string
	dialTimeout  time.Duration
	writeTimeout time.Duration
	queue        chan []byte

	connMu sync.Mutex
	conn   net.Conn

	closed  chan struct{}
	closeMu sync.Once
	wg      sync.WaitGroup

	dropped atomic.Uint64
}

func newAsyncTCPWriter(addr string, queueSize int, dialTimeout, writeTimeout time.Duration) *asyncTCPWriter {
	w := &asyncTCPWriter{
		addr:         addr,
		dialTimeout:  dialTimeout,
		writeTimeout: writeTimeout,
		queue:        make(chan []byte, queueSize),
		closed:       make(chan struct{}),
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for payload := range w.queue {
			_ = w.writePayload(payload)
		}
	}()

	return w
}

func (w *asyncTCPWriter) Write(p []byte) (int, error) {
	line := bytes.TrimSpace(p)
	if len(line) == 0 {
		return len(p), nil
	}

	payload := make([]byte, 0, len(line)+1)
	payload = append(payload, line...)
	payload = append(payload, '\n')

	select {
	case w.queue <- payload:
	default:
		w.dropped.Add(1)
	}

	return len(p), nil
}

func (w *asyncTCPWriter) writePayload(payload []byte) error {
	conn, err := w.ensureConn()
	if err != nil {
		return err
	}

	_ = conn.SetWriteDeadline(time.Now().Add(w.writeTimeout))
	if _, err := conn.Write(payload); err != nil {
		w.resetConn()
		return err
	}

	return nil
}

func (w *asyncTCPWriter) ensureConn() (net.Conn, error) {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	if w.conn != nil {
		return w.conn, nil
	}

	conn, err := net.DialTimeout("tcp", w.addr, w.dialTimeout)
	if err != nil {
		return nil, err
	}

	w.conn = conn
	return w.conn, nil
}

func (w *asyncTCPWriter) resetConn() {
	w.connMu.Lock()
	defer w.connMu.Unlock()
	if w.conn != nil {
		_ = w.conn.Close()
		w.conn = nil
	}
}

func (w *asyncTCPWriter) Close() error {
	w.closeMu.Do(func() {
		close(w.queue)
		w.wg.Wait()
		w.resetConn()
		close(w.closed)
	})
	return nil
}

func (w *asyncTCPWriter) Dropped() uint64 {
	return w.dropped.Load()
}

func Setup(serviceName, appPort string) (func(), error) {
	level := parseLevel(getEnv("LOG_LEVEL", "INFO"))

	consoleEncoderCfg := zap.NewDevelopmentEncoderConfig()
	consoleEncoderCfg.TimeKey = "ts"
	consoleEncoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	consoleCore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(consoleEncoderCfg),
		zapcore.AddSync(os.Stdout),
		level,
	)

	cores := []zapcore.Core{consoleCore}
	closers := make([]io.Closer, 0, 1)

	if addr := strings.TrimSpace(getEnv("FLUENTBIT_ADDR", "")); addr != "" {
		queueSize := getEnvInt("FLUENTBIT_QUEUE_SIZE", 1)
		if queueSize <= 0 {
			queueSize = 1
		}
		dialTimeout := time.Duration(getEnvInt("FLUENTBIT_DIAL_TIMEOUT_MS", 1000)) * time.Millisecond
		writeTimeout := time.Duration(getEnvInt("FLUENTBIT_WRITE_TIMEOUT_MS", 1000)) * time.Millisecond

		tcpWriter := newAsyncTCPWriter(addr, queueSize, dialTimeout, writeTimeout)
		closers = append(closers, tcpWriter)

		jsonEncoderCfg := zap.NewProductionEncoderConfig()
		jsonEncoderCfg.TimeKey = "@timestamp"
		jsonEncoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

		tcpCore := zapcore.NewCore(
			zapcore.NewJSONEncoder(jsonEncoderCfg),
			zapcore.AddSync(tcpWriter),
			level,
		).With([]zapcore.Field{
			zap.String("app_name", serviceName),
			zap.String("app_port", appPort),
		})
		cores = append(cores, tcpCore)
	}

	logger := zap.New(
		zapcore.NewTee(cores...),
		zap.AddCaller(),
		zap.AddCallerSkip(1),
	)

	zap.ReplaceGlobals(logger)

	log.SetFlags(0)
	log.SetOutput(&zapio.Writer{Log: logger, Level: level})

	cleanup := func() {
		for _, c := range closers {
			_ = c.Close()
		}
		_ = logger.Sync()
	}

	return cleanup, nil
}

func parseLevel(value string) zapcore.Level {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return zapcore.DebugLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

func getEnv(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func getEnvInt(key string, def int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return def
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return def
	}
	return n
}
