package gocelery

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
	Broker  CeleryBroker
	backend CeleryBackend
	worker  *CeleryWorker
}

// CeleryBroker is interface for celery broker database
type CeleryBroker interface {
	SendCeleryMessage(*CeleryMessage) error
	GetTaskMessage() (*TaskMessage, *amqp.Delivery, error) // must be non-blocking
	GetAckLate() bool
	GetExchange() string
	GetQueue() string
	GetConnection() *amqp.Connection
}

// CeleryBackend is interface for celery backend database
type CeleryBackend interface {
	GetResult(string) (*ResultMessage, error) // must be non-blocking
	SetResult(taskID string, result *ResultMessage) error
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker, backend CeleryBackend, numWorkers int) (*CeleryClient, error) {
	return &CeleryClient{
		broker,
		backend,
		NewCeleryWorker(broker, backend, numWorkers),
	}, nil
}

// Register task
func (cc *CeleryClient) Register(name string, task interface{}) {
	cc.worker.Register(name, task)
}

// StartWorker starts celery workers
func (cc *CeleryClient) StartWorker() {
	cc.worker.StartWorker()
}

// StopWorker stops celery workers
func (cc *CeleryClient) StopWorker() {
	cc.worker.StopWorker()
}

// Delay gets asynchronous result
func (cc *CeleryClient) Delay(task string, args ...interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Args = args
	return cc.delay(celeryTask, "celery", "celery")
}

// DelayKwargs gets asynchronous results with argument map
func (cc *CeleryClient) DelayKwargs(task string, args map[string]interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Kwargs = args
	return cc.delay(celeryTask, "celery", "celery")
}

func (cc *CeleryClient) ApplyAsync(task string, exchange string, routingKey string, kwargs interface{}, args ...interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Args = args
	celeryTask.Kwargs = kwargs
	return cc.delay(celeryTask, exchange, routingKey)

}

func (cc *CeleryClient) delay(task *TaskMessage, exchange string, routingKey string) (*AsyncResult, error) {
	defer releaseTaskMessage(task)
	encodedMessage, err := task.Encode()
	if err != nil {
		return nil, err
	}

	celeryMessage := getCeleryMessage(encodedMessage, exchange, routingKey)
	defer releaseCeleryMessage(celeryMessage)
	err = cc.Broker.SendCeleryMessage(celeryMessage)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		taskID:  task.ID,
		backend: cc.backend,
	}, nil
}

// CeleryTask is an interface that represents actual task
// Passing CeleryTask interface instead of function pointer
// avoids reflection and may have performance gain.
// ResultMessage must be obtained using GetResultMessage()
type CeleryTask interface {

	// ParseKwargs - define a method to parse kwargs
	ParseKwargs(interface{}) error

	// RunTask - define a method to run
	RunTask() (interface{}, error)
}

// AsyncResult is pending result
type AsyncResult struct {
	taskID  string
	backend CeleryBackend
	result  *ResultMessage
}

// Get gets actual result from redis
// It blocks for period of time set by timeout and return error if unavailable
func (ar *AsyncResult) Get(timeout time.Duration) (interface{}, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-timeoutChan:
			err := fmt.Errorf("%v timeout getting result for %s", timeout, ar.taskID)
			return nil, err
		case <-ticker.C:
			val, err := ar.AsyncGet()
			if err != nil {
				continue
			}
			return val, nil
		}
	}
}

// AsyncGet gets actual result from redis and returns nil if not available
func (ar *AsyncResult) AsyncGet() (interface{}, error) {
	if ar.result != nil {
		return ar.result.Result, nil
	}
	// process
	val, err := ar.backend.GetResult(ar.taskID)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, err
	}
	if val.Status != "SUCCESS" {
		return nil, fmt.Errorf("error response status %v", val)
	}
	ar.result = val
	return val.Result, nil
}

// Ready checks if actual result is ready
func (ar *AsyncResult) Ready() (bool, error) {
	if ar.result != nil {
		return true, nil
	}
	val, err := ar.backend.GetResult(ar.taskID)
	if err != nil {
		return false, err
	}
	ar.result = val
	return (val != nil), nil
}
