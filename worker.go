package gocelery

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"reflect"
	"sync"
)

type DeliveredMessage struct {
	messageChannel chan *TaskMessage
	delivery       *amqp.Delivery
	err            error
}

// CeleryWorker represents distributed task worker
type CeleryWorker struct {
	broker          CeleryBroker
	backend         CeleryBackend
	numWorkers      int
	registeredTasks map[string]interface{}
	taskLock        sync.RWMutex
	stopChannel     chan struct{}
	workWG          sync.WaitGroup
	delivered       *DeliveredMessage
}

// NewCeleryWorker returns new celery worker
func NewCeleryWorker(broker CeleryBroker, backend CeleryBackend, numWorkers int) *CeleryWorker {

	return &CeleryWorker{
		broker:          broker,
		backend:         backend,
		numWorkers:      numWorkers,
		registeredTasks: make(map[string]interface{}),
		delivered: &DeliveredMessage{
			messageChannel: make(chan *TaskMessage),
			delivery:       nil,
			err:            nil,
		},
	}
}

func (w *CeleryWorker) consume(x int) {
	trace.Infof("Entering Consume %v...\n", x)

	taskMessage, delivery, err := w.broker.GetTaskMessage()
	w.delivered.messageChannel <- taskMessage
	w.delivered.delivery = delivery
	w.delivered.err = err

	trace.Debugf("Exiting Consume %v...\n", x)

	x++

	go w.consume(x)
}

// StartWorker starts celery worker
func (w *CeleryWorker) StartWorker() {

	w.stopChannel = make(chan struct{})
	w.workWG.Add(w.numWorkers)

	for i := 0; i < w.numWorkers; i++ {
		go func(workerID int) {
			defer w.workWG.Done()

			go w.consume(0)

			for {
				select {
				case <-w.stopChannel:
					log.Printf("WORKER %d received STOP\n", workerID)
					return

				case msg := <-w.delivered.messageChannel:

					log.Printf("WORKER %d received MESSAGE \n", workerID)
					if w.delivered.err != nil || msg == nil {
						continue
					}

					log.Printf("WORKER %d task message received\n", workerID)

					// run task
					resultMsg, err := w.RunTask(msg)
					if err != nil {
						log.Println(err)
						w.delivered.delivery.Nack(false, true)
						continue
					}
					if w.broker.GetAckLate() {
						log.Printf("WORKER %d task completed %v\n", workerID, resultMsg)
						w.delivered.delivery.Ack(false)
					}
					log.Printf("WORKER %d task completed %v\n", workerID, resultMsg)

					defer releaseResultMessage(resultMsg)
				}
			}

			//for {
			//	select {
			//	case <-w.stopChannel:
			//		log.Printf("WORKER %d stop received\n", workerID)
			//		return
			//	default:
			//		log.Printf("WORKER %d check for message\n", workerID)
			//
			//		// process messages
			//		taskMessage, ack, err := w.broker.GetTaskMessage()
			//		if err != nil || taskMessage == nil {
			//			continue
			//		}
			//
			//		log.Printf("WORKER %d task message received\n", workerID)
			//
			//		// run task
			//		resultMsg, err := w.RunTask(taskMessage)
			//		if err != nil {
			//			log.Println(err)
			//			ack.Nack(false, true)
			//			continue
			//		}
			//		if w.broker.GetAckLate() {
			//			log.Printf("WORKER %d task completed %v\n", workerID, resultMsg)
			//			ack.Ack(false)
			//		}
			//		log.Printf("WORKER %d task completed %v\n", workerID, resultMsg)
			//
			//		defer releaseResultMessage(resultMsg)
			//
			//		// push result to backend
			//		//err = w.backend.SetResult(taskMessage.ID, resultMsg)
			//		//if err != nil {
			//		//	log.Println(err)
			//		//	continue
			//		//}
			//	}
			//}
		}(i)
	}
}

// StopWorker stops celery workers
func (w *CeleryWorker) StopWorker() {
	for i := 0; i < w.numWorkers; i++ {
		log.Printf("Sending Stop to WORKER %d \n", i)

		w.stopChannel <- struct{}{}

		log.Printf("Stop Sent to WORKER %d \n", i)

	}
	w.workWG.Wait()
}

// GetNumWorkers returns number of currently running workers
func (w *CeleryWorker) GetNumWorkers() int {
	return w.numWorkers
}

// Register registers tasks (functions)
func (w *CeleryWorker) Register(name string, task interface{}) {
	w.taskLock.Lock()
	w.registeredTasks[name] = task
	w.taskLock.Unlock()
}

// GetTask retrieves registered task
func (w *CeleryWorker) GetTask(name string) interface{} {
	w.taskLock.RLock()
	task, ok := w.registeredTasks[name]
	if !ok {
		w.taskLock.RUnlock()
		return nil
	}
	w.taskLock.RUnlock()
	return task
}

// RunTask runs celery task
func (w *CeleryWorker) RunTask(message *TaskMessage) (*ResultMessage, error) {

	// get task
	task := w.GetTask(message.Task)
	if task == nil {
		return nil, fmt.Errorf("task %s is not registered", message.Task)
	}

	// convert to task interface
	taskInterface, ok := task.(CeleryTask)
	if ok {
		log.Println("using task interface")
		if err := taskInterface.ParseKwargs(message.Kwargs); err != nil {
			return nil, err
		}
		val, err := taskInterface.RunTask()
		if err != nil {
			return nil, err
		}
		return getResultMessage(val), err
	}
	log.Println("using reflection")

	// use reflection to execute function ptr
	taskFunc := reflect.ValueOf(task)
	return runTaskFunc(&taskFunc, message)
}

func runTaskFunc(taskFunc *reflect.Value, message *TaskMessage) (*ResultMessage, error) {

	// check number of arguments
	numArgs := taskFunc.Type().NumIn()
	messageNumArgs := len(message.Args)
	if numArgs != messageNumArgs {
		return nil, fmt.Errorf("Number of task arguments %d does not match number of message arguments %d", numArgs, messageNumArgs)
	}
	// construct arguments
	in := make([]reflect.Value, messageNumArgs)
	for i, arg := range message.Args {
		origType := taskFunc.Type().In(i).Kind()
		msgType := reflect.TypeOf(arg).Kind()
		// special case - convert float64 to int if applicable
		// this is due to json limitation where all numbers are converted to float64
		if origType == reflect.Int && msgType == reflect.Float64 {
			arg = int(arg.(float64))
		}

		in[i] = reflect.ValueOf(arg)
	}

	// call method
	res := taskFunc.Call(in)
	if len(res) == 0 {
		return nil, nil
	}
	//defer releaseResultMessage(resultMessage)
	return getReflectionResultMessage(&res[0]), nil
}
