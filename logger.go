package gocelery

import "log"

var trace = NewCeleryLogger()

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Panicf(format string, args ...interface{})
	//
	Debug(args ...interface{})
	Info(args ...interface{})
	Print(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Fatal(args ...interface{})
	Panic(args ...interface{})
}

type CeleryLogger struct {
	logger Logger
}

func NewCeleryLogger() *CeleryLogger {
	return &CeleryLogger{
		logger: nil,
	}
}

func SetLogger(logger Logger) {
	trace.logger = logger
}

func (c *CeleryLogger) Infof(format string, args ...interface{}) {
	if c.logger != nil {
		c.logger.Infof(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func (c *CeleryLogger) Debugf(format string, args ...interface{}) {
	if c.logger != nil {
		c.logger.Debugf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func (c *CeleryLogger) Warnf(format string, args ...interface{}) {
	if c.logger != nil {
		c.logger.Warnf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func (c *CeleryLogger) Errorf(format string, args ...interface{}) {
	if c.logger != nil {
		c.logger.Errorf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func (c *CeleryLogger) Fatalf(format string, args ...interface{}) {
	if c.logger != nil {
		c.logger.Fatalf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func (c *CeleryLogger) Panicf(format string, args ...interface{}) {
	if c.logger != nil {
		c.logger.Panicf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func (c *CeleryLogger) Debug(args ...interface{}) {
	if c.logger != nil {
		c.logger.Debug(args...)
	} else {
		log.Print(args...)
	}
}

func (c *CeleryLogger) Info(args ...interface{}) {
	if c.logger != nil {
		c.logger.Info(args...)
	} else {
		log.Print(args...)
	}
}

func (c *CeleryLogger) Print(args ...interface{}) {
	if c.logger != nil {
		c.logger.Print(args...)
	} else {
		log.Print(args...)
	}
}

func (c *CeleryLogger) Warn(args ...interface{}) {
	if c.logger != nil {
		c.logger.Warn(args...)
	} else {
		log.Print(args...)
	}
}

func (c *CeleryLogger) Error(args ...interface{}) {
	if c.logger != nil {
		c.logger.Error(args...)
	} else {
		log.Print(args...)
	}
}

func (c *CeleryLogger) Fatal(args ...interface{}) {
	if c.logger != nil {
		c.logger.Fatal(args...)
	} else {
		log.Print(args...)
	}
}

func (c *CeleryLogger) Panic(args ...interface{}) {
	if c.logger != nil {
		c.logger.Panic(args...)
	} else {
		log.Print(args...)
	}
}
