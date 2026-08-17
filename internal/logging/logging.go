package logging

import (
	"os"

	"github.com/sirupsen/logrus"
)

func Setup(serviceName string) *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
		},
	})
	logger.SetReportCaller(false)
	logger.SetLevel(logrus.InfoLevel)
	logger.AddHook(serviceNameHook{serviceName: serviceName})
	logger.Info("logger initialized")
	return logger
}

type serviceNameHook struct {
	serviceName string
}

func (h serviceNameHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h serviceNameHook) Fire(entry *logrus.Entry) error {
	entry.Data["service_name"] = h.serviceName
	return nil
}
