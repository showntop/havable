package ha

import (
	logger "github.com/sirupsen/logrus"
)

type ALogger struct {
}

func (al *ALogger) Printf(format string, b ...interface{}) {
	logger.Infof(format, b...)
}
