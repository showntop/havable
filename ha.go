package ha

import (
	"errors"
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	logger "github.com/sirupsen/logrus"
)

const (
	MASTER_ZNODE_NAME = "master"
)

type HAManager struct {
	option   *Option
	RootPath string
	zkConn   *zk.Conn
}

// var (
// 	ham *HAManager
// )

func New(option *Option) (*HAManager, error) {
	ham := &HAManager{}
	ham.option = option
	ham.RootPath = option.RootPath

	var connChan <-chan zk.Event
	var err error
	ham.zkConn, connChan, err = zk.Connect(option.ZkHosts, time.Second)
	if err != nil {
		return ham, err
	}
	// 等待连接成功
	for {
		isConnected := false
		select {
		case connEvent := <-connChan:
			if connEvent.State == zk.StateConnected {
				isConnected = true
				return ham, nil
			}
		case _ = <-time.After(time.Second * 3): // 3秒仍未连接成功则返回连接超时
			return ham, fmt.Errorf("connect to zookeeper server %s timeout!", option.ZkHosts)
		}
		if isConnected {
			break
		}
	}
	return ham, nil
}

func (ham *HAManager) AttackMaster() error {
	masterPath := ham.RootPath //+ MASTER_ZNODE_NAME
	logger.Debugln(masterPath)

	path, err := ham.zkConn.Create(masterPath, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return fmt.Errorf("attack master failure, %s", err)
	}

	if path == masterPath { // 返回的path表示在zookeeper上创建的znode路径
		logger.Debugln(masterPath)
		return nil
	} else {
		return errors.New("Create returned different path " + masterPath + " != " + path)
	}
}

func (ham *HAManager) WatchMaster() {
	// watch zk根znode下面的子znode，当有连接断开时，对应znode被删除，触发事件后重新选举
	children, state, childCh, err := ham.zkConn.ChildrenW(ham.RootPath)
	if err != nil {
		logger.Errorln("|System| watch children error, ", err)
	}
	logger.Infoln("|System| watch children result, ", children, state)
	for {
		select {
		case childEvent := <-childCh:
			if childEvent.Type == zk.EventNodeDeleted {
				if err := ham.AttackMaster(); err != nil {
					logger.Errorln("|System| attack new master error, ", err)
				}
			}
		}
	}
}
