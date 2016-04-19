package server

import (
	"git.apache.org/thrift.git/lib/go/thrift"
)

var transport_factory = thrift.NewTBufferedTransportFactory(8192)
var protocol_factory = thrift.NewTBinaryProtocolFactoryDefault()

const MAIN_CHANNEL_BUFFER_SIZE = int(10e5)
