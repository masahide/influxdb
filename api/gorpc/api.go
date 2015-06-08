package gorpc

import (
	"fmt"
	"net"
	"net/rpc"
	"os"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/api"
	"github.com/influxdb/influxdb/cluster"
	. "github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/protocol"
)

type Server struct {
	network       string
	listenAddress string
	shutdown      chan bool
	InfluxGoRpc
}

type AuthParam struct {
	Database string
	User     string
	Password string
}

type Series []SerializedSeries

type InfluxGoRpc struct {
	Database      string
	User          User
	coordinator   api.Coordinator
	clusterConfig *cluster.ClusterConfiguration
}

type Result struct{}

func NewServer(network, listenAddress string, coord api.Coordinator, clusterConfig *cluster.ClusterConfiguration) *Server {
	self := &Server{}
	self.network = network
	self.listenAddress = listenAddress
	self.coordinator = coord
	self.shutdown = make(chan bool, 1)
	self.clusterConfig = clusterConfig

	return self
}

func (self *Server) ListenAndServe() {

	if self.network == "unix" {
		if info, err := os.Stat(self.listenAddress); err == nil && (info.Mode()&os.ModeSocket == os.ModeSocket) {
			os.Remove(self.listenAddress)
		}
	}
	listener, err := net.Listen(self.network, self.listenAddress)
	if err != nil {
		log.Error("GoRPCServer: Listen: ", err)
		return
	}
	defer listener.Close()
	if self.network == "unix" {
		os.Chmod(self.listenAddress, os.ModeSocket|0777)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		i := self.InfluxGoRpc
		sv := rpc.NewServer()
		sv.Register(&i)
		sv.ServeConn(conn)
		conn.Close()
	}
}

func (i *InfluxGoRpc) Auth(a *AuthParam, reply *Result) error {
	var err error
	i.User, err = i.clusterConfig.AuthenticateDbUser(a.Database, a.User, a.Password)
	if err != nil {
		i.User = nil
		return err
	}
	i.Database = a.Database
	return nil
}

func (i *InfluxGoRpc) Post(ss *Series, reply *Result) error {
	for _, s := range *ss {
		if len(s.Points) == 0 {
			continue
		}

		series, err := convertToDataStoreSeries(&s, SecondPrecision)
		if err != nil {
			log.Error("GoRPC cannot convert received data: ", err)
			return err
		}

		serie := []*protocol.Series{series}
		err = i.coordinator.WriteSeriesData(i.User, i.Database, serie)
		if err != nil {
			log.Error("GoRPC cannot write data: ", err)
			return err
		}
	}
	return nil
}

func hasDuplicates(ss []string) bool {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		if _, ok := m[s]; ok {
			return true
		}
		m[s] = struct{}{}
	}
	return false
}
func convertToDataStoreSeries(s ApiSeries, precision TimePrecision) (*protocol.Series, error) {
	points := make([]*protocol.Point, 0, len(s.GetPoints()))
	if hasDuplicates(s.GetColumns()) {
		return nil, fmt.Errorf("Cannot have duplicate field names")
	}

	for _, point := range s.GetPoints() {
		if len(point) != len(s.GetColumns()) {
			return nil, fmt.Errorf("invalid payload")
		}

		values := make([]*protocol.FieldValue, 0, len(point))
		var timestamp *int64
		var sequence *uint64

		for idx, field := range s.GetColumns() {

			value := point[idx]
			if field == "time" {
				switch x := value.(type) {
				case float64:
					_timestamp := int64(x)
					switch precision {
					case SecondPrecision:
						_timestamp *= 1000
						fallthrough
					case MillisecondPrecision:
						_timestamp *= 1000
					}
					timestamp = &_timestamp
					continue
				default:
					return nil, fmt.Errorf("time field must be float but is %T (%v)", value, value)
				}
			}

			if field == "sequence_number" {
				switch x := value.(type) {
				case float64:
					_sequenceNumber := uint64(x)
					sequence = &_sequenceNumber
					continue
				default:
					return nil, fmt.Errorf("sequence_number field must be float but is %T (%v)", value, value)
				}
			}

			switch v := value.(type) {
			case string:
				values = append(values, &protocol.FieldValue{StringValue: &v})
			case int:
				i := int64(v)
				values = append(values, &protocol.FieldValue{Int64Value: &i})
			case int8:
				i := int64(v)
				values = append(values, &protocol.FieldValue{Int64Value: &i})
			case int16:
				i := int64(v)
				values = append(values, &protocol.FieldValue{Int64Value: &i})
			case int32:
				i := int64(v)
				values = append(values, &protocol.FieldValue{Int64Value: &i})
			case int64:
				i := int64(v)
				values = append(values, &protocol.FieldValue{Int64Value: &i})
			case uint:
				i := int64(v)
				values = append(values, &protocol.FieldValue{Int64Value: &i})
			case uint8:
				i := int64(v)
				values = append(values, &protocol.FieldValue{Int64Value: &i})
			case uint16:
				i := int64(v)
				values = append(values, &protocol.FieldValue{Int64Value: &i})
			case uint32:
				i := int64(v)
				values = append(values, &protocol.FieldValue{Int64Value: &i})
			case float32:
				f := float64(v)
				values = append(values, &protocol.FieldValue{DoubleValue: &f})
			case float64:
				f := float64(v)
				values = append(values, &protocol.FieldValue{DoubleValue: &f})
			case bool:
				values = append(values, &protocol.FieldValue{BoolValue: &v})
			case nil:
				values = append(values, &protocol.FieldValue{IsNull: &TRUE})
			default:
				// if we reached this line then the dynamic type didn't match
				return nil, fmt.Errorf("Unknown type %T", value)
			}
		}
		points = append(points, &protocol.Point{
			Values:         values,
			Timestamp:      timestamp,
			SequenceNumber: sequence,
		})
	}

	fields := removeTimestampFieldDefinition(s.GetColumns())

	series := &protocol.Series{
		Name:   protocol.String(s.GetName()),
		Fields: fields,
		Points: points,
	}
	return series, nil
}

func removeTimestampFieldDefinition(fields []string) []string {
	fields = removeField(fields, "time")
	return removeField(fields, "sequence_number")
}
func removeField(fields []string, name string) []string {
	index := -1
	for idx, field := range fields {
		if field == name {
			index = idx
			break
		}
	}

	if index == -1 {
		return fields
	}

	return append(fields[:index], fields[index+1:]...)
}
