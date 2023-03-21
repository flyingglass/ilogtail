package pyroscope

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sort"
	"testing"

	"github.com/alibaba/ilogtail/helper"
	"github.com/alibaba/ilogtail/plugins/test"

	"github.com/pyroscope-io/pyroscope/pkg/structs/transporttrie"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_DecodeTire(t *testing.T) {

	type value struct {
		k string
		v uint64
	}
	values := []value{
		{"foo;bar;baz", 1},
		{"foo;bar;baz;a", 1},
		{"foo;bar;baz;b", 1},
		{"foo;bar;baz;c", 1},

		{"foo;bar;bar", 1},

		{"foo;bar;qux", 1},

		{"foo;bax;bar", 1},

		{"zoo;boo", 1},
		{"zoo;bao", 1},
	}

	trie := transporttrie.New()
	for _, v := range values {
		trie.Insert([]byte(v.k), v.v)
	}
	var buf bytes.Buffer
	trie.Serialize(&buf)
	println(buf.String())
	request, err := http.NewRequest("POST", "http://localhost:8080?aggregationType=sum&from=1673495500&name=demo.cpu{a=b}&sampleRate=100&spyName=ebpfspy&units=samples&until=1673495510", &buf)
	request.Header.Set("Content-Type", "binary/octet-stream+trie")
	assert.NoError(t, err)
	d := new(Decoder)
	logs, err := d.Decode(buf.Bytes(), request, map[string]string{"cluster": "sls-mall"})
	assert.NoError(t, err)
	assert.True(t, len(logs) == 9)
	log := logs[1]
	require.Equal(t, test.ReadLogVal(log, "name"), "baz")
	require.Equal(t, test.ReadLogVal(log, "stack"), "bar\nfoo")
	require.Equal(t, test.ReadLogVal(log, "language"), "ebpf")
	require.Equal(t, test.ReadLogVal(log, "type"), "profile_cpu")
	require.Equal(t, test.ReadLogVal(log, "units"), "nanoseconds")
	require.Equal(t, test.ReadLogVal(log, "valueTypes"), "cpu")
	require.Equal(t, test.ReadLogVal(log, "aggTypes"), "sum")
	require.Equal(t, test.ReadLogVal(log, "dataType"), "CallStack")
	require.Equal(t, test.ReadLogVal(log, "durationNs"), "10000000000")
	require.Equal(t, test.ReadLogVal(log, "labels"), "{\"__name__\":\"demo\",\"a\":\"b\",\"cluster\":\"sls-mall\"}")
	require.Equal(t, test.ReadLogVal(log, "val"), "10000000.00")
}

func TestDecoder_DecodePprofCumulative(t *testing.T) {
	data, err := ioutil.ReadFile("test/dump_pprof_mem_data")
	require.NoError(t, err)
	var length uint32
	buffer := bytes.NewBuffer(data)
	require.NoError(t, binary.Read(buffer, binary.BigEndian, &length))
	data = data[4 : 4+int(length)]
	var d helper.DumpData
	require.NoError(t, json.Unmarshal(data, &d))
	request, err := http.NewRequest("POST", d.Req.URL, bytes.NewReader(d.Req.Body))
	require.NoError(t, err)
	request.Header = d.Req.Header
	dec := new(Decoder)
	logs, err := dec.Decode(d.Req.Body, request, map[string]string{"cluster": "sls-mall"})
	require.NoError(t, err)
	require.Equal(t, len(logs), 4)
	sort.Slice(logs, func(i, j int) bool {
		if test.ReadLogVal(logs[i], "name") < test.ReadLogVal(logs[j], "name") {
			return true
		} else if test.ReadLogVal(logs[i], "name") == test.ReadLogVal(logs[j], "name") {
			return test.ReadLogVal(logs[i], "valueTypes") < test.ReadLogVal(logs[j], "valueTypes")
		}
		return false
	})
	require.Equal(t, test.ReadLogVal(logs[0], "name"), "compress/flate.NewWriter /Users/evan/sdk/go1.19.4/src/compress/flate/deflate.go")
	require.Equal(t, test.ReadLogVal(logs[0], "valueTypes"), "alloc_objects")
	require.Equal(t, test.ReadLogVal(logs[0], "val"), "1.00")

	require.Equal(t, test.ReadLogVal(logs[1], "name"), "compress/flate.NewWriter /Users/evan/sdk/go1.19.4/src/compress/flate/deflate.go")
	require.Equal(t, test.ReadLogVal(logs[1], "valueTypes"), "alloc_space")
	require.Equal(t, test.ReadLogVal(logs[1], "val"), "924248.00")

	require.Equal(t, test.ReadLogVal(logs[2], "name"), "runtime/pprof.WithLabels /Users/evan/sdk/go1.19.4/src/runtime/pprof/label.go")
	require.Equal(t, test.ReadLogVal(logs[2], "valueTypes"), "alloc_objects")
	require.Equal(t, test.ReadLogVal(logs[2], "val"), "1820.00")

	require.Equal(t, test.ReadLogVal(logs[3], "name"), "runtime/pprof.WithLabels /Users/evan/sdk/go1.19.4/src/runtime/pprof/label.go")
	require.Equal(t, test.ReadLogVal(logs[3], "valueTypes"), "alloc_space")
	require.Equal(t, test.ReadLogVal(logs[3], "val"), "524432.00")
}