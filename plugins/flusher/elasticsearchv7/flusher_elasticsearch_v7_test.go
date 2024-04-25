// Copyright 2023 iLogtail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package elasticsearchv7

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
)

func TestGetIndexKeys(t *testing.T) {
	Convey("Given an empty index", t, func() {
		flusher := &FlusherElasticSearch{
			Index: "",
		}
		Convey("When getIndexKeys is called", func() {
			keys, isDynamicIndex, err := flusher.getIndexKeys()
			Convey("Then the keys should not be extracted correctly", func() {
				So(err, ShouldNotBeNil)
				So(isDynamicIndex, ShouldBeFalse)
				So(keys, ShouldBeNil)
			})
		})
	})
	Convey("Given a normal index", t, func() {
		flusher := &FlusherElasticSearch{
			Index: "normal_index",
		}
		Convey("When getIndexKeys is called", func() {
			keys, isDynamicIndex, err := flusher.getIndexKeys()
			Convey("Then the keys should be extracted correctly", func() {
				So(err, ShouldBeNil)
				So(isDynamicIndex, ShouldBeFalse)
				So(len(keys), ShouldEqual, 0)
			})
		})
	})
	Convey("Given a variable index", t, func() {
		flusher := &FlusherElasticSearch{
			Index: "index_${var}",
		}
		Convey("When getIndexKeys is called", func() {
			keys, isDynamicIndex, err := flusher.getIndexKeys()
			Convey("Then the keys should be extracted correctly", func() {
				So(err, ShouldBeNil)
				So(isDynamicIndex, ShouldBeFalse)
				So(len(keys), ShouldEqual, 0)
			})
		})
	})
	Convey("Given a field dynamic index expression", t, func() {
		flusher := &FlusherElasticSearch{
			Index: "index_%{content.field}",
		}
		Convey("When getIndexKeys is called", func() {
			keys, isDynamicIndex, err := flusher.getIndexKeys()
			Convey("Then the keys should be extracted correctly", func() {
				So(err, ShouldBeNil)
				So(isDynamicIndex, ShouldBeTrue)
				So(len(keys), ShouldEqual, 1)
				So(keys[0], ShouldEqual, "content.field")
			})
		})
	})
	Convey("Given a timestamp dynamic index expression", t, func() {
		flusher := &FlusherElasticSearch{
			Index: "index_%{+yyyyMM}",
		}
		Convey("When getIndexKeys is called", func() {
			keys, isDynamicIndex, err := flusher.getIndexKeys()
			Convey("Then the keys should be extracted correctly", func() {
				So(err, ShouldBeNil)
				So(isDynamicIndex, ShouldBeTrue)
				So(len(keys), ShouldEqual, 0)
			})
		})
	})
	Convey("Given a composite dynamic index expression", t, func() {
		flusher := &FlusherElasticSearch{
			Index: "index_%{content.field}_%{tag.host.ip}_%{+yyyyMMdd}",
		}
		Convey("When getIndexKeys is called", func() {
			keys, isDynamicIndex, err := flusher.getIndexKeys()
			Convey("Then the keys should be extracted correctly", func() {
				So(err, ShouldBeNil)
				So(isDynamicIndex, ShouldBeTrue)
				So(len(keys), ShouldEqual, 2)
				So(keys[0], ShouldEqual, "content.field")
				So(keys[1], ShouldEqual, "tag.host.ip")
			})
		})
	})
}

func TestTimestamp(t *testing.T) {
	//dateString := "2021-04-25T12:30:00Z"
	//t1, err := time.Parse(time.RFC3339, dateString)
	//if err != nil {
	//	fmt.Printf("Error parsing date string: %s\n", err)
	//	return
	//}
	//esDate := t1.Format(time.RFC3339)
	//fmt.Println("es date string:", esDate)

	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://logses.mddcloud.com.cn:9200",
		},
		Username: "elastic",
		Password: "TvbC@Es2020",
	}
	es, _ := elasticsearch.NewClient(cfg)

	bulkBody := strings.Builder{}
	bulkBody.WriteString(`{"index": {"_index": "logtail-test"}}` + "\n")
	bulkBody.WriteString(`{"host.ip":"172.16.228.170","host.name":"mdd-log-service-v1-55dd5b857-5vxbl","log.file.path":"/data/tvbcserver/mdd/logs/app/info.log","message":"mdd-log-service [main] INFO  c.m.s.l.MlsApplication 55 Starting MlsApplication v0.0.1-SNAPSHOT on mdd-log-service-v1-55dd5b857-5vxbl with PID 1 (/data/tvbcserver/mdd/mdd-log-service.jar started by tvbcserver in /data/tvbcserver/mdd)","time":1713956818, "@timestamp": "2024-04-23T00:00:00Z"}` + "\n")
	bulkBody.WriteString(`{"index": {"_index": "logtail-test"}}` + "\n")
	bulkBody.WriteString(`{"host.ip":"172.16.228.171","host.name":"mdd-log-service-v1-55dd5b857-5vxbl","log.file.path":"/data/tvbcserver/mdd/logs/app/info.log","message":"mdd-log-service [main] INFO  c.m.s.l.MlsApplication 55 Starting MlsApplication v0.0.1-SNAPSHOT on mdd-log-service-v1-55dd5b857-5vxbl with PID 1 (/data/tvbcserver/mdd/mdd-log-service.jar started by tvbcserver in /data/tvbcserver/mdd)","time":1713956818, "@timestamp": "2024-04-23T00:00:00Z"}` + "\n")
	//bulkBody.WriteString(`{"@timestamp": "2024-04-23T00:00:00Z", "timestamp": "2024-04-24 19:06:52.832"}` + "\n")
	bulkBody.WriteString("\n")

	bulkRequest := esapi.BulkRequest{
		Body: strings.NewReader(bulkBody.String()),
	}
	res, _ := bulkRequest.Do(context.Background(), es)

	// 检查响应状态
	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			fmt.Printf("Error parsing the error response: %s\n", err)
			return
		}
		fmt.Printf("Error response: %s\n", e["error"].(map[string]interface{})["reason"])
		return
	}
}
