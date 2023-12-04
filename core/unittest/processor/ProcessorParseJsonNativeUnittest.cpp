// Copyright 2023 iLogtail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include <cstdlib>

#include "common/JsonUtil.h"
#include "config/Config.h"
#include "models/LogEvent.h"
#include "plugin/instance/ProcessorInstance.h"
#include "processor/ProcessorParseJsonNative.h"
#include "unittest/Unittest.h"

namespace logtail {

class ProcessorParseJsonNativeUnittest : public ::testing::Test {
public:
    void SetUp() override { mContext.SetConfigName("project##config_0"); }

    void TestInit();
    void TestProcessJson();
    void TestAddLog();
    void TestProcessEventKeepUnmatch();
    void TestProcessEventDiscardUnmatch();
    void TestProcessJsonContent();
    void TestProcessJsonRaw();

    PipelineContext mContext;
};

UNIT_TEST_CASE(ProcessorParseJsonNativeUnittest, TestInit);

UNIT_TEST_CASE(ProcessorParseJsonNativeUnittest, TestAddLog);

UNIT_TEST_CASE(ProcessorParseJsonNativeUnittest, TestProcessJson);

UNIT_TEST_CASE(ProcessorParseJsonNativeUnittest, TestProcessEventKeepUnmatch);

UNIT_TEST_CASE(ProcessorParseJsonNativeUnittest, TestProcessEventDiscardUnmatch);

UNIT_TEST_CASE(ProcessorParseJsonNativeUnittest, TestProcessJsonContent);

UNIT_TEST_CASE(ProcessorParseJsonNativeUnittest, TestProcessJsonRaw);

void ProcessorParseJsonNativeUnittest::TestInit() {
    // make config
    Json::Value config;
    config["SourceKey"] = "content";
    config["KeepingSourceWhenParseFail"] = true;
    config["KeepingSourceWhenParseSucceed"] = false;
    config["CopingRawLog"] = false;
    config["RenamedSourceKey"] = "rawLog";

    // run function
    ProcessorParseJsonNative& processor = *(new ProcessorParseJsonNative);
    std::string pluginId = "testID";
    ProcessorInstance processorInstance(&processor, pluginId);
    APSARA_TEST_TRUE_FATAL(processorInstance.Init(config, mContext));
}

void ProcessorParseJsonNativeUnittest::TestAddLog() {
    // make config
    Json::Value config;
    config["SourceKey"] = "content";
    ProcessorParseJsonNative& processor = *(new ProcessorParseJsonNative);
    std::string pluginId = "testID";
    ProcessorInstance processorInstance(&processor, pluginId);
    APSARA_TEST_TRUE_FATAL(processorInstance.Init(config, mContext));

    auto sourceBuffer = std::make_shared<SourceBuffer>();
    auto logEvent = LogEvent::CreateEvent(sourceBuffer);
    char key[] = "key";
    char value[] = "value";
    processor.AddLog(key, value, *logEvent);
    // check observability
    APSARA_TEST_EQUAL_FATAL(strlen(key) + strlen(value) + 5, processor.GetContext().GetProcessProfile().logGroupSize);
}

void ProcessorParseJsonNativeUnittest::TestProcessJson() {
    // make config
    Json::Value config;
    config["SourceKey"] = "content";
    config["KeepingSourceWhenParseFail"] = true;
    config["KeepingSourceWhenParseSucceed"] = true;
    config["CopingRawLog"] = true;
    config["RenamedSourceKey"] = "rawLog";

    // make events
    auto sourceBuffer = std::make_shared<SourceBuffer>();
    PipelineEventGroup eventGroup(sourceBuffer);
    std::string inJson = R"({
        "events" :
        [
            {
                "contents" :
                {
                    "content" : "{\"url\": \"POST /PutData?Category=YunOsAccountOpLog HTTP/1.1\",\"time\": \"07/Jul/2022:10:30:28\"}",
                    "__file_offset__": "0"
                },
                "timestampNanosecond" : 0,
                "timestamp" : 12345678901,
                "type" : 1
            },
            {
                "contents" :
                {
                    "content" : "{\"name\":\"Mike\",\"age\":25,\"is_student\":false,\"address\":{\"city\":\"Hangzhou\",\"postal_code\":\"100000\"},\"courses\":[\"Math\",\"English\",\"Science\"],\"scores\":{\"Math\":90,\"English\":85,\"Science\":95}}",
                    "__file_offset__": "0"
                },
                "timestampNanosecond" : 0,
                "timestamp" : 12345678901,
                "type" : 1
            }
        ]
    })";
    eventGroup.FromJsonString(inJson);
    // run function
    ProcessorParseJsonNative& processor = *(new ProcessorParseJsonNative);
    std::string pluginId = "testID";
    ProcessorInstance processorInstance(&processor, pluginId);
    APSARA_TEST_TRUE_FATAL(processorInstance.Init(config, mContext));
    std::vector<PipelineEventGroup> eventGroupList;
    eventGroupList.emplace_back(std::move(eventGroup));
    processorInstance.Process(eventGroupList);
    eventGroup = std::move(eventGroupList[0]);
    // judge result
    std::string expectJson = R"({
        "events" :
        [
            {
                "contents" :
                {
                    "__file_offset__": "0",
                    "rawLog" : "{\"url\": \"POST /PutData?Category=YunOsAccountOpLog HTTP/1.1\",\"time\": \"07/Jul/2022:10:30:28\"}",
                    "time" : "07/Jul/2022:10:30:28",
                    "url" : "POST /PutData?Category=YunOsAccountOpLog HTTP/1.1"
                },
                "timestamp" : 12345678901,
                "timestampNanosecond" : 0,
                "type" : 1
            },
            {
                "contents" :
                {
                    "__file_offset__": "0",
                    "address" : "{\"city\":\"Hangzhou\",\"postal_code\":\"100000\"}",
                    "age":"25",
                    "courses":"[\"Math\",\"English\",\"Science\"]",
                    "is_student":"false",
                    "name":"Mike",
                    "rawLog" : "{\"name\":\"Mike\",\"age\":25,\"is_student\":false,\"address\":{\"city\":\"Hangzhou\",\"postal_code\":\"100000\"},\"courses\":[\"Math\",\"English\",\"Science\"],\"scores\":{\"Math\":90,\"English\":85,\"Science\":95}}",
                    "scores":"{\"Math\":90,\"English\":85,\"Science\":95}"
                },
                "timestamp" : 12345678901,
                "timestampNanosecond" : 0,
                "type" : 1
            }
        ]
    })";
    std::string outJson = eventGroup.ToJsonString();
    APSARA_TEST_STREQ_FATAL(CompactJson(expectJson).c_str(), CompactJson(outJson).c_str());
    APSARA_TEST_GT_FATAL(processorInstance.mProcTimeMS->GetValue(), 0);
}

void ProcessorParseJsonNativeUnittest::TestProcessJsonContent() {
    // make config
    Json::Value config;
    config["SourceKey"] = "content";
    config["KeepingSourceWhenParseFail"] = true;
    config["KeepingSourceWhenParseSucceed"] = true;
    config["CopingRawLog"] = true;
    config["RenamedSourceKey"] = "rawLog";

    // make events
    auto sourceBuffer = std::make_shared<SourceBuffer>();
    PipelineEventGroup eventGroup(sourceBuffer);
    std::string inJson = R"({
        "events" :
        [
            {
                "contents" :
                {
                    "content" : "{\"content\":\"content_test\",\"name\":\"Mike\",\"age\":25,\"is_student\":false,\"address\":{\"city\":\"Hangzhou\",\"postal_code\":\"100000\"},\"courses\":[\"Math\",\"English\",\"Science\"],\"scores\":{\"Math\":90,\"English\":85,\"Science\":95}}",
                    "__file_offset__": "0"
                },
                "timestampNanosecond" : 0,
                "timestamp" : 12345678901,
                "type" : 1
            }
        ]
    })";
    eventGroup.FromJsonString(inJson);
    // run function
    ProcessorParseJsonNative& processor = *(new ProcessorParseJsonNative);
    std::string pluginId = "testID";
    ProcessorInstance processorInstance(&processor, pluginId);
    APSARA_TEST_TRUE_FATAL(processorInstance.Init(config, mContext));
    std::vector<PipelineEventGroup> eventGroupList;
    eventGroupList.emplace_back(std::move(eventGroup));
    processorInstance.Process(eventGroupList);
    eventGroup = std::move(eventGroupList[0]);
    // judge result
    std::string expectJson = R"({
        "events" :
        [
            {
                "contents" :
                {
                    "__file_offset__": "0",
                    "address" : "{\"city\":\"Hangzhou\",\"postal_code\":\"100000\"}",
                    "age":"25",
                    "content":"content_test",
                    "courses":"[\"Math\",\"English\",\"Science\"]",
                    "is_student":"false",
                    "name":"Mike",
                    "rawLog" : "{\"content\":\"content_test\",\"name\":\"Mike\",\"age\":25,\"is_student\":false,\"address\":{\"city\":\"Hangzhou\",\"postal_code\":\"100000\"},\"courses\":[\"Math\",\"English\",\"Science\"],\"scores\":{\"Math\":90,\"English\":85,\"Science\":95}}",
                    "scores":"{\"Math\":90,\"English\":85,\"Science\":95}"
                },
                "timestamp" : 12345678901,
                "timestampNanosecond" : 0,
                "type" : 1
            }
        ]
    })";
    std::string outJson = eventGroup.ToJsonString();
    APSARA_TEST_STREQ_FATAL(CompactJson(expectJson).c_str(), CompactJson(outJson).c_str());
    APSARA_TEST_GT_FATAL(processorInstance.mProcTimeMS->GetValue(), 0);
}

void ProcessorParseJsonNativeUnittest::TestProcessJsonRaw() {
    // make config
    Json::Value config;
    config["SourceKey"] = "content";
    config["KeepingSourceWhenParseFail"] = true;
    config["KeepingSourceWhenParseSucceed"] = true;
    config["CopingRawLog"] = true;
    config["RenamedSourceKey"] = "rawLog";

    // make events
    auto sourceBuffer = std::make_shared<SourceBuffer>();
    PipelineEventGroup eventGroup(sourceBuffer);
    std::string inJson = R"({
        "events" :
        [
            {
                "contents" :
                {
                    "content" : "{\"rawLog\":\"content_test\",\"name\":\"Mike\",\"age\":25,\"is_student\":false,\"address\":{\"city\":\"Hangzhou\",\"postal_code\":\"100000\"},\"courses\":[\"Math\",\"English\",\"Science\"],\"scores\":{\"Math\":90,\"English\":85,\"Science\":95}}",
                    "__file_offset__": "0"
                },
                "timestampNanosecond" : 0,
                "timestamp" : 12345678901,
                "type" : 1
            }
        ]
    })";
    eventGroup.FromJsonString(inJson);
    // run function
    ProcessorParseJsonNative& processor = *(new ProcessorParseJsonNative);
    std::string pluginId = "testID";
    ProcessorInstance processorInstance(&processor, pluginId);
    APSARA_TEST_TRUE_FATAL(processorInstance.Init(config, mContext));
    std::vector<PipelineEventGroup> eventGroupList;
    eventGroupList.emplace_back(std::move(eventGroup));
    processorInstance.Process(eventGroupList);
    eventGroup = std::move(eventGroupList[0]);
    // judge result
    std::string expectJson = R"({
        "events" :
        [
            {
                "contents" :
                {
                    "__file_offset__": "0",
                    "address" : "{\"city\":\"Hangzhou\",\"postal_code\":\"100000\"}",
                    "age":"25",
                    "courses":"[\"Math\",\"English\",\"Science\"]",
                    "is_student":"false",
                    "name":"Mike",
                    "rawLog" : "content_test",
                    "scores":"{\"Math\":90,\"English\":85,\"Science\":95}"
                },
                "timestamp" : 12345678901,
                "timestampNanosecond" : 0,
                "type" : 1
            }
        ]
    })";
    std::string outJson = eventGroup.ToJsonString();
    APSARA_TEST_STREQ_FATAL(CompactJson(expectJson).c_str(), CompactJson(outJson).c_str());
    APSARA_TEST_GT_FATAL(processorInstance.mProcTimeMS->GetValue(), 0);
}

void ProcessorParseJsonNativeUnittest::TestProcessEventKeepUnmatch() {
    // make config
    // make config
    Json::Value config;
    config["SourceKey"] = "content";
    config["KeepingSourceWhenParseFail"] = true;
    config["KeepingSourceWhenParseSucceed"] = false;
    config["CopingRawLog"] = false;
    config["RenamedSourceKey"] = "rawLog";

    // make events
    auto sourceBuffer = std::make_shared<SourceBuffer>();
    PipelineEventGroup eventGroup(sourceBuffer);
    std::string inJson = R"({
        "events" :
        [
            {
                "contents" :
                {
                    "content" : "{\"url\": \"POST /PutData?Category=YunOsAccountOpLog HTTP/1.1\",\"time\": \"07/Jul/2022:10:30:28\"",
                    "__file_offset__": "0"
                },
                "timestampNanosecond" : 0,
                "timestamp" : 12345678901,
                "type" : 1
            }
        ]
    })";
    eventGroup.FromJsonString(inJson);
    // run function
    ProcessorParseJsonNative& processor = *(new ProcessorParseJsonNative);
    std::string pluginId = "testID";
    ProcessorInstance processorInstance(&processor, pluginId);
    APSARA_TEST_TRUE_FATAL(processorInstance.Init(config, mContext));
    std::vector<PipelineEventGroup> eventGroupList;
    eventGroupList.emplace_back(std::move(eventGroup));
    processorInstance.Process(eventGroupList);
    eventGroup = std::move(eventGroupList[0]);

    int count = 1;

    // check observablity
    APSARA_TEST_EQUAL_FATAL(count, processor.GetContext().GetProcessProfile().parseFailures);

    APSARA_TEST_EQUAL_FATAL(count, processorInstance.mProcInRecordsTotal->GetValue());
    std::string expectValue
        = "{\"url\": \"POST /PutData?Category=YunOsAccountOpLog HTTP/1.1\",\"time\": \"07/Jul/2022:10:30:28\"";
    APSARA_TEST_EQUAL_FATAL((expectValue.length()) * count, processor.mProcParseInSizeBytes->GetValue());
    APSARA_TEST_EQUAL_FATAL(count, processorInstance.mProcOutRecordsTotal->GetValue());
    expectValue = "rawLog{\"url\": \"POST /PutData?Category=YunOsAccountOpLog HTTP/1.1\",\"time\": "
                  "\"07/Jul/2022:10:30:28\"";
    APSARA_TEST_EQUAL_FATAL((expectValue.length()) * count, processor.mProcParseOutSizeBytes->GetValue());

    APSARA_TEST_EQUAL_FATAL(0, processor.mProcDiscardRecordsTotal->GetValue());

    APSARA_TEST_EQUAL_FATAL(count, processor.mProcParseErrorTotal->GetValue());

    // judge result
    std::string expectJson = R"({
        "events" :
        [
            {
                "contents" :
                {
                    "__file_offset__": "0",
                    "rawLog" : "{\"url\": \"POST /PutData?Category=YunOsAccountOpLog HTTP/1.1\",\"time\": \"07/Jul/2022:10:30:28\""
                },
                "timestamp" : 12345678901,
                "timestampNanosecond" : 0,
                "type" : 1
            }
        ]
    })";
    std::string outJson = eventGroup.ToJsonString();
    APSARA_TEST_STREQ_FATAL(CompactJson(expectJson).c_str(), CompactJson(outJson).c_str());
    APSARA_TEST_GT_FATAL(processorInstance.mProcTimeMS->GetValue(), 0);
}

void ProcessorParseJsonNativeUnittest::TestProcessEventDiscardUnmatch() {
    // make config
    Json::Value config;
    config["SourceKey"] = "content";
    config["KeepingSourceWhenParseFail"] = false;
    config["KeepingSourceWhenParseSucceed"] = false;
    config["CopingRawLog"] = false;
    config["RenamedSourceKey"] = "rawLog";

    // make events
    auto sourceBuffer = std::make_shared<SourceBuffer>();
    PipelineEventGroup eventGroup(sourceBuffer);
    std::string inJson = R"({
        "events" :
        [
            {
                "contents" :
                {
                    "content" : "{\"url\": \"POST /PutData?Category=YunOsAccountOpLog HTTP/1.1\",\"time\": \"07/Jul/2022:10:30:28\"",
                    "__file_offset__": "0"
                },
                "timestampNanosecond" : 0,
                "timestamp" : 12345678901,
                "type" : 1
            }
        ]
    })";
    eventGroup.FromJsonString(inJson);
    // run function
    ProcessorParseJsonNative& processor = *(new ProcessorParseJsonNative);
    std::string pluginId = "testID";
    ProcessorInstance processorInstance(&processor, pluginId);
    APSARA_TEST_TRUE_FATAL(processorInstance.Init(config, mContext));
    std::vector<PipelineEventGroup> eventGroupList;
    eventGroupList.emplace_back(std::move(eventGroup));
    processorInstance.Process(eventGroupList);
    eventGroup = std::move(eventGroupList[0]);

    int count = 1;

    // check observablity
    APSARA_TEST_EQUAL_FATAL(count, processor.GetContext().GetProcessProfile().parseFailures);

    APSARA_TEST_EQUAL_FATAL(count, processorInstance.mProcInRecordsTotal->GetValue());
    std::string expectValue
        = "{\"url\": \"POST /PutData?Category=YunOsAccountOpLog HTTP/1.1\",\"time\": \"07/Jul/2022:10:30:28\"";
    APSARA_TEST_EQUAL_FATAL((expectValue.length()) * count, processor.mProcParseInSizeBytes->GetValue());
    // discard unmatch, so output is 0
    APSARA_TEST_EQUAL_FATAL(0, processorInstance.mProcOutRecordsTotal->GetValue());
    APSARA_TEST_EQUAL_FATAL(0, processor.mProcParseOutSizeBytes->GetValue());
    APSARA_TEST_EQUAL_FATAL(count, processor.mProcDiscardRecordsTotal->GetValue());

    APSARA_TEST_EQUAL_FATAL(count, processor.mProcParseErrorTotal->GetValue());

    std::string outJson = eventGroup.ToJsonString();
    APSARA_TEST_STREQ_FATAL("null", CompactJson(outJson).c_str());
    APSARA_TEST_GT_FATAL(processorInstance.mProcTimeMS->GetValue(), 0);
}

} // namespace logtail

UNIT_TEST_MAIN
