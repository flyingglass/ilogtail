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

#include "input/InputFile.h"

#include <filesystem>

#include "app_config/AppConfig.h"
#include "config_manager/ConfigManager.h"
#include "common/JsonUtil.h"
#include "common/LogtailCommonFlags.h"
#include "common/ParamExtractor.h"
// #include "file/FileServer.h"
#include "pipeline/Pipeline.h"

using namespace std;

DEFINE_FLAG_INT32(search_checkpoint_default_dir_depth, "0 means only search current directory", 0);
DEFINE_FLAG_INT32(max_exactly_once_concurrency, "", 512);
DEFINE_FLAG_INT32(default_plugin_log_queue_size, "", 10);

namespace logtail {

const string InputFile::sName = "input_file";

InputFile::InputFile()
    : mMaxCheckpointDirSearchDepth(static_cast<uint32_t>(INT32_FLAG(search_checkpoint_default_dir_depth))) {
}

bool InputFile::Init(const Json::Value& config, Json::Value& optionalGoPipeline) {
    string errorMsg;

    mFileDiscovery.Init(config, *mContext, sName);

    // EnableContainerDiscovery
    if (!GetOptionalBoolParam(config, "EnableContainerDiscovery", mEnableContainerDiscovery, errorMsg)) {
        PARAM_WARNING_DEFAULT(mContext->GetLogger(), errorMsg, false, sName, mContext->GetConfigName());
    } else if (mEnableContainerDiscovery && !AppConfig::GetInstance()->IsPurageContainerMode()) {
        PARAM_ERROR(mContext->GetLogger(),
                    "iLogtail is not in container, but container discovery is required",
                    sName,
                    mContext->GetConfigName());
    }
    
    if (mEnableContainerDiscovery) {
        mFileDiscovery.SetEnableContainerDiscoveryFlag(true);
        mContainerDiscovery.Init(config, *mContext, sName);
        GenerateContainerMetaFetchingGoPipeline(optionalGoPipeline);

        // 过渡使用
        auto allContainers = ConfigManager::GetInstance()->GetAllContainerInfo();
        auto iter = allContainers.find(mContext->GetConfigName());
        if (iter != allContainers.end()) {
            mFileDiscovery.SetContainerInfo(iter->second);
            allContainers.erase(iter);
        }
    }

    mFileReader.Init(config, *mContext, sName);

    // Multiline
    const char* key = "Multiline";
    const Json::Value* itr = config.find(key, key + strlen(key));
    if (itr) {
        if (!itr->isObject()) {
            PARAM_WARNING_IGNORE(
                mContext->GetLogger(), "param Multiline is not of type object", sName, mContext->GetConfigName());
        } else {
            mMultiline.Init(*itr, *mContext, sName);
        }
    }

    // AppendingLogPositionMeta
    if (!GetOptionalBoolParam(config, "AppendingLogPositionMeta", mAppendingLogPositionMeta, errorMsg)) {
        PARAM_WARNING_DEFAULT(mContext->GetLogger(), errorMsg, false, sName, mContext->GetConfigName());
    }

    // MaxCheckpointDirSearchDepth
    if (!GetOptionalUIntParam(config, "MaxCheckpointDirSearchDepth", mMaxCheckpointDirSearchDepth, errorMsg)) {
        PARAM_WARNING_DEFAULT(mContext->GetLogger(), errorMsg, 0, sName, mContext->GetConfigName());
    }

    // ExactlyOnceConcurrency (param is unintentionally named as EnableExactlyOnce, which should be deprecated in the
    // future)
    uint32_t exactlyOnceConcurrency;
    if (!GetOptionalUIntParam(config, "EnableExactlyOnce", exactlyOnceConcurrency, errorMsg)) {
        PARAM_WARNING_DEFAULT(mContext->GetLogger(), errorMsg, 0, sName, mContext->GetConfigName());
    } else if (exactlyOnceConcurrency > static_cast<uint32_t>(INT32_FLAG(max_exactly_once_concurrency))) {
        PARAM_WARNING_DEFAULT(mContext->GetLogger(),
                              "param EnableExactlyOnce is larger than 512",
                              INT32_FLAG(max_exactly_once_concurrency),
                              sName,
                              mContext->GetConfigName());
    } else {
        mExactlyOnceConcurrency = exactlyOnceConcurrency;
    }

    return true;
}

bool InputFile::Start() {
    // mContainerInfos = FileServer::GetInstance()->GetAndRemoveContainerInfo(mContext->GetPipeline().lock()->Name());
    // FileServer::GetInstance()->AddPipeline(mContext->GetPipeline().lock());
    return true;
}

bool InputFile::Stop(bool isPipelineRemoving) {
    // if (!isPipelineRemoving) {
    //     FileServer::GetInstance()->SaveContainerInfo(mContext->GetPipeline().lock()->Name(), mContainerInfos);
    // }
    // FileServer::GetInstance()->RemovePipeline(mContext->GetPipeline().lock());
    return true;
}

void InputFile::GenerateContainerMetaFetchingGoPipeline(Json::Value& res) const {
    Json::Value plugin(Json::objectValue), detail(Json::objectValue), object(Json::objectValue);
    auto ConvertMapToJsonObj = [&](const char* key, const unordered_map<string, string>& map) {
        if (!map.empty()) {
            object.clear();
            for (const auto& item : map) {
                object[item.first] = Json::Value(item.second);
            }
            detail[key] = object;
        }
    };

    if (!mFileDiscovery.GetWilecardPaths().empty()) {
        detail["LogPath"] = Json::Value(mFileDiscovery.GetWilecardPaths()[0]);
        detail["MaxDepth"] = Json::Value(static_cast<int32_t>(mFileDiscovery.GetWilecardPaths().size()) + mFileDiscovery.mMaxDirSearchDepth - 1);
    } else {
        detail["LogPath"] = Json::Value(mFileDiscovery.GetBasePath());
        detail["MaxDepth"] = Json::Value(mFileDiscovery.mMaxDirSearchDepth);
    }
    detail["FileParttern"] = Json::Value(mFileDiscovery.GetFilePattern());
    if (!mContainerDiscovery.mContainerFilters.mK8sNamespaceRegex.empty()) {
        detail["K8sNamespaceRegex"] = Json::Value(mContainerDiscovery.mContainerFilters.mK8sNamespaceRegex);
    }
    if (!mContainerDiscovery.mContainerFilters.mK8sPodRegex.empty()) {
        detail["K8sPodRegex"] = Json::Value(mContainerDiscovery.mContainerFilters.mK8sPodRegex);
    }
    if (!mContainerDiscovery.mContainerFilters.mK8sContainerRegex.empty()) {
        detail["K8sContainerRegex"] = Json::Value(mContainerDiscovery.mContainerFilters.mK8sContainerRegex);
    }
    ConvertMapToJsonObj("IncludeK8sLabel", mContainerDiscovery.mContainerFilters.mIncludeK8sLabel);
    ConvertMapToJsonObj("ExcludeK8sLabel", mContainerDiscovery.mContainerFilters.mExcludeK8sLabel);
    ConvertMapToJsonObj("IncludeEnv", mContainerDiscovery.mContainerFilters.mIncludeEnv);
    ConvertMapToJsonObj("ExcludeEnv", mContainerDiscovery.mContainerFilters.mExcludeEnv);
    ConvertMapToJsonObj("IncludeContainerLabel", mContainerDiscovery.mContainerFilters.mIncludeContainerLabel);
    ConvertMapToJsonObj("ExcludeContainerLabel", mContainerDiscovery.mContainerFilters.mExcludeContainerLabel);
    ConvertMapToJsonObj("ExternalK8sLabelTag", mContainerDiscovery.mExternalK8sLabelTag);
    ConvertMapToJsonObj("ExternalEnvTag", mContainerDiscovery.mExternalEnvTag);
    if (mContainerDiscovery.mCollectingContainersMeta) {
        detail["CollectingContainersMeta"] = Json::Value(true);
    }
    plugin["type"] = Json::Value("metric_docker_file");
    plugin["detail"] = detail;

    res["inputs"].append(plugin);
    // these param will be overriden if the same param appears in the global module of config, which will be parsed
    // later.
    res["global"]["DefaultLogQueueSize"] = Json::Value(INT32_FLAG(default_plugin_log_queue_size));
    res["global"]["AlwaysOnline"] = Json::Value(true);
}

} // namespace logtail
