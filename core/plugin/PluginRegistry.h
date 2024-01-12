/*
 * Copyright 2022 iLogtail Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include "common/DynamicLibHelper.h"
#include "plugin/creator/PluginCreator.h"
#include "plugin/instance/PluginInstance.h"
#include "plugin/instance/ProcessorInstance.h"

struct processor_interface_t;

namespace logtail {

class PluginRegistry {
public:
    static PluginRegistry* GetInstance();
    // 加载所有插件
    void LoadPlugins();

    // 卸载所有插件
    void UnloadPlugins();

    // std::unique_ptr<InputInstance> CreateInput(const std::string& name, const std::string& pluginId);
    std::unique_ptr<ProcessorInstance> CreateProcessor(const std::string& name, const PluginInstance::PluginMeta& pluginMeta);
    // std::unique_ptr<FlusherInstance> CreateFlusher(const std::string& name, const std::string& pluginId);

private:
    enum PluginCat { INPUT_PLUGIN, PROCESSOR_PLUGIN, FLUSHER_PLUGIN };

    void LoadStaticPlugins();
    void LoadDynamicPlugins(const std::set<std::string>& plugins);
    // void RegisterInputCreator(PluginCreator* creator);
    void RegisterProcessorCreator(PluginCreator* creator);
    // void RegisterFlusherCreator(PluginCreator* creator);
    PluginCreator* LoadProcessorPlugin(DynamicLibLoader& loader, const std::string pluginName);
    void RegisterCreator(PluginCat cat, PluginCreator* creator);
    std::unique_ptr<PluginInstance> Create(PluginCat cat, const std::string& name, const PluginInstance::PluginMeta& pluginMeta);

    struct PluginKey {
        PluginCat cat;
        std::string name;
        PluginKey(PluginCat cat, const std::string& name) : cat(cat), name(name) {}
        bool operator==(const PluginKey& other) const { return cat == other.cat && name == other.name; }
    };

    struct PluginKeyHash {
        size_t operator()(const PluginKey& obj) const {
            return std::hash<int>()(obj.cat) ^ std::hash<std::string>()(obj.name);
        }
    };
    std::unordered_map<PluginKey, std::shared_ptr<PluginCreator>, PluginKeyHash> mPluginDict;

#ifdef APSARA_UNIT_TEST_MAIN
    friend class PluginRegistryUnittest;
#endif
};

} // namespace logtail