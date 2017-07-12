/**
 * Copyright 2017 Autonomic Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/
package io.confluent.connect.elasticsearch;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public class CustomIndexConfigurationProvider implements IndexConfigurationProvider {
  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public String getIndexName(SinkRecord record) {
    Struct struct = (Struct) record.value();
    String time = struct.get("time").toString();

    return "custom_index_" + time;
  }

  @Override
  public Map.Entry<String, String> getIndexAliasMapping(SinkRecord record) {
    return null;
  }

  @Override
  public String getDocumentId(SinkRecord record) {
    return "customId";
  }

  @Override
  public IndexOperation getIndexOperation(SinkRecord record) {
    return IndexOperation.index;
  }

  @Override
  public String getIndexCreationSettings() {
    return "{ \"settings\" : { \"number_of_shards\" : 1, " +
        "\"number_of_replicas\" : 2} }";
  }

  @Override
  public String getFieldMappingConfiguration(SinkRecord record) {
    return null;
  }

  @Override
  public String getDocumentRootFieldName(SinkRecord record) {
    return null;
  }
}
