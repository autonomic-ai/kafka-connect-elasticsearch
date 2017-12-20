/**
 * Copyright 2016 Confluent Inc.
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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.INDEX_CONFIGURATION_PROVIDER_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.INDEX_CREATION_STRATEGY_CONFIG;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ElasticsearchSinkTaskTest extends ElasticsearchSinkTestBase {

  private Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();
    props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, TYPE);
    props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "localhost");
    props.put(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
    return props;
  }

  @Test
  public void testPutAndFlush() throws Exception {
    client.setException(true);
    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(3);
    Map<String, String> props = createProps();

    ElasticsearchSinkTask task = new ElasticsearchSinkTask();
    task.start(props, client, factory);
    task.open(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)));

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 0);
    records.add(sinkRecord);

    sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1);
    records.add(sinkRecord);

    task.put(records);
    task.flush(null);

    refresh();

    verifySearchResults(records, true, false);
    client.setException(false);
  }

  @Test
  public void testPutAndFlushWithIndexConfigurationProvider() throws Exception {
    client.setException(true);
    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(3);
    Map<String, String> props = createProps();
    CustomIndexConfigurationProvider customIndexConfigurationProvider = new CustomIndexConfigurationProvider();

    props.put(INDEX_CONFIGURATION_PROVIDER_CONFIG, "io.confluent.connect.elasticsearch.CustomIndexConfigurationProvider");
    props.put(INDEX_CREATION_STRATEGY_CONFIG, "atWrite");

    ElasticsearchSinkTask task = new ElasticsearchSinkTask();
    task.start(props, client, factory);
    task.open(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)));

    Schema schema = SchemaBuilder.struct().name("record")
            .field("user", Schema.STRING_SCHEMA)
            .field("message", Schema.STRING_SCHEMA)
            .field("time", SchemaBuilder.INT64_SCHEMA)
            .build();

    Struct record = new Struct(schema);
    record.put("user", "Liquan");
    record.put("message", "trying out Elastic Search.");
    record.put("time", Instant.now().getMillis());

    String key = "key";

    Collection<SinkRecord> records = new ArrayList<>();
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 0);
    records.add(sinkRecord);

    task.put(records);
    task.flush(null);

    refresh();

    verifySearchResults(records, customIndexConfigurationProvider.getIndexName(sinkRecord), true, false, customIndexConfigurationProvider);
    client.setException(false);
  }
}
