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
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ElasticsearchSinkTaskTest extends ElasticsearchSinkTestBase {

  private Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();
    props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, TYPE);
    props.put(ElasticsearchSinkConnectorConfig.TRANSPORT_ADDRESSES_CONFIG, "localhost");
    props.put(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
    return props;
  }

  @Test
  public void testStart() throws Exception {
    Map<String, String> props = createProps();
    SinkTask task = new ElasticsearchSinkTask();
    task.initialize(context);
    try {
      task.start(props);
      fail("Invalid transport address.");
    } catch (ConnectException e) {
      // expected
    }
  }

  @Test
  public void testPutAndFlush() throws Exception {
    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(3);
    Client client = cluster.client();

    Map<String, String> props = createProps();

    ElasticsearchSinkTask task = new ElasticsearchSinkTask();
    task.initialize(context);
    task.start(props, client);
    task.open(assignment);

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

    Thread.sleep(SLEEP_INTERVAL_MS);
    verifySearch(records, search(client, "user", "liquan"), true);
    verifySearch(records, search(client, "message", "elastic"), true);
  }
}
