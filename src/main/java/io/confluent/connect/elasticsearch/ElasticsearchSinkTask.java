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

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConstants.DEFAULT_TYPES;

public class ElasticsearchSinkTask extends SinkTask {

  public static final String CREATE_INDEX_AT_OPEN = "atOpen";
  public static final String CREATE_INDEX_AT_WRITE = "atWrite";
  public static final int DISABLE_MAX_IDLE_CONNECTION_TIMEOUT = -1;

  public static Map<Schema.Type, String> fieldTypes = new HashMap<>();

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);
  private ElasticsearchWriter writer;
  private JestClient client;
  private String indexCreationStrategy = CREATE_INDEX_AT_OPEN;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    start(props, null);
  }

  // public for testing
  public void start(Map<String, String> props, JestClient client) {
    try {
      log.info("Starting ElasticsearchSinkTask.");

      ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
      String type = config.getString(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG);
      boolean ignoreKey = config.getBoolean(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG);
      boolean ignoreSchema = config.getBoolean(ElasticsearchSinkConnectorConfig.SCHEMA_IGNORE_CONFIG);

      Map<String, String> schemaToFieldMap = parseMapConfigKeyToUpper(config.getList(ElasticsearchSinkConnectorConfig.SCHEMA_FIELD_MAP_OVERRIDES_CONFIG));
      Map<String, String> topicToIndexMap = parseMapConfig(config.getList(ElasticsearchSinkConnectorConfig.TOPIC_INDEX_MAP_CONFIG));
      Set<String> topicIgnoreKey = new HashSet<>(config.getList(ElasticsearchSinkConnectorConfig.TOPIC_KEY_IGNORE_CONFIG));
      Set<String> topicIgnoreSchema =  new HashSet<>(config.getList(ElasticsearchSinkConnectorConfig.TOPIC_SCHEMA_IGNORE_CONFIG));

      long flushTimeoutMs = config.getLong(ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG);
      int maxBufferedRecords = config.getInt(ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG);
      int batchSize = config.getInt(ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG);
      long lingerMs = config.getLong(ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG);
      int maxInFlightRequests = config.getInt(ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG);
      long retryBackoffMs = config.getLong(ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG);
      int socketReadTimeoutMs = config.getInt(ElasticsearchSinkConnectorConfig.SOCKET_READ_TIMEOUT_MS_CONFIG);
      int maxRetry = config.getInt(ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG);
      IndexConfigurationProvider indexConfigurationProvider = config.getConfiguredInstance(ElasticsearchSinkConnectorConfig.INDEX_CONFIGURATION_PROVIDER_CONFIG, IndexConfigurationProvider.class);
      indexCreationStrategy = config.getString(ElasticsearchSinkConnectorConfig.INDEX_CREATION_STRATEGY_CONFIG);
      CustomDocumentTransformer customDocumentTransformer = config.getConfiguredInstance(ElasticsearchSinkConnectorConfig.CUSTOM_DOCUMENT_TRANSFORMER_CONFIG, CustomDocumentTransformer.class);
      Metrics metrics = config.getConfiguredInstance(ElasticsearchSinkConnectorConfig.CUSTOM_METRICS_CONFIG, Metrics.class);
      int idleConnectionTimeout = config.getInt(ElasticsearchSinkConnectorConfig.MAX_IDLE_CONNECTION_TIMEOUT_MS_CONFIG);

      if (metrics == null)
        metrics = new Metrics();

      switch (indexCreationStrategy) {
        case CREATE_INDEX_AT_OPEN:
            /* FALLTHROUGH */
        case CREATE_INDEX_AT_WRITE:
          break;
        default:
          log.warn("Ignoring unsupported index creation strategy: {}. defaulting to {}", indexCreationStrategy, CREATE_INDEX_AT_OPEN);
          indexCreationStrategy = CREATE_INDEX_AT_OPEN;
      }

      loadDataTypes(schemaToFieldMap);

      if (client != null) {
        this.client = client;
      } else {
        List<String> address = config.getList(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG);
        JestClientFactory factory = new JestClientFactory();
        if (idleConnectionTimeout != DISABLE_MAX_IDLE_CONNECTION_TIMEOUT)
          factory.setHttpClientConfig(new HttpClientConfig.Builder(address).readTimeout(socketReadTimeoutMs).maxConnectionIdleTime(idleConnectionTimeout, TimeUnit.MILLISECONDS).multiThreaded(true).build());
        else
          factory.setHttpClientConfig(new HttpClientConfig.Builder(address).readTimeout(socketReadTimeoutMs).multiThreaded(true).build());
        this.client = factory.getObject();
      }

      if (indexConfigurationProvider != null) {
        indexConfigurationProvider.configure(props);
      }

      ElasticsearchWriter.Builder builder = new ElasticsearchWriter.Builder(this.client)
          .setType(type)
          .setIgnoreKey(ignoreKey, topicIgnoreKey)
          .setIgnoreSchema(ignoreSchema, topicIgnoreSchema)
          .setTopicToIndexMap(topicToIndexMap)
          .setFlushTimoutMs(flushTimeoutMs)
          .setMaxBufferedRecords(maxBufferedRecords)
          .setMaxInFlightRequests(maxInFlightRequests)
          .setBatchSize(batchSize)
          .setLingerMs(lingerMs)
          .setRetryBackoffMs(retryBackoffMs)
          .setMaxRetry(maxRetry)
          .setIndexConfigurationProvider(indexConfigurationProvider)
          .setCustomDocumentTransformer(customDocumentTransformer)
          .setMetrics(metrics);

      writer = builder.build();
      writer.start();
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start ElasticsearchSinkTask due to configuration error:", e);
    }
  }

  synchronized private void loadDataTypes(Map<String, String> schemaToFieldMap) {
    loadPrimitiveTypes();
    loadCustomMappingTypes(schemaToFieldMap);
  }

  private static void loadCustomMappingTypes(Map<String, String> overrideEntries) {

    if (overrideEntries != null && overrideEntries.size() > 0) {
      for (Map.Entry<Schema.Type, String> typeEntry : fieldTypes.entrySet()) {
        if (overrideEntries.containsKey(typeEntry.getKey().toString().toUpperCase())) {
          typeEntry.setValue(overrideEntries.get(typeEntry.getKey().toString().toUpperCase()));
        }
      }
    }
  }

  // public for testing
  public static void loadPrimitiveTypes() {

    fieldTypes.clear();

    // set default schema type -> ES field type map
    for (Map.Entry<Schema.Type, String> entry : DEFAULT_TYPES.entrySet()) {
      fieldTypes.put(entry.getKey(), entry.getValue());
    }
  }

  // public for testing
  synchronized public static void reload(List<String> values) {
    loadPrimitiveTypes();
    loadCustomMappingTypes(parseMapConfigKeyToUpper(values));
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    log.debug("Opening the task for topic partitions: {}", partitions);
    Set<String> topics = topicsFromPartitions(partitions);

    if (indexCreationStrategy.equals(CREATE_INDEX_AT_OPEN))
      writer.createIndicesForTopics(topics);
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    log.trace("Putting {} to Elasticsearch.", records);

    if (records.size() > 0 && indexCreationStrategy.equals(CREATE_INDEX_AT_WRITE)) {
      writer.createIndicesForRecords(records);
    }

    writer.write(records);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    log.debug("Flushing data to Elasticsearch with the following offsets: {}", offsets);
    writer.flush();
    log.debug("Flush returned successfully.");
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    log.debug("Closing the task for topic partitions: {}", partitions);
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopping ElasticsearchSinkTask.");
    if (writer != null) {
      writer.stop();
    }
    if (client != null) {
      client.shutdownClient();
    }
  }

  private Set<String> topicsFromPartitions(Collection<TopicPartition> partitions) {
    Set<String> topics = new HashSet<>();
    for (TopicPartition tp : partitions) {
      topics.add(tp.topic());
    }

    return topics;
  }


  private Map<String, String> parseMapConfig(List<String> values) {
    Map<String, String> map = new HashMap<>();
    for (String value: values) {
      String[] parts = value.split(":");
      String topic = parts[0];
      String type = parts[1];
      map.put(topic, type);
    }
    return map;
  }

  private static Map<String, String> parseMapConfigKeyToUpper(List<String> values) {
    Map<String, String> map = new HashMap<>();
    for (String value: values) {
      String[] parts = value.split(":");
      String topic = parts[0];
      String type = parts[1];
      map.put(topic.toUpperCase(), type);
    }
    return map;
  }

}
