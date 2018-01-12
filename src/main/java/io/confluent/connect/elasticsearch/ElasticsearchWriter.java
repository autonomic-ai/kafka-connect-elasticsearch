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

import io.searchbox.client.JestClientFactory;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.elasticsearch.bulk.BulkProcessor;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.aliases.AddAliasMapping;
import io.searchbox.indices.aliases.ModifyAliases;


public class ElasticsearchWriter {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchWriter.class);

  private JestClient client;
  private final String type;
  private final boolean ignoreKey;
  private final Set<String> ignoreKeyTopics;
  private final boolean ignoreSchema;
  private final Set<String> ignoreSchemaTopics;
  private final Map<String, String> topicToIndexMap;
  private final long flushTimeoutMs;
  private final BulkProcessor<IndexableRecord, ?> bulkProcessor;
  private final IndexConfigurationProvider indexConfigurationProvider;
  private final CustomDocumentTransformer customDocumentTransformer;
  private final Set<String> existingMappings;
  private final static int QUEUE_REPORT_INTERVAL = 30;
  private final static int MILLIS_PER_SECOND = 1000;
  public  final static int MAX_NEW_CONNECTION_ATTEMPTS = 2; // Will result in only 1 retry
  private HashMap<String, Integer> currentIndexTotals = new HashMap<>();
  private long lastReport = 0;
  private Map<Schema.Type, String> fieldTypes = null;
  private final Metrics metrics;
  private final JestClientFactory factory;

  ElasticsearchWriter(
      JestClient client,
      String type,
      boolean ignoreKey,
      Set<String> ignoreKeyTopics,
      boolean ignoreSchema,
      Set<String> ignoreSchemaTopics,
      Map<String, String> topicToIndexMap,
      long flushTimeoutMs,
      int maxBufferedRecords,
      int maxInFlightRequests,
      int batchSize,
      long lingerMs,
      int maxRetries,
      long retryBackoffMs,
      IndexConfigurationProvider indexConfigurationProvider,
      CustomDocumentTransformer customDocumentTransformer,
      Map<Schema.Type, String> fieldTypes,
      Metrics metrics,
      JestClientFactory factory
  ) {
    this.client = client;
    this.type = type;
    this.ignoreKey = ignoreKey;
    this.ignoreKeyTopics = ignoreKeyTopics;
    this.ignoreSchema = ignoreSchema;
    this.ignoreSchemaTopics = ignoreSchemaTopics;
    this.topicToIndexMap = topicToIndexMap;
    this.flushTimeoutMs = flushTimeoutMs;
    this.indexConfigurationProvider = indexConfigurationProvider;
    this.customDocumentTransformer = customDocumentTransformer;
    this.fieldTypes = fieldTypes;
    this.metrics = metrics;
    this.factory = factory;

    metrics.setIndexBufferMax(maxBufferedRecords);

    bulkProcessor = new BulkProcessor<>(
        new SystemTime(),
        new BulkIndexingClient(client),
        maxBufferedRecords,
        maxInFlightRequests,
        batchSize,
        lingerMs,
        maxRetries,
        retryBackoffMs,
        metrics
    );

    existingMappings = new HashSet<>();
  }

  public static class Builder {
    private final JestClient client;
    private String type;
    private boolean ignoreKey = false;
    private Set<String> ignoreKeyTopics = Collections.emptySet();
    private boolean ignoreSchema = false;
    private Set<String> ignoreSchemaTopics = Collections.emptySet();
    private Map<String, String> topicToIndexMap = new HashMap<>();
    private long flushTimeoutMs;
    private int maxBufferedRecords;
    private int maxInFlightRequests;
    private int batchSize;
    private long lingerMs;
    private int maxRetry;
    private long retryBackoffMs;
    private IndexConfigurationProvider indexConfigurationProvider;
    private CustomDocumentTransformer customDocumentTransformer;
    private Map<Schema.Type, String> fieldTypes = null;
    private Metrics metrics;
    private JestClientFactory factory = null;

    public Builder(JestClient client) {
      this.client = client;
    }

    public Builder setIndexConfigurationProvider(IndexConfigurationProvider indexConfigurationProvider) {
      this.indexConfigurationProvider = indexConfigurationProvider;
      return this;
    }

    public Builder setCustomDocumentTransformer(CustomDocumentTransformer customDocumentTransformer) {
      this.customDocumentTransformer = customDocumentTransformer;
      return this;
    }

    public Builder setClientFactory(JestClientFactory factory) {
      this.factory = factory;
      return this;
    }

    public Builder setFieldTypes(Map<Schema.Type, String> fieldTypes) {
      this.fieldTypes = fieldTypes;
      return this;
    }

    public Builder setMetrics(Metrics metrics) {
      this.metrics = metrics;
      return this;
    }

    public Builder setType(String type) {
      this.type = type;
      return this;
    }

    public Builder setIgnoreKey(boolean ignoreKey, Set<String> ignoreKeyTopics) {
      this.ignoreKey = ignoreKey;
      this.ignoreKeyTopics = ignoreKeyTopics;
      return this;
    }

    public Builder setIgnoreSchema(boolean ignoreSchema, Set<String> ignoreSchemaTopics) {
      this.ignoreSchema = ignoreSchema;
      this.ignoreSchemaTopics = ignoreSchemaTopics;
      return this;
    }

    public Builder setTopicToIndexMap(Map<String, String> topicToIndexMap) {
      this.topicToIndexMap = topicToIndexMap;
      return this;
    }

    public Builder setFlushTimoutMs(long flushTimeoutMs) {
      this.flushTimeoutMs = flushTimeoutMs;
      return this;
    }

    public Builder setMaxBufferedRecords(int maxBufferedRecords) {
      this.maxBufferedRecords = maxBufferedRecords;
      return this;
    }

    public Builder setMaxInFlightRequests(int maxInFlightRequests) {
      this.maxInFlightRequests = maxInFlightRequests;
      return this;
    }

    public Builder setBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder setLingerMs(long lingerMs) {
      this.lingerMs = lingerMs;
      return this;
    }

    public Builder setMaxRetry(int maxRetry) {
      this.maxRetry = maxRetry;
      return this;
    }

    public Builder setRetryBackoffMs(long retryBackoffMs) {
      this.retryBackoffMs = retryBackoffMs;
      return this;
    }

    public ElasticsearchWriter build() {
      return new ElasticsearchWriter(
          client,
          type,
          ignoreKey,
          ignoreKeyTopics,
          ignoreSchema,
          ignoreSchemaTopics,
          topicToIndexMap,
          flushTimeoutMs,
          maxBufferedRecords,
          maxInFlightRequests,
          batchSize,
          lingerMs,
          maxRetry,
          retryBackoffMs,
          indexConfigurationProvider,
          customDocumentTransformer,
          fieldTypes,
          metrics,
          factory
      );
    }
  }

  public void write(Collection<SinkRecord> records) {
    String documentRootField = null;
    for (SinkRecord sinkRecord : records) {
      final String indexOverride = topicToIndexMap.get(sinkRecord.topic());
      final String index;
      final boolean ignoreKey = ignoreKeyTopics.contains(sinkRecord.topic()) || this.ignoreKey;
      final boolean ignoreSchema = ignoreSchemaTopics.contains(sinkRecord.topic()) || this.ignoreSchema;

      if (indexConfigurationProvider != null) {
        index = indexConfigurationProvider.getIndexName(sinkRecord);
        documentRootField = indexConfigurationProvider.getDocumentRootFieldName(sinkRecord);
      } else if (indexOverride != null) {
        index = indexOverride;
      } else {
        index = sinkRecord.topic();
      }
      if (!ignoreSchema && !existingMappings.contains(index)) {
        try {
          if (Mapping.getMapping(client, index, type) == null) {
            Mapping.createMapping(client, index, type, sinkRecord.valueSchema(), documentRootField, indexConfigurationProvider, fieldTypes);
          }
        } catch (IOException e) {
          // FIXME: concurrent tasks could attempt to create the mapping and one of the requests may fail
          throw new ConnectException("Failed to initialize mapping for index: " + index, e);
        }
        existingMappings.add(index);
      }

      final IndexableRecord indexableRecord;

      indexableRecord = DataConverter.convertRecord(sinkRecord, index, type, ignoreKey, ignoreSchema, indexConfigurationProvider, customDocumentTransformer);

      bulkProcessor.add(indexableRecord, flushTimeoutMs);

      int count = currentIndexTotals.containsKey(index) ? currentIndexTotals.get(index) : 0;
      currentIndexTotals.put(index, count + 1);
    }

    if (System.currentTimeMillis() > lastReport + (QUEUE_REPORT_INTERVAL * MILLIS_PER_SECOND) &&
            currentIndexTotals.size() > 0) {
      log.info("Documents enqueued in last {}s: ", QUEUE_REPORT_INTERVAL);
      for (Map.Entry<String, Integer> entry : currentIndexTotals.entrySet()) {
        log.info("{}: {}", entry.getKey(), entry.getValue());
      }

      lastReport = System.currentTimeMillis();
      currentIndexTotals.clear();
    }
  }

  private Map<String, String> indexAliasesForRecords(Collection<SinkRecord> records) {
    HashMap<String, String> indexToAliasMap = new HashMap<>();
    for (SinkRecord record : records) {
      if (indexConfigurationProvider != null) {
        Map.Entry<String, String> entry = indexConfigurationProvider.getIndexAliasMapping(record);
        if (entry != null)
          indexToAliasMap.put(entry.getKey(), entry.getValue());
      }
    }
    return indexToAliasMap;
  }

  public void flush() {
    bulkProcessor.flush(flushTimeoutMs);
  }

  public void start() {
    bulkProcessor.start();
  }

  public void stop() {
    try {
      bulkProcessor.flush(flushTimeoutMs);
    } catch (Exception e) {
      log.warn("Failed to flush during stop", e);
    }
    bulkProcessor.stop();
    bulkProcessor.awaitStop(flushTimeoutMs);
  }

  private boolean indexExists(String index) {

    int retries = 1;

    while (retries < MAX_NEW_CONNECTION_ATTEMPTS) {
      Action action = new IndicesExists.Builder(index).build();
      try {
        JestResult result = client.execute(action);
        return result.isSucceeded();
      } catch (IOException e) {
        if (retries > MAX_NEW_CONNECTION_ATTEMPTS || factory == null) {
          throw new ConnectException(e);
        }
        // Reconnect client
        // Will not shutdown client since other connections could still be alive
        client = factory.getObject();
        retries++;
      }
    }

    return false;
  }

  public void createIndicesForTopics(Set<String> assignedTopics) throws ConnectException {
    this.createIndices(indicesForTopics(assignedTopics));
  }

  public void createIndicesForRecords(Collection<SinkRecord> records) throws ConnectException {
    Set<String> indices = indicesForRecords(records);
    this.createIndices(indices);
    this.createIndexAliases(indexAliasesForRecords(records));
  }

  public void createIndexAliases(Map<String, String> indexAliasMap) {
    for (Map.Entry<String, String> entry: indexAliasMap.entrySet()) {
      if (entry.getKey() != null && entry.getValue() != null) {
        ModifyAliases modifyAliases = new ModifyAliases.Builder(
                new AddAliasMapping.Builder(entry.getKey(), entry.getValue())
                        .build())
                .build();

        int retries = 1;
        boolean keepRetrying = true;

        while (retries < MAX_NEW_CONNECTION_ATTEMPTS && keepRetrying) {
          try {
            JestResult result = client.execute(modifyAliases);
            if (!result.isSucceeded()) {
              throw new ConnectException(
                  "Could not create alias " + entry.getValue() + " for index " + entry.getKey());
            } else {
              keepRetrying = false;
            }
          } catch (IOException e) {
            if (retries > MAX_NEW_CONNECTION_ATTEMPTS || factory == null) {
              throw new ConnectException(e);
            }
            // Reconnect client
            // Will not shutdown client since other connections could still be alive
            client = factory.getObject();
            retries++;
          }
        }
      }
    }
  }

  public void createIndices(Set<String> indices) throws ConnectException {
    String customIndexSettings = null;
    CreateIndex createIndex = null;

    if (indexConfigurationProvider != null &&
            indexConfigurationProvider.getIndexCreationSettings() != null) {
      customIndexSettings = indexConfigurationProvider.getIndexCreationSettings();
    }

    for (String index : indices) {
      if (!indexExists(index)) {
        if (customIndexSettings != null) {
          createIndex = new CreateIndex.Builder(index).settings(customIndexSettings).build();
        } else {
          createIndex = new CreateIndex.Builder(index).build();
        }

        int retries = 1;
        boolean keepRetrying = true;

        while (retries < MAX_NEW_CONNECTION_ATTEMPTS && keepRetrying) {
          try {
            JestResult result = client.execute(createIndex);
            if (!result.isSucceeded()) {
              log.error("Unable to create index {}: ", index, result.getErrorMessage());
              throw new ConnectException(
                  "Could not create index: " + index + ": " + result.getErrorMessage());
            } else {
              keepRetrying = false;
            }
          } catch (IOException e) {
            if (retries > MAX_NEW_CONNECTION_ATTEMPTS || factory == null) {
              throw new ConnectException(e);
            }
            // Reconnect client
            // Will not shutdown client since other connections could still be alive
            client = factory.getObject();
            retries++;
          }
        }
      }
    }
  }

  private Set<String> indicesForRecords(Collection<SinkRecord> records) {
    Set<String> topics = new HashSet<>();

    if (indexConfigurationProvider != null) {
      for (SinkRecord record : records) {
        topics.add(indexConfigurationProvider.getIndexName(record));
      }
    } else {
      for (SinkRecord record : records) {
        topics.add(record.topic());
      }
    }

    return topics;
  }

  private Set<String> indicesForTopics(Set<String> assignedTopics) {
    final Set<String> indices = new HashSet<>();
    for (String topic : assignedTopics) {
      final String index = topicToIndexMap.get(topic);
      if (index != null) {
        indices.add(index);
      } else {
        indices.add(topic);
      }
    }
    return indices;
  }

}
