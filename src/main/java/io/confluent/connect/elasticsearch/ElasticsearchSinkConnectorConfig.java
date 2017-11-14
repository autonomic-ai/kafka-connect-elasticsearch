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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkTask.CREATE_INDEX_AT_OPEN;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkTask.CREATE_INDEX_AT_WRITE;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkTask.DISABLE_MAX_IDLE_CONNECTION_TIMEOUT;

public class ElasticsearchSinkConnectorConfig extends AbstractConfig {

  public static final String CONNECTION_URL_CONFIG = "connection.url";
  public static final String BATCH_SIZE_CONFIG = "batch.size";
  public static final String MAX_IN_FLIGHT_REQUESTS_CONFIG = "max.in.flight.requests";
  public static final String MAX_BUFFERED_RECORDS_CONFIG = "max.buffered.records";
  public static final String SOCKET_READ_TIMEOUT_MS_CONFIG = "socket.read.timeout.ms";
  public static final String LINGER_MS_CONFIG = "linger.ms";
  public static final String FLUSH_TIMEOUT_MS_CONFIG = "flush.timeout.ms";
  public static final String MAX_RETRIES_CONFIG = "max.retries";
  public static final String TYPE_NAME_CONFIG = "type.name";
  public static final String TOPIC_INDEX_MAP_CONFIG = "topic.index.map";
  public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
  public static final String KEY_IGNORE_CONFIG = "key.ignore";
  public static final String TOPIC_KEY_IGNORE_CONFIG = "topic.key.ignore";
  public static final String SCHEMA_IGNORE_CONFIG = "schema.ignore";
  public static final String TOPIC_SCHEMA_IGNORE_CONFIG = "topic.schema.ignore";
  public static final String INDEX_CONFIGURATION_PROVIDER_CONFIG = "index.configuration.provider";
  public static final String INDEX_CREATION_STRATEGY_CONFIG = "index.creation.strategy";
  public static final String CUSTOM_DOCUMENT_TRANSFORMER_CONFIG = "custom.document.transformer";
  public static final String CUSTOM_INDEX_TRANSFORMER_CONFIG = "custom.index.transformer";
  public static final String MAX_IDLE_CONNECTION_TIMEOUT_MS_CONFIG = "max.idle.connection.timeout.ms";
  public static final String SCHEMA_FIELD_MAP_OVERRIDES_CONFIG = "schema.field.map.overrides";


  protected static ConfigDef baseConfigDef() {
    final ConfigDef configDef = new ConfigDef();

    {
      final String group = "Connector";
      int order = 0;
      configDef
          .define(CONNECTION_URL_CONFIG, Type.LIST, Importance.HIGH,
                  "List of Elasticsearch HTTP connection URLs e.g. ``http://eshost1:9200,http://eshost2:9200``.",
                  group, ++order, Width.LONG, "Connection URLs")
          .define(BATCH_SIZE_CONFIG, Type.INT, 2000, Importance.MEDIUM,
                  "The number of records to process as a batch when writing to Elasticsearch.",
                  group, ++order, Width.SHORT, "Batch Size")
          .define(MAX_IN_FLIGHT_REQUESTS_CONFIG, Type.INT, 5, Importance.MEDIUM,
                  "The maximum number of indexing requests that can be in-flight to Elasticsearch before blocking further requests.",
                  group, 5, Width.SHORT, "Max In-flight Requests")
          .define(MAX_BUFFERED_RECORDS_CONFIG, Type.INT, 20000, Importance.LOW,
                  "The maximum number of records each task will buffer before blocking acceptance of more records. This config can be used to limit the memory usage for each task.",
                  group, ++order, Width.SHORT, "Max Buffered Records")
          .define(LINGER_MS_CONFIG, Type.LONG, 1L, Importance.LOW,
                  "Linger time in milliseconds for batching.\n"
                  + "Records that arrive in between request transmissions are batched into a single bulk indexing request, based on the ``" + BATCH_SIZE_CONFIG + "`` configuration. "
                  + "Normally this only occurs under load when records arrive faster than they can be sent out. "
                  + "However it may be desirable to reduce the number of requests even under light load and benefit from bulk indexing. "
                  + "This setting helps accomplish that - when a pending batch is not full, rather than immediately sending it out "
                  + "the task will wait up to the given delay to allow other records to be added so that they can be batched into a single request.",
                  group, ++order, Width.SHORT, "Linger (ms)")
          .define(FLUSH_TIMEOUT_MS_CONFIG, Type.LONG, 10000L, Importance.LOW,
                  "The timeout in milliseconds to use for periodic flushing, "
                  + "and when waiting for buffer space to be made available by completed requests as records are added. "
                  + "If this timeout is exceeded the task will fail.",
                  group, ++order, Width.SHORT, "Flush Timeout (ms)")
          .define(MAX_RETRIES_CONFIG, Type.INT, 5, Importance.LOW,
                  "The maximum number of retries that are allowed for failed indexing requests. "
                  + "If the retry attempts are exhausted the task will fail.",
                  group, ++order, Width.SHORT, "Max Retries")
          .define(RETRY_BACKOFF_MS_CONFIG, Type.LONG, 100L, Importance.LOW,
                  "How long to wait in milliseconds before attempting to retry a failed indexing request. "
                  + "This avoids retrying in a tight loop under failure scenarios.",
                  group, ++order, Width.SHORT, "Retry Backoff (ms)")
          .define(SOCKET_READ_TIMEOUT_MS_CONFIG, Type.INT, 3000, Importance.LOW,
                  "How long to wait in milliseconds before timing out on socket reads. Notably, this determines"
                  + "How long to wait to receive responses from Elasticsearch servers.",
                  group, ++order, Width.LONG, "Socket read timeout (ms)")
          .define(MAX_IDLE_CONNECTION_TIMEOUT_MS_CONFIG, Type.INT, DISABLE_MAX_IDLE_CONNECTION_TIMEOUT, Importance.LOW,
                  "Limit the duration a connection to Elasticsearch may remain idle before being closed by the client. "
                  + "(disabled by default)",
                  group, ++order, Width.LONG, "Idle Connection timeout")
          .define(SCHEMA_FIELD_MAP_OVERRIDES_CONFIG, Type.LIST, "", Importance.LOW,
                  "A Map which overrides default Kafka Schema field to index mapping field.",
                  group, ++order, Width.LONG, "Override Kafka Schema field to index map field");
    }

    {
      final String group = "Data Conversion";
      int order = 0;
      configDef
          .define(TYPE_NAME_CONFIG, Type.STRING, Importance.HIGH,
                  "The Elasticsearch type name to use when indexing.",
                  group, ++order, Width.SHORT, "Type Name")
          .define(KEY_IGNORE_CONFIG, Type.BOOLEAN, false, Importance.HIGH,
                  "Whether to ignore the record key for the purpose of forming the Elasticsearch document ID. "
                  + "When this is set to ``true``, document IDs will be generated as the record's ``topic+partition+offset``.\n"
                  + "Note that this is a global config that applies to all topics, use ``" + TOPIC_KEY_IGNORE_CONFIG + "`` to override as ``true`` for specific topics.",
                  group, ++order, Width.SHORT, "Ignore Key mode")
          .define(SCHEMA_IGNORE_CONFIG, Type.BOOLEAN, false, Importance.LOW,
                  "Whether to ignore schemas during indexing. "
                  + "When this is set to ``true``, the record schema will be ignored for the purpose of registering an Elasticsearch mapping. "
                  + "Elasticsearch will infer the mapping from the data (dynamic mapping needs to be enabled by the user).\n"
                  + "Note that this is a global config that applies to all topics, use ``" + TOPIC_SCHEMA_IGNORE_CONFIG + "`` to override as ``true`` for specific topics.",
                  group, ++order, Width.SHORT, "Ignore Schema mode")
          .define(TOPIC_INDEX_MAP_CONFIG, Type.LIST, "", Importance.LOW,
                  "A map from Kafka topic name to the destination Elasticsearch index, represented as a list of ``topic:index`` pairs.",
                  group, ++order, Width.LONG, "Topic to Index Map")
          .define(TOPIC_KEY_IGNORE_CONFIG, Type.LIST, "", Importance.LOW,
                  "List of topics for which ``" + KEY_IGNORE_CONFIG + "`` should be ``true``.",
                  group, ++order, Width.LONG, "Topics for 'Ignore Key' mode")
          .define(TOPIC_SCHEMA_IGNORE_CONFIG, Type.LIST, "", Importance.LOW,
                  "List of topics for which ``" + SCHEMA_IGNORE_CONFIG + "`` should be ``true``.",
                  group, ++order, Width.LONG, "Topics for 'Ignore Schema' mode")
          .define(INDEX_CONFIGURATION_PROVIDER_CONFIG, Type.CLASS, null, Importance.LOW,
                  "Class used to provide index configuration settings",
                  group, ++order, Width.LONG, "Index configuration provider")
          .define(INDEX_CREATION_STRATEGY_CONFIG, Type.STRING, CREATE_INDEX_AT_OPEN, Importance.LOW,
                  "Whether to create indexes when task is started or when documents are written.\n"
                  + "Valid values are `" + CREATE_INDEX_AT_OPEN + "` or `" + CREATE_INDEX_AT_WRITE + "`.")
          .define(CUSTOM_DOCUMENT_TRANSFORMER_CONFIG, Type.CLASS, null, Importance.LOW,
                  "The class to use to perform any final transformations on documents after conversion to JSON.",
                  group, ++order, Width.SHORT, "Custom Document Transformer Class")
          .define(CUSTOM_INDEX_TRANSFORMER_CONFIG, Type.CLASS, null, Importance.LOW,
                  "The class to use to perform any final transformations on document indexes.",
                  group, ++order, Width.SHORT, "Custom Index Transformer Class");
    }

    return configDef;
  }

  public static final ConfigDef CONFIG = baseConfigDef();

  public ElasticsearchSinkConnectorConfig(Map<String, String> props) {
    super(CONFIG, props);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG.toEnrichedRst());
  }
}
