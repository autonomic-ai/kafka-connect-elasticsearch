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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonObject;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Assert;
import org.junit.Test;

public class MappingTest extends ElasticsearchSinkTestBase {

  private static final String INDEX = "kafka-connect";
  private static final String TYPE = "kafka-connect-type";

  @Test
  @SuppressWarnings("unchecked")
  public void testMapping() throws Exception {
    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(1);

    createIndex(INDEX);
    Schema schema = createSchema();
    Mapping.createMapping(client, INDEX, TYPE, schema, null,
        null, ElasticsearchSinkConnectorConstants.DEFAULT_TYPES);

    JsonObject mapping = Mapping.getMapping(client, INDEX, TYPE);
    assertNotNull(mapping);
    verifyMapping(schema, mapping);
  }

  @Test
  public void testCustomMapping() throws Exception {

    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(1);

    createIndex(INDEX);
    Schema schema = createSchema();

    CustomIndex customIndex = new CustomIndex();
    customIndex.setTypeMapping(new HashMap<String, String>(){{ put("boolean","my_type"); }});
    JsonNode document = Mapping.inferMapping(schema, ElasticsearchSinkTask.getDataTypes(Arrays.asList("string:keyword")), customIndex);

    Assert.assertTrue(document.toString().contains("keyword"));
    Assert.assertTrue(document.toString().contains("my_type"));
  }

  @Test
  public void testCustomMappingNotMatching() throws Exception {

    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(1);

    createIndex(INDEX);
    Schema schema = createSchema();

    CustomIndex customIndex = new CustomIndex();
    customIndex.setTypeMapping(new HashMap<String, String>(){{ put("location","my_invalid_type"); }});
    JsonNode document = Mapping.inferMapping(schema, ElasticsearchSinkTask.getDataTypes(Arrays.asList("invalid:keyword")), customIndex);

    Assert.assertTrue(!document.toString().contains("keyword"));
    Assert.assertTrue(!document.toString().contains("my_invalid_type"));
  }

  @Test
  public void testCustomMappingMultipleReplacements() throws Exception {

    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(1);

    createIndex(INDEX);
    Schema schema = createSchema();

    CustomIndex customIndex = new CustomIndex();
    customIndex.setTypeMapping(new HashMap<String, String>(){{ {{ put("boolean","my_boolean");
                                                                  put("decimal", "my_decimal");
                                                                }}; }});
    JsonNode document = Mapping.inferMapping(schema, ElasticsearchSinkTask.getDataTypes(Arrays.asList("string:keyword", "int8:my_int")), customIndex);

    Assert.assertTrue(document.toString().contains("keyword"));
    Assert.assertTrue(document.toString().contains("my_int"));
    Assert.assertTrue(document.toString().contains("my_boolean"));
    Assert.assertTrue(document.toString().contains("my_decimal"));
  }

  @Test
  public void testCustomIndexExtensions() throws Exception {

    InternalTestCluster cluster = ESIntegTestCase.internalCluster();
    cluster.ensureAtLeastNumDataNodes(1);

    createIndex(INDEX);
    Schema schema = createSchema();

    CustomIndexExtensions customIndex = new CustomIndexExtensions();
    customIndex.setTypeMapping(new HashMap<String, String>(){{ put("timestamp","date@notAnalyzed"); }});

    JsonNode document = Mapping.inferMapping(schema, customIndex);

    Assert.assertTrue(document.toString().contains("_all"));
    Assert.assertTrue(document.toString().contains("not_analyzed"));
  }

  protected Schema createSchema() {
    Schema structSchema = createInnerSchema();
    return SchemaBuilder.struct().name("record")
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("int8", Schema.INT8_SCHEMA)
        .field("int16", Schema.INT16_SCHEMA)
        .field("int32", Schema.INT32_SCHEMA)
        .field("int64", Schema.INT64_SCHEMA)
        .field("float32", Schema.FLOAT32_SCHEMA)
        .field("float64", Schema.FLOAT64_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
        .field("struct", structSchema)
        .field("decimal", Decimal.schema(2))
        .field("date", Date.SCHEMA)
        .field("time", Time.SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .build();
  }

  private Schema createInnerSchema() {
    return SchemaBuilder.struct().name("inner")
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("int8", Schema.INT8_SCHEMA)
        .field("int16", Schema.INT16_SCHEMA)
        .field("int32", Schema.INT32_SCHEMA)
        .field("int64", Schema.INT64_SCHEMA)
        .field("float32", Schema.FLOAT32_SCHEMA)
        .field("float64", Schema.FLOAT64_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
        .field("decimal", Decimal.schema(2))
        .field("date", Date.SCHEMA)
        .field("time", Time.SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .build();
  }

  @SuppressWarnings("unchecked")
  private void verifyMapping(Schema schema, JsonObject mapping) throws Exception {
    String schemaName = schema.name();

    Object type = mapping.get("type");
    if (schemaName != null) {
      switch (schemaName) {
        case Date.LOGICAL_NAME:
        case Time.LOGICAL_NAME:
        case Timestamp.LOGICAL_NAME:
          assertEquals("\"" + ElasticsearchSinkConnectorConstants.DATE_TYPE + "\"", type.toString());
          return;
        case Decimal.LOGICAL_NAME:
          assertEquals("\"" + ElasticsearchSinkConnectorConstants.DOUBLE_TYPE + "\"", type.toString());
          return;
      }
    }

    Schema.Type schemaType = schema.type();
    switch (schemaType) {
      case ARRAY:
        verifyMapping(schema.valueSchema(), mapping);
        break;
      case MAP:
        Schema newSchema = DataConverter.preProcessSchema(schema);
        JsonObject mapProperties = mapping.get("properties").getAsJsonObject();
        verifyMapping(newSchema.keySchema(), mapProperties.get(ElasticsearchSinkConnectorConstants.MAP_KEY).getAsJsonObject());
        verifyMapping(newSchema.valueSchema(), mapProperties.get(ElasticsearchSinkConnectorConstants.MAP_VALUE).getAsJsonObject());
        break;
      case STRUCT:
        JsonObject properties = mapping.get("properties").getAsJsonObject();
        for (Field field: schema.fields()) {
          verifyMapping(field.schema(), properties.get(field.name()).getAsJsonObject());
        }
        break;
      default:
        assertEquals("\"" + ElasticsearchSinkConnectorConstants.DEFAULT_TYPES.get(schemaType) + "\"", type.toString());
    }
  }

  class CustomIndex extends CustomIndexConfigurationProvider {

    HashMap<String, String> typeMapping = null;

    public void setTypeMapping(HashMap<String, String> typeMapping) {
      this.typeMapping = typeMapping;
    }

    @Override
    public Map<String, String> getTypeMapping() {
      return typeMapping;
    }
  }

  class CustomIndexExtensions extends CustomIndex {

    @Override
    public Boolean hasGlobalIndexFields() { return true; }

    @Override
    public ArrayList<Entry<String, ObjectNode>> getGlobalIndexFields() {
      ArrayList<Entry<String, ObjectNode>> nodeArray = new ArrayList<Entry<String, ObjectNode>>();
      ObjectNode subCustomTypeNode = JsonNodeFactory.instance.objectNode();
      subCustomTypeNode.set("enabled", JsonNodeFactory.instance.textNode("false"));
      nodeArray.add(new SimpleEntry<String, ObjectNode>("_all", subCustomTypeNode));
      return nodeArray;
    }
  }
}

