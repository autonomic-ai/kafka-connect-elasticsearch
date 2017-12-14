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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Map.Entry;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * Interface that must be extended to provide a customized
 * index semantics
 */
public interface IndexConfigurationProvider {
  /**
   * Configure this index name resolver
   * @param configs
   */
  void configure(Map<String, ?> configs);

  /**
   * Get the appropriate index name for a record
   * @param record the sink record to compute index alias for
   * @return The index name for this record
   */
  String getIndexName(SinkRecord record);

  /**
   * Get the appropriate mapping from index name to alias for a record.
   * @param record the sink record to compute index alias for
   * @return The index name to alias mapping, or null if no index alias
   * is to be generated.
   */
  Entry<String, String> getIndexAliasMapping(SinkRecord record);

  /**
   * Get the String to use as a document Id for the specified record.
   */
  String getDocumentId(SinkRecord record);

  /**
   * Determine what type of request to submit to ES for this record
   * index.
   *
   * @return
   */
  IndexOperation getIndexOperation(SinkRecord record);

  /**
   * Determine the customized index creation  settings.
   * (e.g. overriding default replica or shard counts)
   *
   * A null return value implies the use of defaults.
   */
  String getIndexCreationSettings();

  /**
   * In an Upsert scenario, the document data is specified as a sub-field in
   * the JSON payload
   *
   * @return
   */
  String getDocumentRootFieldName(SinkRecord record);

  /**
   * Perform any final customizations on the document's index.
   * @return Map<String,String> Collection of field names to replace.
   */
  public Map<String, String> getTypeMapping();

  /* Determine if there are per Index fields */
  public Boolean hasGlobalIndexFields();

  /* Array of ObjectNodes to be added in the per Index field region */
  public ArrayList<Entry<String, ObjectNode>> getGlobalIndexFields();
}
