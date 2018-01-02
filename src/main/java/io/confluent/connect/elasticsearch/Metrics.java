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

/**
 * Generic object to capture metrics on Elasticsearch and document buffer.
 */
public class Metrics {

  /**
   * Capture starting time of record added to the buffer.
   * @return Custom timer object
   */
  public synchronized Object startIndexingBlockTimer() {
    return null;
  }

  /**
   * Record the time a record was on hold to be added to the indexing buffer.
   * @param timer Custom timer.
   */
  public synchronized void observeIndexingBlockTime(Object timer) {
  }

  /**
   * Record the time needed to process a record in Elasticsearch
   * @param latencySecs Provide record indexing seconds.
   */
  public synchronized void observeIndexingLatency(double latencySecs) {
  }

  /**
   * Set the size of the record buffer at.
   * @param amount Size of buffer at the time.
   */
  public synchronized void setIndexBufferUsed(double amount) {
  }

  /**
   * Set the maximum buffer size.
   * @param amount Maximum buffer size.
   */
  public synchronized void setIndexBufferMax(double amount) {
  }

  /**
   * Record the number of successfully indexed documents in Elasticsearch.
   * @param increment
   */
  public synchronized void incSuccessfulIndexedDocuments(int increment) {
  }
}
