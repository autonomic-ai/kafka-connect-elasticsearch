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

import java.util.Map;

public interface CustomIndexTransformer {

  /**
   * Perform any final customizations on the document's index.
   * @return Map<String,String> Collection of field names to replace.
   */
  public Map<String, String> getTransformTypes();

}
