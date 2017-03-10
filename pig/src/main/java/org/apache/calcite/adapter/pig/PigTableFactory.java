/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.pig;

import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.util.Util;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Factory that creates a {@link PigTable}.
 *
 * <p>Allows a Pig table to be included in a model.json file.</p>
 */
public class PigTableFactory implements TableFactory<PigTable> {
  static {
    // Work around
    //   [CALCITE-1694] Clash in protobuf versions between Avatica and Hadoop
    // by loading Avatica protobuf before Hadoop kicks in.
    Util.discard(ConnectionPropertiesImpl.class);
  }

  // public constructor, per factory contract
  public PigTableFactory() {
  }

  @SuppressWarnings("unchecked")
  public PigTable create(SchemaPlus schema, String name,
      Map<String, Object> operand, RelDataType rowType) {
    String fileName = (String) operand.get("file");
    File file = new File(fileName);
    final File base =
        (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    if (base != null && !file.isAbsolute()) {
      file = new File(base, fileName);
    }
    final List<String> fieldNames = (List<String>) operand.get("columns");
    final PigTable result = new PigTable(file.getAbsolutePath(), fieldNames.toArray(new String[0]));
    schema.unwrap(PigSchema.class).registerTable(name, result);
    return result;
  }
}

// End PigTableFactory.java
