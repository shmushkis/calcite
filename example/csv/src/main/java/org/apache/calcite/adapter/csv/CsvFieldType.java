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
package org.apache.calcite.adapter.csv;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.Map;

/**
 * Type of a field in a CSV file.
 *
 * <p>Usually, and unless specified explicitly in the header row, a field is
 * of type {@link #STRING}. But specifying the field type in the header row
 * makes it easier to write SQL.</p>
 */
enum CsvFieldType {
  STRING(String.class, "string"),
  BOOLEAN(Primitive.BOOLEAN),
  BYTE(Primitive.BYTE),
  CHAR(Primitive.CHAR),
  SHORT(Primitive.SHORT),
  INT(Primitive.INT),
  LONG(Primitive.LONG),
  FLOAT(Primitive.FLOAT),
  DOUBLE(Primitive.DOUBLE),
  DATE(java.sql.Date.class, "date"),
  TIME(java.sql.Time.class, "time"),
  TIMESTAMP(java.sql.Timestamp.class, "timestamp");

  private final Class clazz;
  private final String simpleName;

  private static final Map<String, CsvFieldType> MAP = new HashMap<>();

  static {
    for (CsvFieldType value : values()) {
      MAP.put(value.simpleName, value);
    }
  }

  CsvFieldType(Primitive primitive) {
    this(primitive.boxClass, primitive.primitiveClass.getSimpleName());
  }

  CsvFieldType(Class clazz, String simpleName) {
    this.clazz = clazz;
    this.simpleName = simpleName;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    switch (this) {
    case DATE:
      return typeFactory.createSqlType(SqlTypeName.DATE);
    case TIME:
      return typeFactory.createSqlType(SqlTypeName.TIME);
    case TIMESTAMP:
      return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    default:
      return typeFactory.createJavaType(clazz);
    }
  }

  public static CsvFieldType of(String typeString) {
    return MAP.get(typeString);
  }
}

// End CsvFieldType.java
