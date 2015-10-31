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
package org.apache.calcite.linq4j.function;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Executable;

/**
 * Annotation that supplies metadata about a function parameter.
 *
 * <p>A typical use is to derive names for the parameters of user-defined
 * functions.
 *
 * <p>If present, supersedes information that might be available via
 * {@link Executable#getParameters()}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER })
public @interface Parameter {
  /** The name of the parameter.
   *
   * <p>The name is used when the function is called
   * with parameter assignment, for example {@code foo(x => 1, y => 'a')}.
   */
  String name();

  /** Returns whether the parameter is optional.
   *
   * <p>An optional parameter does not need to be specified when you call the
   * function.
   *
   * <p>If you call a function using positional parameter syntax, you can omit
   * optional parameters on the trailing edge. For example, if you have a
   * function
   * {@code baz(int a, int b optional, int c, int d optional, int e optional)}
   * then you can call {@code baz(a, b, c, d, e)}
   * or {@code baz(a, b, c, d)}
   * or {@code baz(a, b, c)}
   * but not {@code baz(a, b)} because {@code c} is not optional.
   *
   * <p>If you call a function using parameter name assignment syntax, you can
   * omit any parameter that has a default value. For example, you can call
   * {@code baz(a => 1, e => 5, c => 3)}, omitting optional parameters {@code b}
   * and {@code d}.
   *
   * <p>Currently, the default value used when a parameter is not specified
   * is NULL, and therefore optional parameters must be nullable.
   */
  boolean optional() default false;
}

// End Parameter.java
