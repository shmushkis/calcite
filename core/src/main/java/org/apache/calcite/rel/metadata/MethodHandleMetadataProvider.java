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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectiveVisitor;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the {@link RelMetadataProvider} interface that dispatches
 * metadata methods to methods on a given object via
 * {@link java.lang.invoke.MethodHandle}s.
 *
 * <p>The methods on the target object must be public and non-static, and have
 * the same signature as the implemented metadata method except for an
 * additional first parameter of type {@link RelNode} or a sub-class. That
 * parameter gives this provider an indication of that relational expressions it
 * can handle.</p>
 *
 * <p>For an example, see {@link RelMdColumnOrigins#SOURCE}.
 */
public class MethodHandleMetadataProvider
    implements RelMetadataProvider, ReflectiveVisitor {
  final Object target;

  public MethodHandleMetadataProvider(Object target) {
    this.target = target;
  }


  public <M extends Metadata>
  UnboundMetadata<M> apply(Class<? extends RelNode> relClass,
      Class<? extends M> metadataClass) {
    final MethodType mt = MethodType.methodType(Boolean.class,
        relClass.getSuperclass(), RelMetadataQuery.class, ImmutableBitSet.class,
        boolean.class);
    final MethodHandles.Lookup lookup = MethodHandles.lookup();
    final MethodHandle mh;
    try {
      mh = lookup.findVirtual(RelMdColumnUniqueness.class, "areColumnsUnique", mt);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
    return new MethodHandleUnboundMetadata<>(metadataClass, ImmutableList.<Method>of(), new ArrayList<>(), mh);
  }

  public static RelMetadataProvider source(Method method, Object target) {
    return source(target, ImmutableList.of(method));
  }

  private static RelMetadataProvider source(Object target,
      ImmutableList<Method> methods) {
    return new MethodHandleMetadataProvider(target);
  }

  /** Base class for implementing {@link Metadata} using method handles. */
  static class Base implements Metadata {
    final Object target;
    final RelNode rel;
    final RelMetadataQuery mq;
    final MethodHandle mh;

    Base(Object target, RelNode rel, RelMetadataQuery mq, MethodHandle mh) {
      this.target = target;
      this.rel = rel;
      this.mq = mq;
      this.mh = mh;
    }

    public RelNode rel() {
      return rel;
    }
  }

  /** Implementation of
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.ColumnUniqueness}
   * using method handles. */
  static class ColumnUniquenessImpl extends Base
      implements BuiltInMetadata.ColumnUniqueness {
    ColumnUniquenessImpl(Object target, RelNode rel, RelMetadataQuery mq,
        MethodHandle mh) {
      super(target, rel, mq, mh);
    }

    public Boolean areColumnsUnique(ImmutableBitSet columns,
        boolean ignoreNulls) {
      try {
        return (Boolean) mh.invoke(target, rel, mq, columns, ignoreNulls);
      } catch (Throwable e) {
        throw Throwables.propagate(e);
      }
    }
  }

  static class MethodHandleUnboundMetadata<M extends Metadata>
      implements UnboundMetadata<M> {
    private final Class<Metadata> metadataClass0;
    private final ImmutableList<Method> methods;
    private final List<Method> handlerMethods;
    private final Object target;

    public MethodHandleUnboundMetadata(Class<Metadata> metadataClass0,
        ImmutableList<Method> methods,
        List<Method> handlerMethods,
        Object target) {
      this.metadataClass0 = metadataClass0;
      this.methods = methods;
      this.handlerMethods = handlerMethods;
      this.target = target;
    }

    public M bind(RelNode rel, RelMetadataQuery mq) {
      //noinspection unchecked
      return (M) new ColumnUniquenessImpl(target, rel, mq, mh);
    }
  }
}

// End MethodHandleMetadataProvider.java
