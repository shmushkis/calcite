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

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Implementation of the {@link RelMetadataProvider} interface that generates
 * a class that dispatches to the underlying providers.
 */
public class JaninoRelMetadataProvider implements RelMetadataProvider {
  private final RelMetadataProvider provider;

  public JaninoRelMetadataProvider(RelMetadataProvider provider) {
    super();
    this.provider = provider;
  }

  public <M extends Metadata> UnboundMetadata<M> apply(
      Class<? extends RelNode> relClass, Class<? extends M> metadataClass) {
    throw new UnsupportedOperationException();
  }

  public Map<Method, Object> handlers(Class<? extends Metadata> metadataClass) {
    return provider.handlers(metadataClass);
  }

  private final Set<Class<? extends RelNode>> allRels = new HashSet<>();
  private final LoadingCache<Class<?>, ?> handlers =
      CacheBuilder.newBuilder().build(
          new CacheLoader<Class<?>, Object>() {
            public Object load(@Nonnull Class<?> key) throws Exception {
              return load_(key);
            }
          });

  private <H> H load_(Class<H> rClass) {
    if (rClass == BuiltInMetadata.UniqueKeys.Handler.class) {
      return rClass.cast(
          new BuiltInMetadata.UniqueKeys.Handler() {
            final RelMdUniqueKeys provider =
                (RelMdUniqueKeys)
                    ((ReflectiveRelMetadataProvider) RelMdUniqueKeys.SOURCE).target;

            public Set<ImmutableBitSet> getUniqueKeys(RelNode r,
                RelMetadataQuery mq, boolean ignoreNulls) {
              switch (r.getClass().getName()) {
              case "org.apache.calcite.rel.logical.LogicalAggregate":
                return provider.getUniqueKeys((Aggregate) r, mq, ignoreNulls);
              default:
                throw NoHandler.INSTANCE;
              }
            }
          });
    }
    throw new UnsupportedOperationException("cannot handle " + rClass);
  }

  synchronized <H> H create(Class<H> rClass) {
    try {
      //noinspection unchecked
      return (H) handlers.get(rClass);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  synchronized <H> H revise(Class<? extends RelNode> rClass,
      Class<H> hClass) {
    if (allRels.add(rClass)) {
      handlers.invalidateAll();
    }
    return create(hClass);
  }
}

// End ReflectiveRelMetadataProvider.java
