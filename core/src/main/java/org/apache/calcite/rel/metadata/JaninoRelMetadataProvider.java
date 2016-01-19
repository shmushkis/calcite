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

import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;

/**
 * Implementation of the {@link RelMetadataProvider} interface that generates
 * a class that dispatches to the underlying providers.
 */
public class JaninoRelMetadataProvider implements RelMetadataProvider {
  private final RelMetadataProvider provider;

  private static final Set<Class<? extends RelNode>> ALL_RELS =
      new CopyOnWriteArraySet<>();

  private final LoadingCache<ClassPair, MetadataHandler> handlers =
      CacheBuilder.newBuilder().build(
          new CacheLoader<ClassPair, MetadataHandler>() {
            public MetadataHandler load(@Nonnull ClassPair key)
                throws Exception {
              return load_(key.left, key.right);
            }
          });

  private JaninoRelMetadataProvider(RelMetadataProvider provider) {
    this.provider = provider;
  }

  /** Creates a JaninoRelMetadataProvider.
   *
   * @param provider Underlying provider
   */
  public static JaninoRelMetadataProvider of(RelMetadataProvider provider) {
    if (provider instanceof JaninoRelMetadataProvider) {
      return (JaninoRelMetadataProvider) provider;
    }
    return new JaninoRelMetadataProvider(provider);
  }

  public <M extends Metadata> UnboundMetadata<M> apply(
      Class<? extends RelNode> relClass, Class<? extends M> metadataClass) {
    throw new UnsupportedOperationException();
  }

  public Map<Method, Object> handlers(Class<? extends Metadata> metadataClass) {
    return provider.handlers(metadataClass);
  }

  private <M extends Metadata, H extends MetadataHandler<M>>
  H load_(Class<M> mClass, Class<H> hClass) {
    final StringBuilder b = new StringBuilder();
    final String name = "Mdp";
    final Set<Object> providerSet = new HashSet<>();
    final List<Pair<String, Object>> providerList = new ArrayList<>();
    final List<Class<RelNode>> relList = new ArrayList<>();
    //new ReflectiveRelMetadataProvider.Space()
    for (Map.Entry<Method, Object> entry : handlers(mClass).entrySet()) {
      final Object handler = entry.getValue();
      if (providerSet.add(handler)) {
        providerList.add(Pair.of("provider" + (providerSet.size() - 1), handler));
      }
      //noinspection unchecked
      relList.add((Class<RelNode>) entry.getKey().getParameterTypes()[0]);
    }

    b.append("  private final java.util.List relClasses;\n");
    for (Pair<String, Object> pair : providerList) {
      b.append("  public final ").append(pair.right.getClass().getName())
          .append(' ').append(pair.left).append(";\n");
    }
    b.append("  public Mdp(java.util.List relClasses");
    for (Pair<String, Object> pair : providerList) {
      b.append(",\n")
          .append(pair.right.getClass().getName())
          .append(' ')
          .append(pair.left);
    }
    b.append(") {\n")
        .append("    this.relClasses = relClasses;\n");

    for (Pair<String, Object> pair : providerList) {
      b.append("    this.").append(pair.left).append(" = ").append(pair.left)
          .append(";\n");
    }
    b.append("  }\n")
        .append("  public Class getMetadataClass() {\n")
        .append("    return ")
        .append(hClass.getName())
        .append(".class;\n")
        .append("  }\n")
        .append("  public java.util.Set getUniqueKeys(\n")
        .append("      org.apache.calcite.rel.RelNode r,\n")
        .append("      org.apache.calcite.rel.metadata.RelMetadataQuery mq,\n")
        .append("      boolean ignoreNulls) {\n")
        .append("    switch (relClasses.indexOf(r.getClass())) {\n")
        .append("    case 0:\n")
        .append("      return provider10.getUniqueKeys((org.apache.calcite.rel.logical.LogicalAggregate) r, mq, ignoreNulls);\n")
        .append("    default:\n")
        .append("      throw ")
        .append(RelMetadataQuery.NoHandler.class.getName())
        .append(".INSTANCE;\n")
        .append("      }\n")
        .append("    }");
    ClassDeclaration decl = new ClassDeclaration(0, name, Object.class,
        ImmutableList.<Type>of(), ImmutableList.<MemberDeclaration>of());
    final List<Object> argList = new ArrayList<>(Pair.right(providerList));
    argList.add(0, ImmutableList.copyOf(relList));
    try {
      return compile(decl, b.toString(), hClass, argList);
    } catch (CompileException | IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static <H extends MetadataHandler> H compile(ClassDeclaration expr,
      String s, Class<H> class_, List<Object> argList)
      throws CompileException, IOException {
    final ICompilerFactory compilerFactory;
    try {
      compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to instantiate java compiler", e);
    }
    final IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
    cbe.setClassName(expr.name);
    cbe.setImplementedInterfaces(new Class[]{class_});
    cbe.setParentClassLoader(JaninoRexCompiler.class.getClassLoader());
    if (CalcitePrepareImpl.DEBUG || true) {
      // Add line numbers to the generated janino class
      cbe.setDebuggingInformation(true, true, true);
      System.out.println(s);
    }
    cbe.cook(new StringReader(s));
    final Constructor constructor = cbe.getClazz().getDeclaredConstructors()[0];
    final Object o;
    try {
      o = constructor.newInstance(argList.toArray());
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
    return class_.cast(o);
  }

  synchronized <M extends Metadata, H extends MetadataHandler<M>>
  H create(Class<M> mClass, Class<H> hClass) {
    try {
      //noinspection unchecked
      return (H) handlers.get(new ClassPair(mClass, hClass));
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  synchronized <M extends Metadata, H extends MetadataHandler<M>>
  H revise(Class<? extends RelNode> rClass, Class<M> mClass, Class<H> hClass) {
    if (ALL_RELS.add(rClass)) {
      handlers.invalidateAll();
    }
    return create(mClass, hClass);
  }

  /** A {@link Metadata} class and a {@link MetadataHandler} class. */
  private static class ClassPair
      extends Pair<Class<? extends Metadata>, Class<? extends MetadataHandler>> {
    public ClassPair(Class<? extends Metadata> left,
        Class<? extends MetadataHandler> right) {
      super(left, right);
    }
  }
}

// End JaninoRelMetadataProvider.java
