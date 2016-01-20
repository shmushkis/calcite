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

import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableJoin;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.stream.LogicalChi;
import org.apache.calcite.rel.stream.LogicalDelta;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Pair;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

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
import java.util.Arrays;
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
 *
 * <p>TODO:
 * 1. Check that handler has the same methods as the provider but with just
 * 2 fewer parameters</p>
 */
public class JaninoRelMetadataProvider implements RelMetadataProvider {
  private final RelMetadataProvider provider;

  // Constants and static fields

  public static final JaninoRelMetadataProvider DEFAULT =
      JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE);

  private static final Set<Class<? extends RelNode>> ALL_RELS =
      new CopyOnWriteArraySet<>();

  /** Cache of pre-generated handlers by provider and kind of metadata.
   * For the cache to be effective, providers should implement identity
   * correctly. */
  private static final LoadingCache<Key, MetadataHandler> HANDLERS =
      CacheBuilder.newBuilder().build(
          new CacheLoader<Key, MetadataHandler>() {
            public MetadataHandler load(@Nonnull Key key) {
              //noinspection unchecked
              return load3(key.def, key.provider.handlers(key.def),
                  key.relClasses);
            }
          });

  static {
    DEFAULT.register(
        Arrays.asList(RelNode.class,
            AbstractRelNode.class,
            RelSubset.class,
            HepRelVertex.class,
            ConverterImpl.class,
            AbstractConverter.class,

            LogicalAggregate.class,
            LogicalCalc.class,
            LogicalCorrelate.class,
            LogicalExchange.class,
            LogicalFilter.class,
            LogicalIntersect.class,
            LogicalJoin.class,
            LogicalMinus.class,
            LogicalProject.class,
            LogicalSort.class,
            LogicalTableFunctionScan.class,
            LogicalTableModify.class,
            LogicalTableScan.class,
            LogicalUnion.class,
            LogicalValues.class,
            LogicalWindow.class,
            LogicalChi.class,
            LogicalDelta.class,

            EnumerableAggregate.class,
            EnumerableFilter.class,
            EnumerableProject.class,
            EnumerableJoin.class,
            EnumerableTableScan.class));
  }

  /** Private constructor; use {@link #of}. */
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

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof JaninoRelMetadataProvider
        && ((JaninoRelMetadataProvider) obj).provider.equals(provider);
  }

  @Override public int hashCode() {
    return 109 + provider.hashCode();
  }

  public <M extends Metadata> UnboundMetadata<M> apply(
      Class<? extends RelNode> relClass, Class<? extends M> metadataClass) {
    throw new UnsupportedOperationException();
  }

  public <M extends Metadata> Map<Method, MetadataHandler<M>>
  handlers(MetadataDef<M> def) {
    return provider.handlers(def);
  }

  private static <M extends Metadata>
  MetadataHandler<M> load3(MetadataDef<M> def,
      Map<Method, MetadataHandler<M>> map,
      ImmutableList<Class<? extends RelNode>> relClasses) {
    final StringBuilder buff = new StringBuilder();
    final String name =
        "GeneratedMetadataHandler_" + def.getMetadataClass().getSimpleName();
    final Set<MetadataHandler> providerSet = new HashSet<>();
    final List<Pair<String, MetadataHandler>> providerList = new ArrayList<>();
    //noinspection unchecked
    final ReflectiveRelMetadataProvider.Space space =
        new ReflectiveRelMetadataProvider.Space((Map) map);
    for (MetadataHandler provider : space.providerMap.values()) {
      if (providerSet.add(provider)) {
        providerList.add(Pair.of("provider" + (providerSet.size() - 1),
            provider));
      }
    }

    buff.append("  private final java.util.List relClasses;\n");
    for (Pair<String, MetadataHandler> pair : providerList) {
      buff.append("  public final ").append(pair.right.getClass().getName())
          .append(' ').append(pair.left).append(";\n");
    }
    buff.append("  public ").append(name).append("(java.util.List relClasses");
    for (Pair<String, MetadataHandler> pair : providerList) {
      buff.append(",\n")
          .append(pair.right.getClass().getName())
          .append(' ')
          .append(pair.left);
    }
    buff.append(") {\n")
        .append("    this.relClasses = relClasses;\n");

    for (Pair<String, MetadataHandler> pair : providerList) {
      buff.append("    this.").append(pair.left).append(" = ").append(pair.left)
          .append(";\n");
    }
    buff.append("  }\n")
        .append("  public org.apache.calcite.rel.metadata.MetadataDef getDef() {\n")
        .append("    return ")
        .append(def.getMetadataClass().getName())
        .append(".DEF;\n")
        .append("  }\n");
    for (Method method : def.methods) {
      buff.append("  public ")
          .append(method.getReturnType().getName())
          .append(" ")
          .append(method.getName())
          .append("(\n")
          .append("      org.apache.calcite.rel.RelNode r,\n")
          .append("      org.apache.calcite.rel.metadata.RelMetadataQuery mq");
      paramList(buff, method)
          .append(") {\n");
      buff.append("    final java.util.List key = ")
          .append(
              (method.getParameterTypes().length < 2
                  ? org.apache.calcite.runtime.FlatLists.class
                  : ImmutableList.class).getName())
          .append(".of(")
          .append(def.getMetadataClass().getName())
          .append(".DEF, r");
      argList(buff, method)
          .append(");\n")
          .append("    if (!mq.set.add(key)) {\n")
          .append("      throw ")
          .append(CyclicMetadataException.class.getName())
          .append(".INSTANCE;\n")
          .append("    }\n")
          .append("    try {\n")
          .append("      switch (relClasses.indexOf(r.getClass())) {\n");
      for (Ord<Class<? extends RelNode>> relClass : Ord.zip(relClasses)) {
        buff.append("      case ").append(relClass.i).append(":\n");
        if (relClass.e == HepRelVertex.class) {
          buff.append("      return ")
              .append(method.getName())
              .append("(((")
              .append(relClass.e.getName())
              .append(") r).getCurrentRel(), mq");
          argList(buff, method)
              .append(");\n");
          continue;
        }
        final Method handler = space.find(relClass.e, method);
        final String v = findProvider(providerList, handler.getDeclaringClass());
        buff.append("        return ")
            .append(v)
            .append(".")
            .append(method.getName())
            .append("((")
            .append(relClass.e.getName())
            .append(") r, mq");
        argList(buff, method)
            .append(");\n");
      }
      buff.append("      default:\n")
          .append("        throw new ")
          .append(NoHandler.class.getName())
          .append("(r.getClass());\n")
          .append("      }\n")
          .append("    } finally {\n")
          .append("      mq.set.remove(key);\n")
          .append("    }\n")
          .append("  }\n");
    }
    ClassDeclaration decl = new ClassDeclaration(0, name, Object.class,
        ImmutableList.<Type>of(), ImmutableList.<MemberDeclaration>of());
    final List<Object> argList = new ArrayList<Object>(Pair.right(providerList));
    argList.add(0, ImmutableList.copyOf(relClasses));
    try {
      return compile(decl, buff.toString(), def, argList);
    } catch (CompileException | IOException e) {
      System.out.println(buff);
      throw Throwables.propagate(e);
    }
  }

  private static String
  findProvider(List<Pair<String, MetadataHandler>> providerList,
      Class<?> declaringClass) {
    for (Pair<String, MetadataHandler> pair : providerList) {
      if (declaringClass.isInstance(pair.right)) {
        return pair.left;
      }
    }
    throw new AssertionError("not found: " + declaringClass);
  }

  /** Returns e.g. ", ignoreNulls". */
  private static StringBuilder argList(StringBuilder b, Method method) {
    for (Ord<Class<?>> t : Ord.zip(method.getParameterTypes())) {
      b.append(", a").append(t.i);
    }
    return b;
  }

  /** Returns e.g. ",\n boolean ignoreNulls". */
  private static StringBuilder paramList(StringBuilder b, Method method) {
    for (Ord<Class<?>> t : Ord.zip(method.getParameterTypes())) {
      b.append(",\n      ").append(t.e.getName()).append(" a").append(t.i);
    }
    return b;
  }

  static <M extends Metadata> MetadataHandler<M>
  compile(ClassDeclaration expr, String s, MetadataDef<M> def,
      List<Object> argList) throws CompileException, IOException {
    final ICompilerFactory compilerFactory;
    try {
      compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to instantiate java compiler", e);
    }
    final IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
    cbe.setClassName(expr.name);
    cbe.setImplementedInterfaces(new Class[]{def.getHandlerClass()});
    cbe.setParentClassLoader(JaninoRexCompiler.class.getClassLoader());
    if (CalcitePrepareImpl.DEBUG) {
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
    return def.getHandlerClass().cast(o);
  }

  synchronized <M extends Metadata, H extends MetadataHandler<M>> H
  create(MetadataDef<M> def) {
    try {
      final Key key = new Key((MetadataDef) def, provider,
          ImmutableList.copyOf(ALL_RELS));
      //noinspection unchecked
      return (H) HANDLERS.get(key);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  synchronized <M extends Metadata, H extends MetadataHandler<M>> H
  revise(Class<? extends RelNode> rClass, MetadataDef<M> def) {
    if (ALL_RELS.add(rClass)) {
      HANDLERS.invalidateAll();
    }
    //noinspection unchecked
    return (H) create(def);
  }

  /** Registers some classes. Does not flush the providers, but next time we
   * need to generate a provider, it will handle all of these classes. So,
   * calling this method reduces the number of times we need to re-generate. */
  public void register(Iterable<Class<? extends RelNode>> classes) {
    // Register the classes and their base classes up to RelNode. Don't bother
    // to remove duplicates; addAll will do that.
    final List<Class<? extends RelNode>> list = Lists.newArrayList(classes);
    for (int i = 0; i < list.size(); i++) {
      final Class<? extends RelNode> c = list.get(i);
      final Class s = c.getSuperclass();
      if (s != null && RelNode.class.isAssignableFrom(s)) {
        //noinspection unchecked
        list.add(s);
      }
    }
    if (ALL_RELS.addAll(list)) {
      HANDLERS.invalidateAll();
    }
  }

  /** Exception that indicates there there should be a handler for
   * this class but there is not. The action is probably to
   * re-generate the handler class. */
  public static class NoHandler extends ControlFlowException {
    public final Class<? extends RelNode> relClass;

    public NoHandler(Class<? extends RelNode> relClass) {
      this.relClass = relClass;
    }
  }

  /** Key for the cache. */
  private static class Key {
    public final MetadataDef def;
    public final RelMetadataProvider provider;
    public final ImmutableList<Class<? extends RelNode>> relClasses;

    private Key(MetadataDef def, RelMetadataProvider provider,
        ImmutableList<Class<? extends RelNode>> relClassList) {
      this.def = def;
      this.provider = provider;
      this.relClasses = relClassList;
    }

    @Override public int hashCode() {
      return (def.hashCode() * 37
          + provider.hashCode()) * 37
          + relClasses.hashCode();
    }

    @Override public boolean equals(Object obj) {
      return this == obj
          || obj instanceof Key
          && ((Key) obj).def.equals(def)
          && ((Key) obj).provider.equals(provider)
          && ((Key) obj).relClasses.equals(relClasses);
    }
  }
}

// End JaninoRelMetadataProvider.java
