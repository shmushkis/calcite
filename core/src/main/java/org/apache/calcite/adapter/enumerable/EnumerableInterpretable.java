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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.avatica.Helper;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.interpreter.InterpretableConvention;
import org.apache.calcite.interpreter.InterpretableRel;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.Typed;
import org.apache.calcite.runtime.Utilities;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;

/**
 * Relational expression that converts an enumerable input to interpretable
 * calling convention.
 */
public class EnumerableInterpretable extends ConverterImpl
    implements InterpretableRel {
  protected EnumerableInterpretable(RelOptCluster cluster, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, cluster.traitSetOf(
        InterpretableConvention.INSTANCE), input);
  }

  public Node implement(final InterpreterImplementor implementor) {
    EnumerableRelImplementor relImplementor =
        new EnumerableRelImplementor(getCluster().getRexBuilder(),
            implementor.internalParameters);

    final EnumerableRel input = (EnumerableRel) getInput();
    final ClassDeclaration expr =
        relImplementor.implementRoot(input, EnumerableRel.Prefer.ARRAY);
    String s = Expressions.toString(expr.memberDeclarations, "\n", false);

    if (CalcitePrepareImpl.DEBUG) {
      debugCode(System.out, s);
    }

    Hook.JAVA_PLAN.run(s);

    final Bindable bindable;
    try {
      if (implementor.spark.enabled()) {
        bindable = implementor.spark.compile(expr, s);
      } else {
        bindable = getBindable(expr, s);
      }
    } catch (Exception e) {
      throw Helper.INSTANCE.wrap("Error while compiling generated Java code:\n"
          + s, e);
    }
    //noinspection unchecked
    final Enumerable<Object[]> enumerable =
        bindable.bind(implementor.dataContext);
    return new EnumerableNode(enumerable, implementor.interpreter, this);
  }

  /**
   * Prints the given code with line numbering.
   */
  private void debugCode(PrintStream out, String code) {
    out.println();
    StringReader sr = new StringReader(code);
    BufferedReader br = new BufferedReader(sr);
    try {
      String line;
      for (int i = 1; (line = br.readLine()) != null; i++) {
        out.print("/*");
        String number = Integer.toString(i);
        if (number.length() < 4) {
          Spaces.append(out, 4 - number.length());
        }
        out.print(number);
        out.print(" */ ");
        out.println(line);
      }
    } catch (IOException e) {
      // not possible
    }
  }

  private Bindable getBindable(ClassDeclaration expr, String s)
      throws CompileException, IOException {
    ICompilerFactory compilerFactory;
    try {
      compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to instantiate java compiler", e);
    }
    IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
    cbe.setClassName(expr.name);
    cbe.setExtendedClass(Utilities.class);
    cbe.setImplementedInterfaces(new Class[]{Bindable.class, Typed.class});
    cbe.setParentClassLoader(getClass().getClassLoader());
    if (CalcitePrepareImpl.DEBUG) {
      // Add line numbers to the generated janino class
      cbe.setDebuggingInformation(true, true, true);
    }
    return (Bindable) cbe.createInstance(new StringReader(s));
  }

  /** Interpreter node that reads from an {@link Enumerable}.
   *
   * <p>From the interpreter's perspective, it is a leaf node. */
  private static class EnumerableNode implements Node {
    private final Enumerable<Object[]> enumerable;
    private final Sink sink;

    public EnumerableNode(Enumerable<Object[]> enumerable,
        Interpreter interpreter, EnumerableInterpretable rel) {
      this.enumerable = enumerable;
      this.sink = interpreter.sink(rel);
    }

    public void run() throws InterruptedException {
      final Enumerator<Object[]> enumerator = enumerable.enumerator();
      while (enumerator.moveNext()) {
        Object[] values = enumerator.current();
        sink.send(Row.of(values));
      }
    }
  }
}

// End EnumerableInterpretable.java
