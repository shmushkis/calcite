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
package org.apache.calcite.avatica.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implementation of {@link org.apache.calcite.avatica.util.Cursor}
 * on top of an {@link Iterator} that
 * returns a record for each row. The returned record is cached to avoid
 * multiple computations of current row.
 *
 * @param <E> Element type
 */
public abstract class IteratorCursor<E> extends AbstractCursor {
  private boolean done = false;
  private final Iterator<E> iterator;
  private E current = null;

  /**
   * Creates an {@code IteratorCursor}.
   *
   * @param iterator input iterator
   */
  protected IteratorCursor(Iterator<E> iterator) {
    this.iterator = iterator;
  }

  public boolean next() {
    if (iterator.hasNext()) {
      current = iterator.next();
      return true;
    }
    current = null;
    done = true;
    return false;
  }

  public void close() {
    current = null;
    done = true;
  }

  /**
   * Returns current row.
   * @return current row
   * @throws NoSuchElementException if the iteration has no more elements
   */
  protected E current() {
    if (done) {
      throw new NoSuchElementException();
    }
    return current;
  }
}

// End IteratorCursor.java
