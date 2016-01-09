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
package org.apache.calcite.util;

import java.util.AbstractSequentialList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * Implementation of list similar to {@link LinkedList}, but stores elements
 * in chunks of 32 elements.
 *
 * <p>ArrayList has O(n) insertion and deletion into the middle of the list.
 * ChunkList insertion and deletion are O(1).</p>
 *
 * @param <E> element type
 */
public class ChunkList<E> extends AbstractSequentialList<E> {
  /** The header is the first 3 slots of a chunk: a back link, forward link,
   * and the number of slots occupied. */
  private static final int HEADER_SIZE = 3;
  private static final int CHUNK_SIZE = 64;
  private static final Integer[] INTEGERS = new Integer[CHUNK_SIZE + HEADER_SIZE];

  static {
    for (int i = 0; i < INTEGERS.length; i++) {
      INTEGERS[i] = i;
    }
  }

  /** Number of elements in the list. */
  private int size;
  /** Pointer to first chunk in the list. */
  private Object[] first;
  /** Pointer to last chunk in the list. */
  private Object[] last;

  /**
   * Creates an empty ChunkList.
   */
  public ChunkList() {
  }

  /**
   * Creates a ChunkList whose contents are a given Collection.
   */
  public ChunkList(Collection<E> collection) {
    addAll(collection);
  }

  /**
   * For debugging and testing.
   */
  boolean isValid(boolean fail) {
    if ((first == null) != (last == null)) {
      assert !fail;
      return false;
    }
    if ((first == null) != (size == 0)) {
      assert !fail;
      return false;
    }
    int n = 0;
    for (E e : this) {
      if (n++ > size) {
        assert !fail;
        return false;
      }
    }
    if (n != size) {
      assert !fail;
      return false;
    }
    Object[] prev = null;
    for (Object[] chunk = first; chunk != null; chunk = next(chunk)) {
      if (prev(chunk) != prev) {
        assert !fail;
        return false;
      }
      prev = chunk;
      if (occupied(chunk) == 0) {
        assert !fail;
        return false;
      }
    }
    return true;
  }

  @Override public ListIterator<E> listIterator(int index) {
    return locate(index);
  }

  @Override public int size() {
    return size;
  }

  @Override public boolean add(E element) {
    Object[] chunk = last;
    int occupied;
    if (chunk == null) {
      chunk = first = last = new Object[CHUNK_SIZE + HEADER_SIZE];
      occupied = 0;
    } else {
      occupied = occupied(chunk);
      if (occupied == CHUNK_SIZE) {
        chunk = new Object[CHUNK_SIZE + HEADER_SIZE];
        setNext(last, chunk);
        setPrev(chunk, last);
        occupied = 0;
        last = chunk;
      }
    }
    setOccupied(chunk, occupied + 1);
    setElement(chunk, HEADER_SIZE + occupied, element);
    ++size;
    return true;
  }

  @Override public void add(int index, E element) {
    if (index == size) {
      add(element);
    } else {
      super.add(index, element);
    }
  }

  private static Object[] prev(Object[] chunk) {
    return (Object[]) chunk[0];
  }

  private static void setPrev(Object[] chunk, Object[] prev) {
    chunk[0] = prev;
  }

  private static Object[] next(Object[] chunk) {
    return (Object[]) chunk[1];
  }

  private static void setNext(Object[] chunk, Object[] next) {
    assert chunk != next;
    chunk[1] = next;
  }

  private static int occupied(Object[] chunk) {
    return (Integer) chunk[2];
  }

  private static void setOccupied(Object[] chunk, int size) {
    chunk[2] = INTEGERS[size];
  }

  private static Object element(Object[] chunk, int index) {
    return chunk[index];
  }

  private static void setElement(Object[] chunk, int index, Object element) {
    chunk[index] = element;
  }

  private ChunkListIterator locate(int index) {
    if (index == 0) {
      return new ChunkListIterator();
    }
    int n = 0;
    for (Object[] chunk = first;;) {
      final int occupied = occupied(chunk);
      final int nextN = n + occupied;
      final Object[] next = next(chunk);
      if (nextN > index || next == null) {
        return new ChunkListIterator(
            chunk, n, index - n - 1 + HEADER_SIZE,
            occupied + HEADER_SIZE);
      }
      n = nextN;
      chunk = next;
    }
  }

  /** Iterator over a {@link ChunkList}. */
  private class ChunkListIterator implements ListIterator<E> {
    private Object[] chunk;
    /** Offset in the list of the first element of this chunk. */
    private int startIndex;
    /** Offset within current chunk of the next element to return. */
    private int cursor;
    /** Offset within the current chunk of the last element returned. -1 if
     * next has not been called. */
    private int lastRet;
    /** Offset of the first unoccupied location in the current chunk. */
    private int end;

    /** Creates an iterator that is positioned before the first element.
     * The list may or may not be empty. */
    ChunkListIterator() {
      this(null, 0, -1, 0);
    }

    ChunkListIterator(Object[] chunk, int startIndex, int cursor, int end) {
      this.chunk = chunk;
      this.startIndex = startIndex;
      this.cursor = cursor;
      this.end = end;
    }

    public boolean hasNext() {
      return cursor + 1 < end
          || (chunk == null
          ? first != null
          : ChunkList.next(chunk) != null);
    }

    public E next() {
      ++cursor;
      assert cursor <= end;
      if (cursor == end) {
        if (chunk == null) {
          chunk = first;
        } else {
          chunk = ChunkList.next(chunk);
          startIndex += end - HEADER_SIZE;
        }
        if (chunk == null) {
          throw new NoSuchElementException();
        }
        cursor = HEADER_SIZE;
        end = occupied(chunk) + HEADER_SIZE;
      }
      return (E) element(chunk, cursor);
    }

    public boolean hasPrevious() {
      return cursor >= HEADER_SIZE || ChunkList.prev(chunk) != null;
    }

    public E previous() {
      --cursor;
      if (cursor == HEADER_SIZE - 1) {
        chunk = chunk == null ? last : ChunkList.prev(chunk);
        if (chunk == null) {
          throw new NoSuchElementException();
        }
        end = occupied(chunk);
        startIndex -= end;
        cursor = end - 1;
      }
      return (E) element(chunk, cursor);
    }

    public int nextIndex() {
      return startIndex + (cursor - HEADER_SIZE) + 1;
    }

    public int previousIndex() {
      return startIndex + (cursor - HEADER_SIZE);
    }

    public void remove() {
      if (chunk == null) {
        throw new IllegalStateException();
      }
      --size;
      if (end == HEADER_SIZE + 1) {
        // Chunk is now empty.
        final Object[] prev = prev(chunk);
        final Object[] next = ChunkList.next(chunk);
        if (next == null) {
          last = prev;
          if (prev == null) {
            first = null;
          } else {
            setNext(prev, null);
          }
          chunk = null;
          end = HEADER_SIZE;
          cursor = end - 1;
        } else {
          if (prev == null) {
            first = next;
            setPrev(next, null);
          } else {
            setNext(prev, next);
            setPrev(next, prev);
          }
          chunk = next;
          cursor = HEADER_SIZE;
          end = HEADER_SIZE + occupied(next);
        }
        return;
      }
      // Move existing contents down one.
      System.arraycopy(
          chunk, cursor + 1, chunk, cursor, end - cursor - 1);
      --end;
      setElement(chunk, end, null); // allow gc
      setOccupied(chunk, end - HEADER_SIZE);
      if (cursor == end) {
        final Object[] next = ChunkList.next(chunk);
        if (next != null) {
          startIndex += end - HEADER_SIZE;
          chunk = next;
          cursor = HEADER_SIZE - 1;
          end = HEADER_SIZE + occupied(next);
        }
      }
    }

    public void set(E e) {
      setElement(chunk, cursor, e);
    }

    public void add(E e) {
      if (chunk == null || end == CHUNK_SIZE + HEADER_SIZE) {
        // FIXME We create a new chunk, but the next chunk might be
        // less than half full. We should consider using it.
        Object[] newChunk = new Object[CHUNK_SIZE + HEADER_SIZE];
        if (chunk == null) {
          if (first != null) {
            setNext(newChunk, first);
            setPrev(first, newChunk);
          }
          first = newChunk;
          if (last == null) {
            last = newChunk;
          }
        } else {
          final Object[] next = ChunkList.next(chunk);
          setPrev(newChunk, chunk);
          setNext(chunk, newChunk);

          if (next == null) {
            last = newChunk;
          } else {
            setPrev(next, newChunk);
            setNext(newChunk, next);
          }
          startIndex += CHUNK_SIZE;
        }
        chunk = newChunk;
        end = cursor = HEADER_SIZE;
      } else {
        // Move existing contents up one.
        System.arraycopy(
            chunk, cursor, chunk, cursor + 1, end - cursor);
      }
      setElement(chunk, cursor, e);
//            ++offset;
      ++end;
      setOccupied(chunk, end - HEADER_SIZE);
      ++size;
    }
  }
}

// End ChunkList.java
