/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.apache.flink.util.CollectionUtil.MAX_ARRAY_SIZE;

/**
 * Abstract base class for heap (object array) based implementations of priority queues, with support for fast deletes
 * via {@link HeapPriorityQueueElement}.
 *
 * @param <T> type of the elements contained in the priority queue.
 */
public abstract class AbstractHeapPriorityQueue<T extends HeapPriorityQueueElement>
	implements InternalPriorityQueue<T> {

	/** The array that represents the heap-organized priority queue. */
	@Nonnull
	protected T[] queue;

	/** The current size of the priority queue. */
	@Nonnegative
	protected int size;

	@SuppressWarnings("unchecked")
	public AbstractHeapPriorityQueue(@Nonnegative int minimumCapacity) {
		this.queue = (T[]) new HeapPriorityQueueElement[getHeadElementIndex() + minimumCapacity];
		this.size = 0;
	}

	@Override
	@Nullable
	public T poll() {
		return size() > 0 ? removeInternal(getHeadElementIndex()) : null;
	}

	@Override
	@Nullable
	public T peek() {
		// References to removed elements are expected to become set to null.
		return queue[getHeadElementIndex()];
	}

	@Override
	public boolean add(@Nonnull T toAdd) {
		/**
		 * 按照优先级规则添加元素到堆，并且设置元素在堆中的索引
		 */
		addInternal(toAdd);
		/**
		 * 判断此元素是否为堆的根元素
		 */
		return toAdd.getInternalIndex() == getHeadElementIndex();
	}

	@Override
	public boolean remove(@Nonnull T toRemove) {
		/**
		 * 获取元素在堆中的索引
		 */
		final int elementIndex = toRemove.getInternalIndex();
		/**
		 * 移除元素
		 */
		removeInternal(elementIndex);
		/**
		 * 返回结果为：移除的是否为堆根元素
		 */
		return elementIndex == getHeadElementIndex();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public void addAll(@Nullable Collection<? extends T> toAdd) {

		if (toAdd == null) {
			return;
		}

		resizeForBulkLoad(toAdd.size());

		for (T element : toAdd) {
			add(element);
		}
	}

	@SuppressWarnings({"unchecked"})
	@Nonnull
	public <O> O[] toArray(O[] out) {
		final int heapArrayOffset = getHeadElementIndex();
		if (out.length < size) {
			return (O[]) Arrays.copyOfRange(queue, heapArrayOffset, heapArrayOffset + size, out.getClass());
		} else {
			System.arraycopy(queue, heapArrayOffset, out, 0, size);
			if (out.length > size) {
				out[size] = null;
			}
			return out;
		}
	}

	/**
	 * Returns an iterator over the elements in this queue. The iterator
	 * does not return the elements in any particular order.
	 *
	 * @return an iterator over the elements in this queue.
	 */
	@Nonnull
	@Override
	public CloseableIterator<T> iterator() {
		return new HeapIterator();
	}

	/**
	 * Clears the queue.
	 */
	public void clear() {
		final int arrayOffset = getHeadElementIndex();
		Arrays.fill(queue, arrayOffset, arrayOffset + size, null);
		size = 0;
	}

	/**
	 * 对指定参数的值进行 expectedSize = totalSize + (totalSize/8), 最低要求容量为 totalSize 的扩容
	 * @param totalSize
	 */
	protected void resizeForBulkLoad(int totalSize) {
		if (totalSize > queue.length) {
			int desiredSize = totalSize + (totalSize >>> 3);
			resizeQueueArray(desiredSize, totalSize);
		}
	}

	/**
	 * 重新调整 数组（堆容量）大小
	 * @param desiredSize 期望大小
	 * @param minRequiredSize 最低必须满足的大小
	 */
	protected void resizeQueueArray(int desiredSize, int minRequiredSize) {
		if (isValidArraySize(desiredSize)) {
			queue = Arrays.copyOf(queue, desiredSize);
		} else if (isValidArraySize(minRequiredSize)) {
			queue = Arrays.copyOf(queue, MAX_ARRAY_SIZE);
		} else {
			throw new OutOfMemoryError("Required minimum heap size " + minRequiredSize +
				" exceeds maximum size of " + MAX_ARRAY_SIZE + ".");
		}
	}

	/**
	 * 将 element 移动到指定位置
	 * @param element ele
	 * @param idx index
	 */
	protected void moveElementToIdx(T element, int idx) {
		queue[idx] = element;
		element.setInternalIndex(idx);
	}

	/**
	 * Implements how to remove the element at the given index from the queue.
	 *
	 * @param elementIndex the index to remove.
	 * @return the removed element.
	 */
	protected abstract T removeInternal(@Nonnegative int elementIndex);

	/**
	 * Implements how to add an element to the queue.
	 *
	 * @param toAdd the element to add.
	 */
	protected abstract void addInternal(@Nonnull T toAdd);

	/**
	 * Returns the start index of the queue elements in the array.
	 */
	protected abstract int getHeadElementIndex();

	private static boolean isValidArraySize(int size) {
		return size >= 0 && size <= MAX_ARRAY_SIZE;
	}

	/**
	 * {@link Iterator} implementation for {@link HeapPriorityQueue}.
	 * {@link Iterator#remove()} is not supported.
	 */
	private final class HeapIterator implements CloseableIterator<T> {

		private int runningIdx;
		private final int endIdx;

		HeapIterator() {
			this.runningIdx = getHeadElementIndex();
			this.endIdx = runningIdx + size;
		}

		@Override
		public boolean hasNext() {
			return runningIdx < endIdx;
		}

		@Override
		public T next() {
			if (runningIdx >= endIdx) {
				throw new NoSuchElementException("Iterator has no next element.");
			}
			return queue[runningIdx++];
		}

		@Override
		public void close() {
		}
	}
}
