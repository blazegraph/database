/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.btree.raba;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A read-only {@link IRaba} that exposes a limited sub-range of a backing {@link IRaba}.
 * 
 * @author bryan
 * 
 * @see BLZG-1537 (Schedule more IOs when loading data)
 */
public class SubRangeRaba implements IRaba {

	/** The delegate. */
	private final IRaba delegate;
	/** The inclusive lower bound index into the delegate (offset). */
	private final int fromIndex;
	/** The exclusive upper bound index into the delegate. */
	private final int toIndex;
	/** The #of visible entries in this view of the delegate. */
	private final int size;
	
	/**
	 * 
	 * @param delegate
	 *            The delegate
	 * @param fromIndex
	 *            The inclusive lower bound in the delegate.
	 * @param toIndex
	 *            The exclusive upper bound in the delegate.
	 * @throws IllegalArgumentException
	 *             if the delegate is null.
	 * @throws IllegalArgumentException
	 *             if the fromIndex is LT ZERO (0).
	 * @throws IllegalArgumentException
	 *             if the toIndex is GT the delegate {@link IRaba#size()}.
	 * @throws IllegalArgumentException
	 *             if the fromIndex is GTE to toIndex.
	 */
	public SubRangeRaba(final IRaba delegate, final int fromIndex, final int toIndex) {

		if (delegate == null)
			throw new IllegalArgumentException();
		if (fromIndex < 0)
			throw new IllegalArgumentException();
		if (toIndex > delegate.size())
			throw new IllegalArgumentException();
		if (fromIndex >= toIndex)
			throw new IllegalArgumentException();

		this.delegate = delegate;
		this.fromIndex = fromIndex;
		this.toIndex = toIndex;
		this.size = toIndex - fromIndex;
	}

	@Override
	final public boolean isReadOnly() {
		return true;
	}

	@Override
	public boolean isKeys() {
		return delegate.isKeys();
	}

	@Override
	public int capacity() {
		return size;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * It is assumed that the delegate is full and this view does not allow
	 * mutation, so this always returns <code>false</code>.
	 */
	@Override
	public boolean isFull() {
		return true;
	}

	private void rangeCheck(final int index) {
		if (index < 0)
			throw new IndexOutOfBoundsException();
		if (index >= size)
			throw new IndexOutOfBoundsException();
	}
	
	@Override
	public boolean isNull(final int index) {
		rangeCheck(index);
		return delegate.isNull(index + fromIndex);
	}

	@Override
	public int length(final int index) {
		rangeCheck(index);
		return delegate.length(index + fromIndex);
	}

	@Override
	public byte[] get(final int index) {
		rangeCheck(index);
		return delegate.get(index + fromIndex);
	}

	@Override
	public int copy(final int index, final OutputStream os) {
		rangeCheck(index);
		return delegate.copy(index + fromIndex, os);
	}

	@Override
	public Iterator<byte[]> iterator() {
		
        return new Iterator<byte[]>() {

        	private int i = fromIndex;

            @Override
            public boolean hasNext() {

                return i < toIndex;

            }

            @Override
            public byte[] next() {

                if (!hasNext())
                    throw new NoSuchElementException();

				return delegate.get(i++);

            }

            @Override
            public void remove() {

                if (isReadOnly())
                    throw new UnsupportedOperationException();

                // @todo support remove on the iterator when mutable.
               throw new UnsupportedOperationException();
                
            }

        };
	}

	@Override
	public void set(int index, byte[] a) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int add(byte[] a) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int add(byte[] value, int off, int len) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int add(DataInput in, int len) throws IOException {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws UnsupportedOperationException
	 *             always.
	 * 
	 *             TODO I am not sure about the semantics of search() for a
	 *             sub-range so it is just disallowed for now. The issue is the
	 *             meaning of a key probe that is LT the key at fromIndex in the
	 *             delegate or GTE the key at toIndex in the delegate. Search in
	 *             fact can generally report positions before the start of an
	 *             ordered array of keys or after their end. But in this case we
	 *             can't see any keys before fromIndex or GTE toIndex. This
	 *             leads to some ambiguity about how to respond to key probes
	 *             that really below in a different {@link SubRangeRaba}.
	 */
	@Override
	public int search(final byte[] searchKey) {
		throw new UnsupportedOperationException();
	}

//	/**
//	 * {@inheritDoc}
//	 * 
//	 * @return The adjusted search position into this key range.
//	 * 
//	 * @throws IndexOutOfBoundsException
//	 *             if the probe key would lie before the first key in the
//	 *             buffer.
//	 * @throws IndexOutOfBoundsException
//	 *             if the probe key would lie after the last key in the buffer.
//	 */
//	@Override
//	public int search(final byte[] searchKey) {
//		// throw new UnsupportedOperationException();
//		final int search = delegate.search(searchKey);
//		int entryIndex = search;
//		if (entryIndex < 0) {
//			entryIndex = -entryIndex;
//		}
//		/*
//		 * Note: Throws exception if the search key lies outside of the key
//		 * range that is visible to the caller when using this IRaba instance.
//		 */
//		rangeCheck(entryIndex);
//		return search;
//	}

	@Override
	public String toString() {

		return getClass().getName() + "{fromIndex=" + fromIndex + ",toIndex=" + toIndex + ",size=" + size + ",delegate="
				+ delegate + "}";
		
	}
	
}
