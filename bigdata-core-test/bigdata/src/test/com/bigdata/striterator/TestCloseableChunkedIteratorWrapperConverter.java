/**

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
/*
 * Created on Sep 6, 2010
 */

package com.bigdata.striterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

import junit.framework.TestCase2;
import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Test suite for {@link CloseableChunkedIteratorWrapperConverter}. This class
 * accepts an {@link IChunkedIterator} and converts it into an
 * {@link ICloseableIterator} whose {@link ICloseableIterator#next()} method
 * visits the chunks reported by {@link IChunkedIterator#nextChunk()}, i.e.,
 * this converts between an {@link IChunkedIterator} and an {@link Iterator}
 * visiting chunks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestDechunkerator.java 3511 2010-09-06 20:45:37Z thompsonbry $
 */
public class TestCloseableChunkedIteratorWrapperConverter extends TestCase2 {

    /**
     * 
     */
    public TestCloseableChunkedIteratorWrapperConverter() {
    }

    /**
     * @param name
     */
    public TestCloseableChunkedIteratorWrapperConverter(final String name) {
        super(name);
    }

    /**
     * Iterator visits each row in an 2D array.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @param <E>
     */
    private static class MyArrayExpander<E> implements IChunkedIterator<E> {

        private final E[][] src;
        private int i = 0;
        public MyArrayExpander(final E[][] src) {
            this.src = src;
        }
        
        @Override
        public boolean hasNext() {
            return i < src.length;
        }

        @Override
        public E next() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            i = src.length; // advance beyohnd the end of the data.
        }

        @Override
        public E[] nextChunk() {
            if (!hasNext())
                throw new NoSuchElementException();
            return src[i++];
        }
        
    }
    
//    /**
//     * Unit test with a single empty chunk.
//     */
//    public void test_singleEmptyChunk() {
//
//        final Object[] expected = new Object[] {//
//                //
//        };
//
//        final CloseableChunkedIteratorWrapperConverter<Object> actual = new CloseableChunkedIteratorWrapperConverter<Object>(
//                new ChunkedArrayIterator<Object>(new Object[]{}));
//
//        assertSameIterator(expected, actual);
//
//    }

    /**
     * Unit test with a single empty chunk.
     */
    public void test_singleEmptyChunk2() {

        final String[][] expected = new String[][] {//
                new String[]{}//
        };

        final IChunkedIterator<String> src = new MyArrayExpander<String>(
                expected);

        final CloseableChunkedIteratorWrapperConverter<String> actual = new CloseableChunkedIteratorWrapperConverter<String>(
                src);

        assertSameIterator(expected, actual);

    }

    /**
     * Unit test with a single chunk with one element.
     */
    public void test_oneElement() {

        final String[][] expected = new String[][] {//
                new String[]{"A"}//
        };

        final IChunkedIterator<String> src = new MyArrayExpander<String>(
                expected);

        final CloseableChunkedIteratorWrapperConverter<String> actual = new CloseableChunkedIteratorWrapperConverter<String>(
                src);

        assertSameIterator(expected, actual);

    }
    
    /**
     * Unit test with two chunks with one element each.
     */
    public void test_twoChunks_oneElementEach() {

        final String[][] expected = new String[][] {//
                new String[]{"A"},//
                new String[]{"B"},//
        };

        final IChunkedIterator<String> src = new MyArrayExpander<String>(
                expected);

        final CloseableChunkedIteratorWrapperConverter<String> actual = new CloseableChunkedIteratorWrapperConverter<String>(
                src);

        assertSameIterator(expected, actual);

    }

    /**
     * Unit test with three chunks two of which have one element each and
     * one of which is empty.
     */
    public void test_threeChunks_1_0_1() {

        final String[][] expected = new String[][] {//
                new String[]{"A"},//
                new String[]{},//
                new String[]{"B"},//
        };

        final IChunkedIterator<String> src = new MyArrayExpander<String>(
                expected);

        final CloseableChunkedIteratorWrapperConverter<String> actual = new CloseableChunkedIteratorWrapperConverter<String>(
                src);

        assertSameIterator(expected, actual);

    }

    /**
     * Unit test with three chunks two of which have two elements each and one
     * of which is empty.
     */
    public void test_threeChunks_2_0_2() {

        final String[][] expected = new String[][] {//
                new String[]{"A","a"},//
                new String[]{},//
                new String[]{"B","b"},//
        };

        final IChunkedIterator<String> src = new MyArrayExpander<String>(
                expected);

        final CloseableChunkedIteratorWrapperConverter<String> actual = new CloseableChunkedIteratorWrapperConverter<String>(
                src);

        assertSameIterator(expected, actual);

    }

    /**
     * Unit test with three chunks, none of which is empty.
     */
    public void test_threeChunks_1_2_1() {

        final String[][] expected = new String[][] {//
                new String[]{"A"},//
                new String[]{"C","c"},//
                new String[]{"B"},//
        };

        final IChunkedIterator<String> src = new MyArrayExpander<String>(
                expected);

        final CloseableChunkedIteratorWrapperConverter<String> actual = new CloseableChunkedIteratorWrapperConverter<String>(
                src);

        assertSameIterator(expected, actual);

    }

}
