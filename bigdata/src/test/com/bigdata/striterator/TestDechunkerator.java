/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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

import java.util.Arrays;
import java.util.Iterator;

import junit.framework.TestCase2;

/**
 * Test suite for {@link Dechunkerator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDechunkerator extends TestCase2 {

    /**
     * 
     */
    public TestDechunkerator() {
    }

    /**
     * @param name
     */
    public TestDechunkerator(String name) {
        super(name);
    }

    /**
     * Unit test with a single empty chunk.
     */
    public void test_nothing() {

        final Object[] expected = new Object[] {

        };

        final Object[][] a = new Object[][] {};
        final Iterator<Object[]> src = Arrays.asList(a).iterator();
        final Dechunkerator<Object> actual = new Dechunkerator<Object>(src);

        assertSameIterator(expected, actual);

    }

    /**
     * Unit test with a single chunk with one element.
     */
    public void test_oneElement() {

        final Object[] expected = new Object[] {
                "A"
        };

        final Object[][] a = new Object[][] {new Object[]{"A"}};
        final Iterator<Object[]> src = Arrays.asList(a).iterator();
        final Dechunkerator<Object> actual = new Dechunkerator<Object>(src);

        assertSameIterator(expected, actual);

    }
    
    /**
     * Unit test with two chunks with one element each.
     */
    public void test_twoChunks_oneElementEach() {

        final Object[] expected = new Object[] {
                "A","B"
        };

        final Object[][] a = new Object[][] {//
                new Object[]{"A"},//
                new Object[]{"B"},//
                };
        final Iterator<Object[]> src = Arrays.asList(a).iterator();
        final Dechunkerator<Object> actual = new Dechunkerator<Object>(src);

        assertSameIterator(expected, actual);

    }

    /**
     * Unit test with three chunks two of which have one element each and
     * one of which is empty.
     */
    public void test_threeChunks_1_0_1() {

        final Object[] expected = new Object[] {
                "A","B"
        };

        final Object[][] a = new Object[][] {//
                new Object[]{"A"},//
                new Object[]{},//
                new Object[]{"B"},//
                };
        final Iterator<Object[]> src = Arrays.asList(a).iterator();
        final Dechunkerator<Object> actual = new Dechunkerator<Object>(src);

        assertSameIterator(expected, actual);

    }

    /**
     * Unit test with three chunks two of which have one element each and
     * one of which is empty.
     */
    public void test_threeChunks_1_2_1() {

        final Object[] expected = new Object[] {
                "A","a","b","B"
        };

        final Object[][] a = new Object[][] {//
                new Object[]{"A"},//
                new Object[]{"a","b"},//
                new Object[]{"B"},//
                };
        final Iterator<Object[]> src = Arrays.asList(a).iterator();
        final Dechunkerator<Object> actual = new Dechunkerator<Object>(src);

        assertSameIterator(expected, actual);

    }

}
