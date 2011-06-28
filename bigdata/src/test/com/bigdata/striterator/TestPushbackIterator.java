/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 7, 2008
 */

package com.bigdata.striterator;

import java.util.Arrays;

import com.bigdata.striterator.PushbackIterator;

import junit.framework.TestCase2;


/**
 * Unit test for {@link PushbackIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestPushbackIterator extends TestCase2 {

    /**
     * 
     */
    public TestPushbackIterator() {
    }

    /**
     * @param arg0
     */
    public TestPushbackIterator(String arg0) {
        super(arg0);
    }

    public void test_filter() {
        
        final PushbackIterator<Long> actual = new PushbackIterator<Long>(Arrays
                .asList(new Long[] { 1L, 5L, 2L }).iterator());

        assertTrue(actual.hasNext());
        assertEquals(Long.valueOf(1L),actual.next());

        actual.pushback();
        
        assertTrue(actual.hasNext());
        assertEquals(Long.valueOf(1L),actual.next());

        assertTrue(actual.hasNext());
        assertEquals(Long.valueOf(5L), actual.next());

        actual.pushback();

        try {
            actual.pushback();
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

        assertTrue(actual.hasNext());
        assertEquals(Long.valueOf(5L),actual.next());

        assertTrue(actual.hasNext());
        assertEquals(Long.valueOf(2L),actual.next());

        assertFalse(actual.hasNext());
        
        actual.pushback();

        assertTrue(actual.hasNext());
        assertEquals(Long.valueOf(2L),actual.next());

        assertFalse(actual.hasNext());

    }

}
