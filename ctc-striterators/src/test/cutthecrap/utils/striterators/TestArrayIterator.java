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
 * Created on Aug 24, 2013
 */
package cutthecrap.utils.striterators;

import junit.framework.TestCase2;

public class TestArrayIterator extends TestCase2 {

    public TestArrayIterator() {
    }

    public TestArrayIterator(String name) {
        super(name);
    }

    public void test_ctor() {

        try {
            new ArrayIterator<String>(null);
            fail();
        } catch (NullPointerException ex) {
            // ignore
        }
 
        try {
            new ArrayIterator<String>(null, 0, 0);
            fail();
        } catch (NullPointerException ex) {
            // ignore
        }

        try {
            new ArrayIterator<String>(new String[] {}, 0, 1);
            fail();
        } catch (IllegalArgumentException ex) {
            // ignore
        }

        try {
            new ArrayIterator<String>(new String[] {}, 0, -1);
            fail();
        } catch (IllegalArgumentException ex) {
            // ignore
        }

        try {
            new ArrayIterator<String>(new String[] {}, -1, 0);
            fail();
        } catch (IllegalArgumentException ex) {
            // ignore
        }

        new ArrayIterator<String>(new String[] {}, 0, 0);
        
        new ArrayIterator<String>(new String[] {"1"}, 0, 1);
        
        new ArrayIterator<String>(new String[] {"1"}, 1, 0);

        try {
            new ArrayIterator<String>(new String[] {"1"}, 1, 1);
            fail();
        } catch (IllegalArgumentException ex) {
            // ignore
        }
        
    }

    public void test_iterator() {

        assertSameIterator(new String[] {}, new ArrayIterator<String>(
                new String[] {}, 0, 0));

        assertSameIterator(new String[] {}, new ArrayIterator<String>(
                new String[] { "1" }, 1, 0));

        assertSameIterator(new String[] { "1" }, new ArrayIterator<String>(
                new String[] { "1" }, 0, 1));

        assertSameIterator(new String[] { "1" }, new ArrayIterator<String>(
                new String[] { "1", "2" }, 0, 1));

        assertSameIterator(new String[] { "1", "2" },
                new ArrayIterator<String>(new String[] { "1", "2" }, 0, 2));

        assertSameIterator(new String[] { "2" }, new ArrayIterator<String>(
                new String[] { "1", "2" }, 1, 1));

    }
    
}
