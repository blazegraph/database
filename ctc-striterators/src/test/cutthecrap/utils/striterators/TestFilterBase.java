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
 * Created on Sep 27, 2010
 */

package cutthecrap.utils.striterators;

import java.util.Iterator;

import junit.framework.TestCase;

/**
 * Test suite for {@link FilterBase}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFilterBase extends TestCase {

    /**
     * 
     */
    public TestFilterBase() {
    }

    /**
     * @param name
     */
    public TestFilterBase(String name) {
        super(name);
    }

    public void test_filterBase_ctor() {
        
        final FilterBase fixture = new MockFilterBase();
        
        assertEquals("state", null, fixture.m_state);
        assertEquals("annotations", null, fixture.annotations);
        assertEquals("filterChain", null, fixture.filterChain);
        
    }

    public void test_filterBase_ctor2() {
        
        final Object state = new Object();
        
        final FilterBase fixture = new MockFilterBase(state);
        
        assertEquals("state", state, fixture.m_state);
        assertEquals("annotations", null, fixture.annotations);
        assertEquals("filterChain", null, fixture.filterChain);
        
    }

    public void test_filterBase_annotations() {
        
        final FilterBase fixture = new MockFilterBase();
        
        assertEquals("annotations", null, fixture.annotations);

        final String name = "name";
        final Object value = Integer.valueOf(0);
        final Object value2 = Integer.valueOf(1);

        try {
            fixture.getRequiredProperty(name);
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {

        }

        assertNull(fixture.getProperty(name));
        assertNull(fixture.setProperty(name, value));
        assertNotNull("annotations", fixture.annotations);
        assertEquals("annotations", 1, fixture.annotations.size());
        assertEquals(value, fixture.getProperty(name));
        assertEquals(value, fixture.getRequiredProperty(name));
        assertEquals(value, fixture.setProperty(name, value2));
        assertEquals(value2, fixture.getProperty(name));
        assertEquals(value2, fixture.getRequiredProperty(name));

    }

    public void test_filterBase_filterChain() {

        final Object s1 = "s1";
        final Object s2 = "s2";
        final Object s3 = "s3";

        final FilterBase f1, f2, f3;

        final FilterBase fixture = f1 = new MockFilterBase(s1);

        assertNull("filterChain", fixture.filterChain);

        fixture.addFilter(f2 = new MockFilterBase(s2));

        assertNotNull("filterChain", fixture.filterChain);

        fixture.addFilter(f3 = new MockFilterBase(s3));

        final IFilter[] expected = new IFilter[] { f2, f3 };

        final IFilter[] actual = f1.filterChain.toArray(new IFilter[] {});

        assertEquals("#filters", expected.length, actual.length);

        for (int i = 0; i < expected.length; i++) {

            assertEquals("filter[" + i + "]", expected[i], actual[i]);
            
        }
        
    }

    /**
     * Test creation of iterator without filter chain. make sure that the
     * context is passed through.
     */
    public void test_filter() {
        
        final FilterBase fixture = new MockFilterBase();

        final Object context = new Object();

        final Iterator src = EmptyIterator.DEFAULT;

        final MockIterator actual = (MockIterator) fixture.filter(src, context);

        assertNotNull(actual);
        assertTrue("src", actual.src == src);
        assertTrue("context", actual.context == context);

    }

    /**
     * Test creation of iterator with filter chain. Make sure that the create
     * order is correct and that the context is passed through to each iterator.
     * The iterators are assembled in FIFO order, so the iterator stack winds up
     * being LIFO.
     */
    public void test_filter2() {
        
        final FilterBase fixture = new MockFilterBase();
        final FilterBase fixture2 = new MockFilterBase();
        final FilterBase fixture3 = new MockFilterBase();

        final Object context = new Object();

        final Iterator src = EmptyIterator.DEFAULT;

        final MockIterator actual = (MockIterator) fixture.filter(src, context);

//        assertNotNull(actual);
//        assertTrue("src", actual.src == src);
//        assertTrue("context", actual.context == context);

        fail("write test");
    }

    /**
     * Mock object.
     */
    private static class MockFilterBase extends FilterBase {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        public MockFilterBase() {
            super();
        }
        
        public MockFilterBase(Object o) {
            super(o);
        }
        
        @Override
        protected Iterator filterOnce(Iterator src, Object context) {
            return new MockIterator(src, context);
        }
        
    }

    private static class MockIterator<E> implements Iterator<E> {

        final Iterator src;

        final Object context;

        public MockIterator(Iterator src,Object context) {

            this.src = src;

            this.context = context;
            
        }
        
        public boolean hasNext() {
            return false;
        }

        public E next() {
            return null;
        }

        public void remove() {
        }
        
    }
    
}
