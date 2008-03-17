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
 * Created on Mar 13, 2008
 */

package com.bigdata.counters;

import java.util.Iterator;
import java.util.regex.Pattern;

import junit.framework.TestCase;

/**
 * Unit tests for {@link CounterSet}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCounters extends TestCase {

    /**
     * 
     */
    public TestCounters() {
        super();
    }

    /**
     * @param arg0
     */
    public TestCounters(String arg0) {
        super(arg0);
    }

    public void test_ctor() {
        
        CounterSet root = new CounterSet();

        assertEquals("",root.getName());
        assertEquals("/",root.getPath());
        
    }

    /**
     * Tests of path construction.
     */
    public void test_pathSemantics1() {
        
        // root (name := "").
        final CounterSet root = new CounterSet();
        
        assertNull(root.getParent());
        assertEquals("/",root.getPath());
        assertEquals("",root.getName());

        // make a child.
        final CounterSet bigdata = root.makePath("www.bigdata.com");

        assertNotNull(bigdata);
        assertFalse(root == bigdata);
        
        // verify parent.
        assertTrue(root == bigdata.getParent());

        assertEquals("www.bigdata.com",bigdata.getName());
        
        assertEquals("/www.bigdata.com",bigdata.getPath());
        
        // make a child of a child using a relative path
        final CounterSet memory = bigdata.makePath("memory");
        
        assertTrue(bigdata == memory.getParent());

        assertEquals("memory",memory.getName());
        
        assertEquals("/www.bigdata.com/memory",memory.getPath());

        // make a child of a child using an absolute path.
        final CounterSet disk = root.makePath("/www.bigdata.com/disk");
        
        assertTrue(bigdata == disk.getParent());

        assertEquals("disk",disk.getName());
        
        assertEquals("/www.bigdata.com/disk",disk.getPath());

        /*
         * verify makePath recognizes existing nodes with absolute
         * paths.
         */
        
        assertTrue(root == root.makePath("/"));
        
        assertTrue(bigdata == root.makePath("/www.bigdata.com"));
        
        assertTrue(memory == root.makePath("/www.bigdata.com/memory"));

        assertTrue(disk == root.makePath("/www.bigdata.com/disk"));

        // illegal.
        try {
            root.makePath("");
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        
        /*
         * verify makePath recognizes existing nodes with relative
         * paths.
         */
        
        assertTrue(bigdata == root.makePath("www.bigdata.com"));
        
        assertTrue(memory == root.makePath("www.bigdata.com/memory"));

        assertTrue(memory == bigdata.makePath("memory"));
                
        /*
         * Test lookup with absolute paths.
         */
        
        assertTrue(root == root.getPath("/"));

        assertTrue(bigdata == root.getPath("/www.bigdata.com"));

        assertTrue(memory == root.getPath("/www.bigdata.com/memory"));

        /*
         * Test lookup with relative paths.
         */
        
        try {
            root.getPath("");
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        assertTrue(bigdata == root.getPath("www.bigdata.com"));

        assertTrue(memory == root.getPath("www.bigdata.com/memory"));

        assertTrue(memory == bigdata.getPath("memory"));
        
    }
    
    /**
     * FIXME Test attach (aka move). Verify move of children when source is a
     * root and otherwise move of the source itself. Verify detection of cycles
     * (cycles indicate an illegal request).
     */
    public void test_attach1() {

        CounterSet root = new CounterSet();

        CounterSet tmp = new CounterSet();
        
        tmp.addCounter("foo", new Instrument<String>() {
            public String getValue() {
                return "foo";
            }
        });

        root.attach(tmp);
        
        assertNotNull(root.getPath("foo"));
        
    }
    
    public void test_directCounters() {
        
        CounterSet root = new CounterSet();
        
        assertNull(root.getChild("a"));
        
        {
            
            ICounter tmp = root.addCounter("a", new IInstrument<Double>() {
                public Double getValue() {
                    return null;
                }
            });

            assertNotNull(root.getChild("a"));

            assertEquals("a", tmp.getName());

            assertEquals("/a", tmp.getPath());
            
        }
        
        // directly attached counter iterator.
        {
            
            Iterator<ICounter> itr = root.counterIterator(null/*filter*/);
            
            assertTrue( itr.hasNext() );
            
            ICounter c = itr.next();

            assertEquals("a",c.getName());
            
            assertEquals("/a",c.getPath());

            assertFalse( itr.hasNext() );
            
        }
        
        // recursively attached counter iterator.
        {
            
            Iterator<ICounter> itr = root.getCounters(null/*filter*/);
            
            assertTrue( itr.hasNext() );
            
            ICounter c = itr.next();

            assertEquals("a",c.getName());
            
            assertEquals("/a",c.getPath());

            assertFalse( itr.hasNext() );
            
        }

        root.addCounter("b", new IInstrument<Double>() {
            public Double getValue() {
                return null;
            }
        });

        // test with pattern filter.
        {

            Iterator<ICounter> itr = root.getCounters(Pattern.compile("/a"));
            
            assertTrue( itr.hasNext() );
            
            ICounter c = itr.next();

            assertEquals("a",c.getName());
            
            assertEquals("/a",c.getPath());

            assertFalse( itr.hasNext() );

        }
        // again.
        {

            Iterator<ICounter> itr = root.getCounters(Pattern.compile(".*b.*"));
            
            assertTrue( itr.hasNext() );
            
            ICounter c = itr.next();

            assertEquals("b",c.getName());
            
            assertEquals("/b",c.getPath());

            assertFalse( itr.hasNext() );

        }
        
    }

    /**
     * 
     */
    public void test_children() {
        
        final CounterSet root = new CounterSet();
        
        assertNull(root.getChild("cpu"));
        
        final CounterSet cpu = root.makePath("cpu");

        assertNotNull(root.getChild("cpu"));
        
        cpu.addCounter("a", new IInstrument<Double>() {
            public Double getValue() {return null;}
        });
        
        assertNotNull(cpu.getChild("a"));
        
        assertNotNull(cpu.getPath("a"));
        
        assertNotNull(root.getPath("/cpu/a"));

        // directly attached counter set iterator.
        {
            
            Iterator<ICounterSet> itr = root.counterSetIterator();
            
            assertTrue( itr.hasNext() );
            
            ICounterSet tmp = itr.next();

            assertEquals("cpu",tmp.getName());
            
            assertEquals("/cpu",tmp.getPath());

            assertFalse( itr.hasNext() );
            
        }
        
        // recursively attached counter iterator.
        {
            
            Iterator<ICounter> itr = root.getCounters(null/*filter*/);
            
            assertTrue( itr.hasNext() );
            
            ICounter c = itr.next();

            assertEquals("a",c.getName());
            
            assertEquals("/cpu/a",c.getPath());

            assertFalse( itr.hasNext() );
            
        }

    }
    
}
