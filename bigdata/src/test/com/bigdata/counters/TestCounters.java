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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.TestCase;

import org.xml.sax.SAXException;

import com.bigdata.counters.ICounterSet.IInstrumentFactory;

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
            public void sample() {
                setValue("foo");
            }
        });

        root.attach(tmp);
        
        assertNotNull(root.getPath("foo"));
        
    }
    
    public void test_directCounters() {
        
        CounterSet root = new CounterSet();
        
        assertNull(root.getChild("a"));
        
        {
            
            ICounter tmp = root.addCounter("a", new Instrument<Double>() {
                public void sample() {}
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

        root.addCounter("b", new Instrument<Double>() {
            public void sample() {}
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
        
        cpu.addCounter("a", new Instrument<Double>() {
            public void sample() {}
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

    /**
     * Test that empty path components are not allowed.
     */
    public void test_emptyPathComponentsNotAllowed() {

        final CounterSet countersRoot = new CounterSet();

        final String badpath = ICounterSet.pathSeparator
                + ICounterSet.pathSeparator + "foo";
        
        try {
            
            countersRoot.makePath(badpath);
            
            fail("Expecting: " + IllegalArgumentException.class);

        } catch (IllegalArgumentException ex) {
            
            System.err.println("Ignoring expected exception: " + ex);
            
        }

        try {
            
            countersRoot.getPath(badpath);
            
            fail("Expecting: " + IllegalArgumentException.class);

        } catch (IllegalArgumentException ex) {
            
            System.err.println("Ignoring expected exception: " + ex);
            
        }

        
    }
    
    /**
     * Test of XML (de-)serialization.
     * 
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public void test_xml() throws IOException, ParserConfigurationException, SAXException {
        
        final CounterSet root = new CounterSet();
        
        final ICounter elapsed = root.addCounter("elapsed", new Instrument<Long>() {
            public void sample(){
                setValue(System.currentTimeMillis());
                }
            });

        final CounterSet cpu = root.makePath("www.bigdata.com/cpu");

        final ICounter availableProcessors = cpu.addCounter("availableProcessors", new Instrument<Integer>() {
            public void sample(){
                setValue(Runtime.getRuntime().availableProcessors());
                }
            });

        final CounterSet memory = root.makePath("www.bigdata.com/memory");

        final ICounter maxMemory = memory.addCounter("maxMemory", new Instrument<Long>() {
            public void sample(){
                setValue(Runtime.getRuntime().maxMemory());
                }
            });

        CounterSet disk = root.makePath("www.bigdata.com/disk");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        // write out as XML.
        root.asXML(baos, "UTF-8", null/*filter*/);
        
        byte[] data = baos.toByteArray();
        
        Reader r = new InputStreamReader(new ByteArrayInputStream(data),"UTF-8");
        
        StringBuilder sb = new StringBuilder();
        
        while(true) {
            
            int ch = r.read();
            
            if(ch==-1) break;
            
            sb.append((char)ch);
            
        }
        
        System.err.println(sb.toString());
    
        {
            
            CounterSet tmp = new CounterSet();
            
            IInstrumentFactory instrumentFactory = new IInstrumentFactory() {

                public IInstrument newInstance(Class type) {

                    if (type == Double.class) {
                        return new Instrument<Double>() {
                            protected void sample() {
                            }
                        };
                    } else if (type == Long.class) {
                        return new Instrument<Long>() {
                            protected void sample() {
                            }
                        };
                    } else {
                        throw new UnsupportedOperationException("type=" + type);
                    }

                }
                
            };
            
            Pattern filter = null;
            
            tmp.readXML(new ByteArrayInputStream(data), instrumentFactory, filter);
            
            System.err.println("Read back:\n"+tmp.toString());
            
            /*
             * verify the counters that we had declared.
             * 
             * Note: This is a mess - I have to tunnel in to do the comparisons.
             * 
             * @todo also vet the other counters.
             */
            
            assertNotNull(tmp.getPath(availableProcessors.getPath()));

            assertEquals(availableProcessors.lastModified(), ((ICounter) tmp
                    .getPath(availableProcessors.getPath())).lastModified());

            assertEquals(//
                    ((Integer)((Instrument) availableProcessors.getInstrument())
                            .getCurrentValue()).intValue(),//
                            ((Long)((Instrument) ((ICounter) tmp
                            .getPath(availableProcessors.getPath()))
                            .getInstrument()).getCurrentValue()).intValue());
            
        }
        
    }
    
}
