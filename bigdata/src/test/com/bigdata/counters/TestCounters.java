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

import junit.framework.TestCase;

/**
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

    public void test_directCounters() {
        
        CounterSet root = new CounterSet();
        
        assertNull(root.getCounterByName("a"));
        
        {
            
            ICounter tmp = root.addCounter("a", new IInstrument<Double>() {

                public Double getValue() {
                    // TODO Auto-generated method stub
                    return null;
                }

            });

            assertNotNull(root.getCounterByName("a"));

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
        
    }
    
    public void test_children() {
        
        CounterSet root = new CounterSet();
        
        assertNull(root.getCounterByName("cpu"));
        
        CounterSet cpu = root.addCounterSet(new CounterSet("cpu"));

        assertNotNull(root.getCounterSetByName("cpu"));
        
        cpu.addCounter("a", new IInstrument<Double>() {

            public Double getValue() {
                // TODO Auto-generated method stub
                return null;
            }

        });
        
        assertNotNull(cpu.getCounterByName("a"));
        
        assertNotNull(cpu.getCounterByPath("a"));
        
        assertNotNull(root.getCounterByPath("/cpu/a"));

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
