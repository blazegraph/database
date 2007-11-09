/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Feb 13, 2007
 */

package com.bigdata.isolation;

import junit.framework.TestCase;

/**
 * Test suite for {@link Value}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestValue extends TestCase {

    /**
     * 
     */
    public TestValue() {
    }

    /**
     * @param arg0
     */
    public TestValue(String arg0) {
        super(arg0);
    }

    public void test_ctor() {

        { // non-deleted value.
            
            byte[] data = new byte[]{1,3,5};
            
            IValue value = new Value((short)1,false,data);
            
            assertEquals("versionCounter", (short)1,value.getVersionCounter());
            assertFalse("isDeleted",value.isDeleted());
            assertEquals("value",data,value.getValue());
            
        }

        { // deleted value.
            
            IValue value = new Value((short)12,true,null);
            
            assertEquals("versionCounter", (short)12,value.getVersionCounter());
            assertTrue("isDeleted",value.isDeleted());
            assertNull("value",value.getValue());
            
        }
        
        { // non-deleted value with null byte[].
            
            IValue value = new Value((short)12,false,null);
            
            assertEquals("versionCounter", (short)12,value.getVersionCounter());
            assertFalse("isDeleted",value.isDeleted());
            assertNull("value",value.getValue());
            
        }
        
    }

    public void test_ctor_correctRejection() {
        
        /*
         * illegal to have a negative value for the version counter.
         */
        try {
            new Value((short)-1,false,null);
            fail("Expecting: "+IllegalArgumentException.class);
        }catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        /*
         * illegal to have a non-null value with a deletion marker.
         */
        try {
            new Value((short)1,true,new byte[1]);
            fail("Expecting: "+IllegalArgumentException.class);
        }catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }

    }

    public void test_nextVersionCounter() {
        
        assertEquals((short) 1, new Value((short) 0, false, new byte[] { 1 })
                .nextVersionCounter());

        assertEquals((short) 2, new Value((short) 1, false, new byte[] { 1 })
                .nextVersionCounter());

        assertEquals(Short.MAX_VALUE, new Value((short) (Short.MAX_VALUE - 1),
                false, new byte[] { 1 }).nextVersionCounter());

        assertEquals(IValue.ROLLOVER_VERSION_COUNTER, new Value(
                Short.MAX_VALUE, false, new byte[] { 1 }).nextVersionCounter());
           
    }
    
}
