/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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
