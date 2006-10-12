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
 * Created on Oct 8, 2006
 */

package com.bigdata.journal;

import java.util.Random;

import junit.framework.TestCase;

/**
 * Test suite for slot math.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */

public class TestSlotMath extends TestCase {

    Random r = new Random();
    
    /**
     * 
     */
    public TestSlotMath() {
    }

    /**
     * @param arg0
     */
    public TestSlotMath(String arg0) {
        super(arg0);
    }

    /**
     * Correct rejection by the constructor.
     */
    public void test_ctor_correctRejection() {

        try {
            new SlotMath(0);
            fail("Expecting "+IllegalArgumentException.class);
        }
        catch( IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        try {
            new SlotMath(8);
            fail("Expecting "+IllegalArgumentException.class);
        }
        catch( IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        try {
            new SlotMath(-128);
            fail("Expecting "+IllegalArgumentException.class);
        }
        catch( IllegalArgumentException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

    }
    
    /**
     * Normal constructor behavior.
     */
    public void test_ctor() {
        
        SlotMath slotMath = new SlotMath(128);
        
        assertEquals( "headerSize", 8, slotMath.headerSize );

        assertEquals( "slotSize", 128, slotMath.slotSize );
        
        assertEquals( "dataSize", 120, slotMath.dataSize );
        
    }

    /**
     * Test computes the #of slots required to write a byte[].
     */

    public void test_getSlotCount_correctRejection() {

        try {
        
            SlotMath slotMath = new SlotMath(128);

            slotMath.getSlotCount(0);
            
            fail("Expecting: "+IllegalArgumentException.class);
        
        }
        
        catch(IllegalArgumentException ex) {

            System.err.println("Ignoring expected exception: "+ex);
            
        }
        
    }

    /**
     * Test computes the #of slots required to write a byte[].
     */

    public void test_getSlotCount() {

        SlotMath slotMath = new SlotMath(32+8);

        assertEquals( "dataSize", 32, slotMath.dataSize );

        assertEquals(1,slotMath.getSlotCount(31));
        assertEquals(1,slotMath.getSlotCount(32));
        assertEquals(2,slotMath.getSlotCount(33));
        
        assertEquals(2,slotMath.getSlotCount(63));
        assertEquals(2,slotMath.getSlotCount(64));
        assertEquals(3,slotMath.getSlotCount(65));
        
    }
    
}
