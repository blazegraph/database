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
 * Created on Jan 22, 2007
 */

package com.bigdata.btree;

import com.bigdata.btree.MutableKeyBuffer;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMutableKeyBuffer extends TestAbstractKeyBuffer {

    /**
     * 
     */
    public TestMutableKeyBuffer() {
    }

    /**
     * @param name
     */
    public TestMutableKeyBuffer(String name) {
        super(name);
    }

    /**
     * Test for key mutation operations (adding, removing, inserting, etc).
     */
    public void test_mutation() {
        
        MutableKeyBuffer kbuf = new MutableKeyBuffer(3);
        System.err.println(kbuf.toString());
        assertEquals("keys.length",3,kbuf.keys.length);
        assertEquals("maxKeys",3,kbuf.getMaxKeys());
        assertEquals("nkeys",0,kbuf.nkeys);
        assertNull(kbuf.keys[0]);
        assertEquals("prefixLength",0,kbuf.getPrefixLength());
        assertEquals("prefix",new byte[]{},kbuf.getPrefix());
        
        byte[] k1 = new byte[]{1,2,3};
        assertEquals("nkeys",1,kbuf.add(k1));
        System.err.println(kbuf.toString());
        assertEquals("nkeys",1,kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k1,kbuf.keys[0]);
        assertEquals("prefixLength",3,kbuf.getPrefixLength());
        assertEquals("prefix",new byte[]{1,2,3},kbuf.getPrefix());

        byte[] k2 = new byte[]{1,2,4};
        assertEquals("nkeys",2,kbuf.add(k2));
        System.err.println(kbuf.toString());
        assertEquals("nkeys",2,kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k1,kbuf.keys[0]);
        assertEquals(k2,kbuf.keys[1]);
        assertEquals("prefixLength",2,kbuf.getPrefixLength());
        assertEquals("prefix",new byte[]{1,2},kbuf.getPrefix());

        byte[] k3 = new byte[]{1,2,5};
        assertEquals("nkeys",3,kbuf.add(k3));
        System.err.println(kbuf.toString());
        assertEquals("nkeys",3,kbuf.nkeys);
        assertTrue(kbuf.isFull());
        assertEquals(k1,kbuf.keys[0]);
        assertEquals(k2,kbuf.keys[1]);
        assertEquals(k3,kbuf.keys[2]);
        assertEquals("prefixLength",2,kbuf.getPrefixLength());
        assertEquals("prefix",new byte[]{1,2},kbuf.getPrefix());
        
        // remove the 1st key, leaving two keys.
        assertEquals("nkeys",2,kbuf.remove(0));
        System.err.println(kbuf.toString());
        assertEquals("nkeys",2,kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k2,kbuf.keys[0]);
        assertEquals(k3,kbuf.keys[1]);
        assertNull(kbuf.keys[2]);
        assertEquals("prefixLength",2,kbuf.getPrefixLength());
        assertEquals("prefix",new byte[]{1,2},kbuf.getPrefix());
        
        // remove the last key, leaving one key.
        assertEquals("nkeys",1,kbuf.remove(1));
        System.err.println(kbuf.toString());
        assertEquals("nkeys",1,kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k2,kbuf.keys[0]);
        assertNull(kbuf.keys[1]);
        assertNull(kbuf.keys[2]);
        assertEquals("prefixLength",3,kbuf.getPrefixLength());
        assertEquals("prefix",new byte[]{1,2,4},kbuf.getPrefix());
        
        // insert a key in the 1st position (two keys in the buffer).
        assertEquals("nkeys",2,kbuf.insert(0,k1));
        System.err.println(kbuf.toString());
        assertEquals("nkeys",2,kbuf.nkeys);
        assertFalse(kbuf.isFull());
        assertEquals(k1,kbuf.keys[0]);
        assertEquals(k2,kbuf.keys[1]);
        assertNull(kbuf.keys[2]);
        assertEquals("prefixLength",2,kbuf.getPrefixLength());
        assertEquals("prefix",new byte[]{1,2},kbuf.getPrefix());

        // insert a key in the 2nd position (three keys in the buffer).
        byte[] k4 = new byte[]{1,2,3,0};
        assertEquals("nkeys",3,kbuf.insert(1,k4));
        System.err.println(kbuf.toString());
        assertEquals("nkeys",3,kbuf.nkeys);
        assertTrue(kbuf.isFull());
        assertEquals(k1,kbuf.keys[0]);
        assertEquals(k4,kbuf.keys[1]);
        assertEquals(k2,kbuf.keys[2]);
        assertEquals("prefixLength",2,kbuf.getPrefixLength());
        assertEquals("prefix",new byte[]{1,2},kbuf.getPrefix());

    }
    
    public void test_getPrefixLength() {

        MutableKeyBuffer kbuf = new MutableKeyBuffer(3);
        
        /*
         * no keys - zero length prefix.
         */
        assertEquals(0,kbuf.getPrefixLength());

        /*
         * one keys - prefix length is the length of that key.
         */
        kbuf.nkeys = 1;
        kbuf.keys[0] = new byte[]{2,3,5};
        assertEquals(3,kbuf.getPrefixLength());
        
        /*
         * more than one key - prefix length is the #of leading bytes in common.
         * between the first and last keys.
         */
        kbuf.nkeys = 2;
        kbuf.keys[1] = new byte[]{2,4};
        assertEquals(1,kbuf.getPrefixLength());

    }

}
