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
 * Created on Jan 24, 2007
 */

package com.bigdata.btree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.bigdata.btree.DataOutputBuffer;
import com.bigdata.btree.ImmutableKeyBuffer;
import com.bigdata.btree.KeyBufferSerializer;
import com.bigdata.btree.MutableKeyBuffer;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestKeyBufferSerializer extends TestAbstractKeyBuffer {

    /**
     * 
     */
    public TestKeyBufferSerializer() {
            }

    /**
     * @param name
     */
    public TestKeyBufferSerializer(String name) {
        super(name);
    }

    
    /**
     * Simple test of round-trip serialization.
     * 
     * @throws IOException
     */
    public void test_serializer() throws IOException {

        final int nkeys = 4;
        ImmutableKeyBuffer expected = new ImmutableKeyBuffer(nkeys, nkeys+2,
                new byte[][] {//
                new byte[]{1,2},//
                new byte[]{1,2,2},//
                new byte[]{1,2,4,1},//
                new byte[]{1,2,5},//
                new byte[]{1,3}, // Note: ignored at nkeys=4
                new byte[]{2} // Note: ignored at nkeys=5
        });

        doSerializationTest(expected);
        
    }

    /**
     * Test with zero keys.
     * 
     * @throws IOException
     */
    public void test_serializer_noKeys() throws IOException {

        ImmutableKeyBuffer expected = new ImmutableKeyBuffer(0, 4,
                new byte[][] {});

        doSerializationTest(expected);
        
    }
    
    /**
     * Serialization round-trip test helper.
     * 
     * @param expected
     *            The key buffer - verified same data on round-trip
     *            serialization.
     * 
     * @throws IOException
     */
    protected void doSerializationTest(ImmutableKeyBuffer expected) throws IOException {
        
        // serialize.
        
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        
//        DataOutputStream dos = new DataOutputStream(baos);
        DataOutputBuffer dos = new DataOutputBuffer();
        
        KeyBufferSerializer.INSTANCE.putKeys(dos, expected);
        
//        dos.flush();

        // de-serialize.
        
        ByteArrayInputStream bais = new ByteArrayInputStream(dos.buf);

        DataInputStream dis = new DataInputStream(bais);

        ImmutableKeyBuffer actual = (ImmutableKeyBuffer) KeyBufferSerializer.INSTANCE
                .getKeys(dis);

        // verify same data.
        
        assertEquals( "nkeys", expected.nkeys, actual.nkeys);
        assertEquals( "maxKeys", expected.maxKeys, actual.maxKeys);
        assertEquals( "offsets", expected.offsets, actual.offsets);
        assertEquals( "buf", expected.buf, actual.buf);

        // convert to a mutable key buffer.
        MutableKeyBuffer expected2 = new MutableKeyBuffer(expected);
        
        // serialize the mutable key buffer.
        
//        baos = new ByteArrayOutputStream();
        
        dos = new DataOutputBuffer();
        
        KeyBufferSerializer.INSTANCE.putKeys(dos, expected2);
        
//        dos.flush();

        // de-serialize again.
        
        bais = new ByteArrayInputStream(dos.buf);

        dis = new DataInputStream(bais);

        actual = (ImmutableKeyBuffer) KeyBufferSerializer.INSTANCE
                .getKeys(dis);

        // verify same data.
        
        assertEquals( "nkeys", expected.nkeys, actual.nkeys);
        assertEquals( "maxKeys", expected.maxKeys, actual.maxKeys);
        assertEquals( "offsets", expected.offsets, actual.offsets);
        assertEquals( "buf", expected.buf, actual.buf);

    }
    
}
