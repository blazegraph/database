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
 * Created on Jan 24, 2007
 */

package com.bigdata.btree;

import java.io.DataInput;
import java.io.IOException;

import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestKeyBufferSerializer extends AbstractKeyBufferTestCase {

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
        
//        ByteArrayInputStream bais = new ByteArrayInputStream(dos.buf);
//
//        DataInputStream dis = new DataInputStream(bais);

        DataInput dis = new DataInputBuffer(dos.buf,0,dos.len);

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
        
//        bais = new ByteArrayInputStream(dos.buf);

        dis = new DataInputBuffer(dos.buf,0,dos.len);

        actual = (ImmutableKeyBuffer) KeyBufferSerializer.INSTANCE
                .getKeys(dis);

        // verify same data.
        
        assertEquals( "nkeys", expected.nkeys, actual.nkeys);
        assertEquals( "maxKeys", expected.maxKeys, actual.maxKeys);
        assertEquals( "offsets", expected.offsets, actual.offsets);
        assertEquals( "buf", expected.buf, actual.buf);

    }
    
}
