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
 * Created on Nov 5, 2006
 */

package com.bigdata.objndx;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.Adler32;

import com.bigdata.objndx.ChecksumUtility;

import junit.framework.TestCase;

/**
 * Test suite for {@link ChecksumUtility}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestChecksumUtility extends TestCase {

    Random r = new Random();
    
    ChecksumUtility chk = new ChecksumUtility();
    
    /**
     * 
     */
    public TestChecksumUtility() {
    }

    /**
     * @param arg0
     */
    public TestChecksumUtility(String arg0) {
        super(arg0);
    }

    
    /**
     * Test verifies that the checksum of the buffer is being computed
     * correctly.
     */
    public void test_checksum01() {
        
        byte[] data = new byte[100];
        r.nextBytes(data);

        Adler32 adler32 = new Adler32();
        adler32.update(data);
        final int expectedChecksum = (int) adler32.getValue();

        assertEquals(expectedChecksum, chk.checksum(ByteBuffer.wrap(data), 0,
                data.length));

    }

    /**
     * Test verifies that only the specified region of the buffer is used to
     * compute the checksum.
     */
    public void test_checksum02() {

        byte[] data = new byte[100];
        r.nextBytes(data);

        Adler32 adler32 = new Adler32();
        adler32.update(data, 10, 90);
        final int expectedChecksum = (int) adler32.getValue();

        assertEquals(expectedChecksum, chk.checksum(ByteBuffer.wrap(data), 10,
                data.length));
        
    }

    /**
     * Test verifies that the mark, position and limit are unchanged by the
     * checksum operation.
     */
    public void test_checksum03() {

        byte[] data = new byte[100];
        r.nextBytes(data);

        ByteBuffer buf = ByteBuffer.wrap(data);
        // set the limit.
        buf.limit(20);
        /*
         * set the mark (we have to choose a mark less than the position we will
         * set and test below or the mark will be discarded when we set the
         * position).
         */
        buf.position(9);
        buf.mark();
        // set the position.
        buf.position(12);
        chk.checksum(buf, 0, data.length);
        // verify limit unchanged.
        assertEquals(20,buf.limit());
        // verify position unchanged.
        assertEquals(12,buf.position());
        // reset the buffer to the mark and verify the mark was not changed.
        buf.reset();
        assertEquals(9,buf.position());
        
    }

    /**
     * Verify that the computed checksum is the same whether the buffer is
     * backed by an array or not.
     */
    public void test_checksum04() {
        
        byte[] data = new byte[100];
        r.nextBytes(data);

        Adler32 adler32 = new Adler32();
        adler32.update(data);
        final int expectedChecksum = (int) adler32.getValue();

        assertEquals(expectedChecksum, chk.checksum(ByteBuffer.wrap(data), 0,
                data.length));
        
        ByteBuffer direct = ByteBuffer.allocateDirect(data.length);
        direct.put(data);
        assertEquals(expectedChecksum, chk.checksum(direct, 0,
                data.length));
        
    }

    /**
     * Verify that the computed checksum is the same whether the buffer is
     * backed by an array or not when the checksum is computed for only a region
     * of the buffer.
     */
    public void test_checksum05() {
        
        byte[] data = new byte[100];
        r.nextBytes(data);

        Adler32 adler32 = new Adler32();
        adler32.update(data,20,100-10-20);
        final int expectedChecksum = (int) adler32.getValue();

        assertEquals(expectedChecksum, chk.checksum(ByteBuffer.wrap(data), 20,
                data.length-10));
        
        ByteBuffer direct = ByteBuffer.allocateDirect(data.length);
        direct.put(data);
        assertEquals(expectedChecksum, chk.checksum(direct, 20,
                data.length-10));
        
    }

}
