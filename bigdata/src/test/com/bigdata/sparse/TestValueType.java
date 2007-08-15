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
 * Created on Aug 14, 2007
 */

package com.bigdata.sparse;


import java.util.Date;
import java.util.Random;

import junit.framework.TestCase2;

/**
 * Test suite for {@link ValueType}.
 * 
 * @todo test use of thread-local variables to make the encoder and decoder
 *       thread-safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestValueType extends TestCase2 {

    /**
     * 
     */
    public TestValueType() {
        super();
    }

    /**
     * @param name
     */
    public TestValueType(String name) {
        super(name);
    }

    public void test_encode_null() {

        assertEquals(null, ValueType
                .decode(ValueType.encode(null)));

    }
    
    public void test_encode_Integer() {
        
        assertEquals(0, ValueType
                .decode(ValueType.encode(Integer.valueOf(0))));
        
        assertEquals(12, ValueType
                .decode(ValueType.encode(Integer.valueOf(12))));
        
        assertEquals(-12, ValueType
                .decode(ValueType.encode(Integer.valueOf(-12))));
        
        assertEquals(Integer.MAX_VALUE, ValueType
                .decode(ValueType.encode(Integer.valueOf(Integer.MAX_VALUE))));
    
        assertEquals(Integer.MIN_VALUE, ValueType
                .decode(ValueType.encode(Integer.valueOf(Integer.MIN_VALUE))));

    }
    
    public void test_encode_Long() {
        
        assertEquals(0L, ValueType
                .decode(ValueType.encode(Long.valueOf(0L))));
        
        assertEquals(12L, ValueType
                .decode(ValueType.encode(Long.valueOf(12L))));
        
        assertEquals(-12L, ValueType
                .decode(ValueType.encode(Long.valueOf(-12L))));
        
        assertEquals(Long.MAX_VALUE, ValueType
                .decode(ValueType.encode(Long.valueOf(Long.MAX_VALUE))));
    
        assertEquals(Long.MIN_VALUE, ValueType
                .decode(ValueType.encode(Long.valueOf(Long.MIN_VALUE))));

    }
    
    public void test_encode_Float() {
        
        assertEquals(0f, ValueType
                .decode(ValueType.encode(Float.valueOf(0f))));
        
        assertEquals(12f, ValueType
                .decode(ValueType.encode(Float.valueOf(12f))));
        
        assertEquals(-12f, ValueType
                .decode(ValueType.encode(Float.valueOf(-12f))));
        
        assertEquals(Float.MAX_VALUE, ValueType
                .decode(ValueType.encode(Float.valueOf(Float.MAX_VALUE))));
    
        assertEquals(Float.MIN_VALUE, ValueType
                .decode(ValueType.encode(Float.valueOf(Float.MIN_VALUE))));

    }
    
    public void test_encode_Double() {
        
        assertEquals(0d, ValueType
                .decode(ValueType.encode(Double.valueOf(0d))));
        
        assertEquals(12d, ValueType
                .decode(ValueType.encode(Double.valueOf(12d))));
        
        assertEquals(-12d, ValueType
                .decode(ValueType.encode(Double.valueOf(-12d))));
        
        assertEquals(Double.MAX_VALUE, ValueType
                .decode(ValueType.encode(Double.valueOf(Double.MAX_VALUE))));
    
        assertEquals(Double.MIN_VALUE, ValueType
                .decode(ValueType.encode(Double.valueOf(Double.MIN_VALUE))));

    }

    public void test_encode_Unicode() {
        
        assertEquals("", ValueType
                .decode(ValueType.encode("")));
        
        assertEquals("Hello World!", ValueType
                .decode(ValueType.encode("Hello World!")));
        
        assertEquals("Hello\0World!", ValueType
                .decode(ValueType.encode("Hello\0World!")));
        
    }
    
    public void test_encode_Date() {
        
        assertEquals(new Date(0L), ValueType
                .decode(ValueType.encode(new Date(0L))));
        
        assertEquals(new Date(12L), ValueType
                .decode(ValueType.encode(new Date(12L))));
        
        assertEquals(new Date(-12L), ValueType
                .decode(ValueType.encode(new Date(-12L))));
        
        long now = System.currentTimeMillis();
        
        assertEquals(new Date(now), ValueType
                .decode(ValueType.encode(new Date(now))));
    
    }

    public void test_encode_ByteArray() {

        assertEquals(new byte[] {}, (byte[]) ValueType.decode(ValueType
                .encode(new byte[] {})));

        assertEquals(new byte[] { 1, 0, 4, 2 }, (byte[]) ValueType
                .decode(ValueType.encode(new byte[] { 1, 0, 4, 2 })));

        Random r = new Random();

        byte[] data = new byte[r.nextInt(200)];

        r.nextBytes(data);

        assertEquals(data, (byte[]) ValueType.decode(ValueType.encode(data)));
        
    }
    
}
