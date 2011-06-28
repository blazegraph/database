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
 * Created on Aug 14, 2007
 */

package com.bigdata.sparse;


import java.io.Serializable;
import java.util.Date;
import java.util.Random;

import junit.framework.TestCase2;

/**
 * Test suite for {@link ValueType}.
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

    public void test_encode_Object() {

        doEncodeDecodeTest(new MySerializable("abc"));
        
        doEncodeDecodeTest(new MySerializable(null));
        
        doEncodeDecodeTest(new MySerializable("ABC"));
        
    }

    private void doEncodeDecodeTest(final Object o) {

        final Object o2 = ValueType.decode(ValueType.encode(o));

        assertFalse(o == o2);

        assertTrue(o.equals(o2));
        
    }
    
    /**
     * Serializable object with simple state.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class MySerializable implements Serializable {
        
        private static final long serialVersionUID = -3616507592564793343L;
        
        private final String s;
        
        public MySerializable(String s) {
            
            this.s = s;
            
        }

        public boolean equals(Object o) {

            return equals((MySerializable)o);
            
        }
        
        public boolean equals(MySerializable o) {

            if (s == null)
                return o.s == null;

            return s.equals(o.s);

        }
        
    }

}
