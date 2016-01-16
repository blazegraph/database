/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on May 27, 2011
 */

package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.UUID;

import junit.framework.TestCase;

import com.bigdata.util.Bytes;

/**
 * Test suite for {@link DTE}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDTE extends TestCase {

    public TestDTE() {
        super();
    }

    public TestDTE(String name) {
        super(name);
    }

    /**
	 * Unit test for {@link DTE} verifies that the correspondence between the
	 * enumerated types and the internal values is correct.
	 */
    public void test_DTE_selfConsistent() {

        for(DTE e : DTE.values()) {

            if (e == DTE.Extension) {
                continue;
            }
            
            // verify can decode from [v].
            {

                final DTE a = DTE.valueOf(e.v);

                if (e != a)
                    fail("expected: " + e + " (v=" + e.v + "), actual=" + a);
            }

            // verify can decode from [datatype].
            {

                final DTE a = DTE.valueOf(e.getDatatypeURI());

                if (e != a)
                    fail("expected: " + e + " (v=" + e.v + "), actual=" + a);
            }

            assertEquals(e.v, e.v());

        }

    }

    // boolean
    // byte, short, int, long
    // unsigned byte, unsigned short, unsigned int, unsigned long
    // float, double
    // xsd:integer, xsd:decimal

    public void test_XSDBoolean() {
        assertFalse(DTE.XSDBoolean.isNumeric());
        assertFalse(DTE.XSDBoolean.isFixedNumeric());
        assertFalse(DTE.XSDBoolean.isFloatingPointNumeric());
        assertFalse(DTE.XSDBoolean.isUnsignedNumeric());
        assertFalse(DTE.XSDBoolean.isSignedNumeric());
        assertFalse(DTE.XSDBoolean.isBigNumeric());
        assertEquals(Bytes.SIZEOF_BYTE, DTE.XSDBoolean.len());
        assertEquals(Boolean.class, DTE.XSDBoolean.getCls());
        assertEquals(XSD.BOOLEAN, DTE.XSDBoolean.getDatatypeURI());
    }
    
    public void test_XSDByte() {
        assertTrue(DTE.XSDByte.isNumeric());
        assertTrue(DTE.XSDByte.isFixedNumeric());
        assertFalse(DTE.XSDByte.isFloatingPointNumeric());
        assertFalse(DTE.XSDByte.isUnsignedNumeric());
        assertTrue(DTE.XSDByte.isSignedNumeric());
        assertFalse(DTE.XSDByte.isBigNumeric());
        assertEquals(Bytes.SIZEOF_BYTE, DTE.XSDByte.len());
        assertEquals(Byte.class, DTE.XSDByte.getCls());
        assertEquals(XSD.BYTE, DTE.XSDByte.getDatatypeURI());
    }
    
    public void test_XSDShort() {
        assertTrue(DTE.XSDShort.isNumeric());
        assertTrue(DTE.XSDShort.isFixedNumeric());
        assertFalse(DTE.XSDShort.isFloatingPointNumeric());
        assertFalse(DTE.XSDShort.isUnsignedNumeric());
        assertTrue(DTE.XSDShort.isSignedNumeric());
        assertFalse(DTE.XSDShort.isBigNumeric());
        assertEquals(Bytes.SIZEOF_SHORT, DTE.XSDShort.len());
        assertEquals(Short.class, DTE.XSDShort.getCls());
        assertEquals(XSD.SHORT, DTE.XSDShort.getDatatypeURI());
    }

    public void test_XSDInt() {
        assertTrue(DTE.XSDInt.isNumeric());
        assertTrue(DTE.XSDInt.isFixedNumeric());
        assertFalse(DTE.XSDInt.isFloatingPointNumeric());
        assertFalse(DTE.XSDInt.isUnsignedNumeric());
        assertTrue(DTE.XSDInt.isSignedNumeric());
        assertFalse(DTE.XSDInt.isBigNumeric());
        assertEquals(Bytes.SIZEOF_INT, DTE.XSDInt.len());
        assertEquals(Integer.class, DTE.XSDInt.getCls());
        assertEquals(XSD.INT, DTE.XSDInt.getDatatypeURI());
    }

    public void test_XSDLong() {
        assertTrue(DTE.XSDLong.isNumeric());
        assertTrue(DTE.XSDLong.isFixedNumeric());
        assertFalse(DTE.XSDLong.isFloatingPointNumeric());
        assertFalse(DTE.XSDLong.isUnsignedNumeric());
        assertTrue(DTE.XSDLong.isSignedNumeric());
        assertFalse(DTE.XSDLong.isBigNumeric());
        assertEquals(Bytes.SIZEOF_LONG, DTE.XSDLong.len());
        assertEquals(Long.class, DTE.XSDLong.getCls());
        assertEquals(XSD.LONG, DTE.XSDLong.getDatatypeURI());
    }

    // unsigned
    public void test_XSDUnsignedByte() {
        assertTrue(DTE.XSDUnsignedByte.isNumeric());
        assertTrue(DTE.XSDUnsignedByte.isFixedNumeric());
        assertFalse(DTE.XSDUnsignedByte.isFloatingPointNumeric());
        assertTrue(DTE.XSDUnsignedByte.isUnsignedNumeric());
        assertFalse(DTE.XSDUnsignedByte.isSignedNumeric());
        assertFalse(DTE.XSDUnsignedByte.isBigNumeric());
        assertEquals(Bytes.SIZEOF_BYTE, DTE.XSDUnsignedByte.len());
        assertEquals(Byte.class, DTE.XSDUnsignedByte.getCls());
        assertEquals(XSD.UNSIGNED_BYTE, DTE.XSDUnsignedByte.getDatatypeURI());
    }
    
    public void test_XSDUnsignedShort() {
        assertTrue(DTE.XSDUnsignedShort.isNumeric());
        assertTrue(DTE.XSDUnsignedShort.isFixedNumeric());
        assertFalse(DTE.XSDUnsignedShort.isFloatingPointNumeric());
        assertTrue(DTE.XSDUnsignedShort.isUnsignedNumeric());
        assertFalse(DTE.XSDUnsignedShort.isSignedNumeric());
        assertFalse(DTE.XSDUnsignedShort.isBigNumeric());
        assertEquals(Bytes.SIZEOF_SHORT, DTE.XSDUnsignedShort.len());
        assertEquals(Short.class, DTE.XSDUnsignedShort.getCls());
        assertEquals(XSD.UNSIGNED_SHORT, DTE.XSDUnsignedShort.getDatatypeURI());
    }

    public void test_XSDUnsignedInt() {
        assertTrue(DTE.XSDUnsignedInt.isNumeric());
        assertTrue(DTE.XSDUnsignedInt.isFixedNumeric());
        assertFalse(DTE.XSDUnsignedInt.isFloatingPointNumeric());
        assertTrue(DTE.XSDUnsignedInt.isUnsignedNumeric());
        assertFalse(DTE.XSDUnsignedInt.isSignedNumeric());
        assertFalse(DTE.XSDUnsignedInt.isBigNumeric());
        assertEquals(Bytes.SIZEOF_INT, DTE.XSDUnsignedInt.len());
        assertEquals(Integer.class, DTE.XSDUnsignedInt.getCls());
        assertEquals(XSD.UNSIGNED_INT, DTE.XSDUnsignedInt.getDatatypeURI());
    }

    public void test_XSDUnsignedLong() {
        assertTrue(DTE.XSDUnsignedLong.isNumeric());
        assertTrue(DTE.XSDUnsignedLong.isFixedNumeric());
        assertFalse(DTE.XSDUnsignedLong.isFloatingPointNumeric());
        assertTrue(DTE.XSDUnsignedLong.isUnsignedNumeric());
        assertFalse(DTE.XSDUnsignedLong.isSignedNumeric());
        assertFalse(DTE.XSDUnsignedLong.isBigNumeric());
        assertEquals(Bytes.SIZEOF_LONG, DTE.XSDUnsignedLong.len());
        assertEquals(Long.class, DTE.XSDUnsignedLong.getCls());
        assertEquals(XSD.UNSIGNED_LONG, DTE.XSDUnsignedLong.getDatatypeURI());
    }

    // float
    public void test_XSDFloat() {
        assertTrue(DTE.XSDFloat.isNumeric());
        assertTrue(DTE.XSDFloat.isFixedNumeric());
        assertTrue(DTE.XSDFloat.isFloatingPointNumeric());
        assertFalse(DTE.XSDFloat.isUnsignedNumeric());
        assertTrue(DTE.XSDFloat.isSignedNumeric());
        assertFalse(DTE.XSDFloat.isBigNumeric());
        assertEquals(Bytes.SIZEOF_FLOAT, DTE.XSDFloat.len());
        assertEquals(Float.class, DTE.XSDFloat.getCls());
        assertEquals(XSD.FLOAT, DTE.XSDFloat.getDatatypeURI());
    }

    public void test_XSDDouble() {
        assertTrue(DTE.XSDDouble.isNumeric());
        assertTrue(DTE.XSDDouble.isFixedNumeric());
        assertTrue(DTE.XSDDouble.isFloatingPointNumeric());
        assertFalse(DTE.XSDDouble.isUnsignedNumeric());
        assertTrue(DTE.XSDDouble.isSignedNumeric());
        assertFalse(DTE.XSDDouble.isBigNumeric());
        assertEquals(Bytes.SIZEOF_DOUBLE, DTE.XSDDouble.len());
        assertEquals(Double.class, DTE.XSDDouble.getCls());
        assertEquals(XSD.DOUBLE, DTE.XSDDouble.getDatatypeURI());
    }

    // big
    
    public void test_XSDInteger() {
        assertTrue(DTE.XSDInteger.isNumeric());
        assertFalse(DTE.XSDInteger.isFixedNumeric());
        assertFalse(DTE.XSDInteger.isFloatingPointNumeric());
        assertFalse(DTE.XSDInteger.isUnsignedNumeric());
        assertTrue(DTE.XSDInteger.isSignedNumeric());
        assertTrue(DTE.XSDInteger.isBigNumeric());
        assertEquals(0/*varlen*/, DTE.XSDInteger.len());
        assertEquals(BigInteger.class, DTE.XSDInteger.getCls());
        assertEquals(XSD.INTEGER, DTE.XSDInteger.getDatatypeURI());
    }

    public void test_XSDDecimal() {
        assertTrue(DTE.XSDDecimal.isNumeric());
        assertFalse(DTE.XSDDecimal.isFixedNumeric());
        assertTrue(DTE.XSDDecimal.isFloatingPointNumeric());
        assertFalse(DTE.XSDDecimal.isUnsignedNumeric());
        assertTrue(DTE.XSDDecimal.isSignedNumeric());
        assertTrue(DTE.XSDDecimal.isBigNumeric());
        assertEquals(0/*varlen*/, DTE.XSDDecimal.len());
        assertEquals(BigDecimal.class, DTE.XSDDecimal.getCls());
        assertEquals(XSD.DECIMAL, DTE.XSDDecimal.getDatatypeURI());
    }

    // other
    
    public void test_UUID() {
        assertFalse(DTE.UUID.isNumeric());
        assertFalse(DTE.UUID.isFixedNumeric());
        assertFalse(DTE.UUID.isFloatingPointNumeric());
        assertFalse(DTE.UUID.isUnsignedNumeric());
        assertFalse(DTE.UUID.isSignedNumeric());
        assertFalse(DTE.UUID.isBigNumeric());
        assertEquals(Bytes.SIZEOF_UUID, DTE.UUID.len());
        assertEquals(UUID.class, DTE.UUID.getCls());
        assertEquals(XSD.UUID, DTE.UUID.getDatatypeURI());
    }
    
    public void test_XSDString() {
        assertFalse(DTE.XSDString.isNumeric());
        assertFalse(DTE.XSDString.isFixedNumeric());
        assertFalse(DTE.XSDString.isFloatingPointNumeric());
        assertFalse(DTE.XSDString.isUnsignedNumeric());
        assertFalse(DTE.XSDString.isSignedNumeric());
        assertFalse(DTE.XSDString.isBigNumeric());
        assertEquals(0/*varlen*/, DTE.XSDString.len());
        assertEquals(String.class, DTE.XSDString.getCls());
        assertEquals(XSD.STRING, DTE.XSDString.getDatatypeURI());
    }

}
