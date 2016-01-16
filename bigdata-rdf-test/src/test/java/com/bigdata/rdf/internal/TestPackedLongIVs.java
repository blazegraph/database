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
 * Created on Nov 12, 2015
 */

package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.UUID;

import junit.framework.TestCase2;

import org.openrdf.model.URI;

import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.internal.constraints.MathUtility;
import com.bigdata.rdf.internal.impl.extensions.CompressedTimestampExtension;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.NumericIV;
import com.bigdata.rdf.internal.impl.literal.PackedLongIV;
import com.bigdata.rdf.internal.impl.literal.XSDDecimalIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.test.MockTermIdFactory;

/**
 * Test suite for math operations on {@link PackedLongIV} and
 * {@link CompressedTimestampExtension}. 
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestPackedLongIVs extends TestCase2 {

    public TestPackedLongIVs() {
    }

    public TestPackedLongIVs(final String name) {
        super(name);
    }

    /**
     * Test math operations such as +, -, *, /, MIN and MAX over the datatype.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testMath() {
        
        final BigdataValueFactory vf = 
            BigdataValueFactoryImpl.getInstance(getName() + UUID.randomUUID());

        final MockTermIdFactory termIdFactory = new MockTermIdFactory();
        
        final CompressedTimestampExtension<BigdataValue> ext = 
            new CompressedTimestampExtension<BigdataValue>(
                new IDatatypeURIResolver() {
                      @Override
                      public BigdataURI resolve(final URI uri) {
                         final BigdataURI buri = vf.createURI(uri.stringValue());
                         buri.setIV(termIdFactory.newTermId(VTE.URI));
                         return buri;
                      }
                });

        
        final BigdataValue bvZero = 
            vf.createLiteral("0", CompressedTimestampExtension.COMPRESSED_TIMESTAMP);
        final LiteralExtensionIV zero = ext.createIV(bvZero);
        zero.setValue(bvZero);

        final BigdataValue bfOne = 
            vf.createLiteral("1", CompressedTimestampExtension.COMPRESSED_TIMESTAMP);
        final LiteralExtensionIV one = ext.createIV(bfOne);
        one.setValue(bfOne);
        
        final BigdataValue bfTen = 
            vf.createLiteral("10", CompressedTimestampExtension.COMPRESSED_TIMESTAMP);
        final LiteralExtensionIV ten = ext.createIV(bfTen);
        ten.setValue(bfTen);
        
        final BigdataValue bfTwenty = 
            vf.createLiteral("20", CompressedTimestampExtension.COMPRESSED_TIMESTAMP);
        final LiteralExtensionIV twenty = ext.createIV(bfTwenty);
        twenty.setValue(bfTwenty);

        final NumericIV<BigdataLiteral, ?> result10a_int_act = MathUtility.literalMath(zero, ten, MathOp.PLUS);
        final NumericIV<BigdataLiteral, ?> result10b_int_act = MathUtility.literalMath(twenty, ten, MathOp.MINUS);
        final NumericIV<BigdataLiteral, ?> result10c_int_act = MathUtility.literalMath(ten, one, MathOp.MULTIPLY);
        final NumericIV<BigdataLiteral, ?> result10d_dec_act = MathUtility.literalMath(ten, one, MathOp.DIVIDE);
        final NumericIV<BigdataLiteral, ?> result10e_int_act = MathUtility.literalMath(ten, twenty, MathOp.MIN);
        final NumericIV<BigdataLiteral, ?> result10f_int_act = MathUtility.literalMath(twenty, ten, MathOp.MIN);
        final NumericIV<BigdataLiteral, ?> result20a_int_act = MathUtility.literalMath(ten, ten, MathOp.PLUS);
        final NumericIV<BigdataLiteral, ?> result20b_int_act = MathUtility.literalMath(ten, twenty, MathOp.MAX);
        final NumericIV<BigdataLiteral, ?> result20c_int_act = MathUtility.literalMath(twenty, ten, MathOp.MAX);
        
        final XSDIntegerIV<?> result10_int = new XSDIntegerIV<>(new BigInteger("10"));
        final XSDDecimalIV<?> result10_dec = new XSDDecimalIV<>(new BigDecimal(new BigInteger("10")));
        final XSDIntegerIV<?> result20_int = new XSDIntegerIV<>(new BigInteger("20"));

        assertEquals(result10_int, result10a_int_act);
        assertEquals(result10_int, result10b_int_act);
        assertEquals(result10_int, result10c_int_act);
        assertEquals(result10_dec, result10d_dec_act);
        assertEquals(result10_int, result10e_int_act);
        assertEquals(result10_int, result10f_int_act);
        assertEquals(result20_int, result20a_int_act);
        assertEquals(result20_int, result20b_int_act);
        assertEquals(result20_int, result20c_int_act);

    }

}
