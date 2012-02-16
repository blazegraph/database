/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Feb 15, 2012
 */

package com.bigdata.rdf.internal.encoder;

import junit.framework.TestCase2;

import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.internal.impl.uri.FullyInlineURIIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;

/**
 * Test suite for {@link IVBindingSetEncoder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIVBindingSetEncoder extends TestCase2 {

    /**
     * 
     */
    public TestIVBindingSetEncoder() {
    }

    public TestIVBindingSetEncoder(String name) {
        super(name);
    }

    private IVBindingSetEncoder encoder = new IVBindingSetEncoder(false/* filter */);

    protected void tearDown() throws Exception {
        
        super.tearDown();
        
        // Clear references.
        encoder.release();
        encoder = null;
        
    }

    public void test_encodeEmpty() {

        final IBindingSet expected = new ListBindingSet();

        doEncodeDecodeTest(encoder, expected);

    }

    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmpty() {

        final IBindingSet expected = new ListBindingSet();
        expected.set(Var.var("x"), new Constant<IV>(
                new XSDNumericIV<BigdataLiteral>(12)));

        doEncodeDecodeTest(encoder, expected);
    }

    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmpty2() {

        final IBindingSet expected = new ListBindingSet();
        expected.set(Var.var("x"), new Constant<IV>(
                new XSDNumericIV<BigdataLiteral>(12)));
        expected.set(Var.var("y"), new Constant<IV>(
                new FullyInlineURIIV<BigdataURI>(new URIImpl(
                        "http://www.bigdata.com"))));

        doEncodeDecodeTest(encoder, expected);

    }

    /**
     * Multiple solutions where a variable does not appear in the 2nd solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutions() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(12)));
            expected.set(Var.var("y"), new Constant<IV>(
                    new FullyInlineURIIV<BigdataURI>(new URIImpl(
                            "http://www.bigdata.com"))));

            doEncodeDecodeTest(encoder, expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(99)));

            doEncodeDecodeTest(encoder, expected);
        }

    }

    /**
     * Multiple solutions where a new variables appears in the 2nd solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutions2() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(12)));

            doEncodeDecodeTest(encoder, expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(99)));
            expected.set(Var.var("y"), new Constant<IV>(
                    new FullyInlineURIIV<BigdataURI>(new URIImpl(
                            "http://www.bigdata.com"))));

            doEncodeDecodeTest(encoder, expected);
        }

    }

    /**
     * Multiple solutions where an empty solution appears in the middle of the
     * sequence.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutions3() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(12)));

            doEncodeDecodeTest(encoder, expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();

            doEncodeDecodeTest(encoder, expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(99)));
            expected.set(Var.var("y"), new Constant<IV>(
                    new FullyInlineURIIV<BigdataURI>(new URIImpl(
                            "http://www.bigdata.com"))));

            doEncodeDecodeTest(encoder, expected);
        }

    }

    private void doEncodeDecodeTest(final IVBindingSetEncoder encoder,
            final IBindingSet expected) {

        final byte[] data = encoder.encodeSolution(expected);

        final IBindingSet actual = encoder.decodeSolution(data,
                0/* fromOffset */, data.length/* toOffset */);

        assertEquals(expected, actual);
    }

}
