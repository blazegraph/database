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
 * Created on Feb 16, 2012
 */

package com.bigdata.rdf.internal.encoder;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import junit.framework.TestCase2;

import org.openrdf.model.impl.URIImpl;

import com.bigdata.BigdataStatics;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.internal.impl.uri.FullyInlineURIIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * Base class for {@link IBindingSetEncoder}and {@link IBindingSetDecoder} test
 * suites.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBindingSetEncoderTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractBindingSetEncoderTestCase() {
    }

    /**
     * @param name
     */
    public AbstractBindingSetEncoderTestCase(String name) {
        super(name);
    }

    /**
     * When <code>true</code>, {@link #doEncodeDecodeTest(IBindingSet)} will
     * also verify that the {@link IVCache} assertions were decoded.
     */
    protected boolean testCache = true;

    /**
     * The namespace for the {@link BigdataValueFactory}.
     * 
     */
    protected String namespace = getName();

    /**
     * The value factory for that namespace.
     */
    protected BigdataValueFactory valueFactory = BigdataValueFactoryImpl
            .getInstance(namespace);

    /**
     * A {@link TermId} whose {@link IVCache} is set.
     */
    protected TermId<BigdataLiteral> termId;

    /**
     * A {@link TermId} whose {@link IVCache} is set.
     */
    protected TermId<BigdataLiteral> termId2;

    /**
     * A {@link BlobIV} whose {@link IVCache} is set.
     */
    protected BlobIV<BigdataLiteral> blobIV;

    /** A "mockIV". */
    protected TermId<BigdataValue> mockIV1;

    /** A "mockIV". */
    protected TermId<BigdataValue> mockIV2;

    /** A "mockIV". */
    protected TermId<BigdataValue> mockIV3;
    
    /** A "mockIV". */
    protected TermId<BigdataValue> mockIVCarryingUri;
    
    /** A "mockIV". */
    protected TermId<BigdataValue> mockIVCarryingBNode;
    
    /** An inline IV whose {@link IVCache} is set. */
    protected XSDIntegerIV<BigdataLiteral> inlineIV;

    /** An inline IV whose {@link IVCache} is NOT set. */
    protected IV<?,?> inlineIV2;

    /**
     * The encoder under test.
     */
    protected IBindingSetEncoder encoder;
    
    /**
     * The decoder under test.
     */
    protected IBindingSetDecoder decoder;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void setUp() throws Exception {

        super.setUp();
        
        termId = new TermId<BigdataLiteral>(VTE.LITERAL, 12/* termId */);
        termId.setValue(valueFactory.createLiteral("abc"));

        termId2 = new TermId<BigdataLiteral>(VTE.LITERAL, 36/* termId */);
        termId2.setValue(valueFactory.createLiteral("xyz"));

        blobIV = new BlobIV<BigdataLiteral>(VTE.LITERAL, 912/* hash */,
                (short) 0/* collisionCounter */);
        blobIV.setValue(valueFactory.createLiteral("bigfoo"));
        
        mockIV1 = (TermId) TermId.mockIV(VTE.LITERAL);
        mockIV1.setValue((BigdataValue) valueFactory.createLiteral("red"));

        mockIV2 = (TermId) TermId.mockIV(VTE.LITERAL);
        mockIV2.setValue((BigdataValue) valueFactory.createLiteral("blue"));

        mockIV3 = (TermId) TermId.mockIV(VTE.LITERAL);
        mockIV3.setValue((BigdataValue) valueFactory.createLiteral("green"));
        
        mockIVCarryingUri = (TermId) TermId.mockIV(VTE.URI);
        mockIVCarryingUri.setValue((BigdataValue) valueFactory.createURI("http://green.as.uri"));

        mockIVCarryingBNode = (TermId) TermId.mockIV(VTE.BNODE);
        mockIVCarryingBNode.setValue((BigdataValue) valueFactory.createBNode("_:green_as_bnode"));


        inlineIV = new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(100));
        inlineIV.setValue((BigdataLiteral) valueFactory.createLiteral("100",
                XSD.INTEGER));
        
        inlineIV2 = XSDBooleanIV.TRUE;
        
    }

    protected void tearDown() throws Exception {
        
        super.tearDown();
        
        // Clear references.
        encoder.release();
        encoder = null;
        decoder.release();
        decoder = null;
        valueFactory.remove();
        valueFactory = null;
        namespace = null;
        termId = termId2 = null;
        blobIV = null;
        mockIV1 = mockIV2 = mockIV3 = null;
        inlineIV = null;
        inlineIV2 = null;
        
    }

    protected void doEncodeDecodeTest(final IBindingSet expected) {

        doEncodeDecodeTest(expected, testCache);

    }

    protected void doEncodeDecodeTest(final IBindingSet expected,
            final boolean testCache) {

        final byte[] data = encoder.encodeSolution(expected);

        // Vector updates against the cache.
        encoder.flush();

        final Random r = new Random();
        
        if(r.nextBoolean()){

            // Decode.
            final IBindingSet actual = decoder.decodeSolution(data, 0/* off */,
                    data.length/* len */, true/* resolveCachedValues */);

            assertEquals(expected, actual, testCache);
        
        } else  {

            /*
             * Copy the record to be decoded to a different byte offset and the
             * re-decode the record. This allows us to check for correct
             * handling of the [off] argument by decodeSolution().
             */

            final int off2 = r.nextInt(20) + 1;

            // Decode from a different offset.
            final byte[] data2 = new byte[data.length + off2];

            System.arraycopy(data/* src */, 0/* srcPos */, data2/* dest */,
                    off2/* destPos */, data.length);

            final IBindingSet actual2 = decoder
                    .decodeSolution(data2, off2/* off */, data.length/* len */,
                            true/* resolveCachedValues */);

            assertEquals(expected, actual2, testCache);

        }
        
    }

    @SuppressWarnings("rawtypes")
    protected void assertEquals(final IBindingSet expected,
            final IBindingSet actual, final boolean testCache) {

        // Check the binding sets (w/o regard to the IVCache associations).
        super.assertEquals(expected, actual);

        if (!testCache)
            return;

        // Check the IVCache associations.
        final Iterator<Entry<IVariable, IConstant>> itr = expected.iterator();

        while (itr.hasNext()) {

            final Entry<IVariable, IConstant> e = itr.next();

            final IConstant c = e.getValue();

            final IV iv = (IV) c.get();

            /*
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/532
             * (ClassCastException during hash join (can not be cast to TermId))
             */
            if (iv.hasValue() && !iv.isInline()) {

                final IVariable var = e.getKey();

                final IConstant c2 = actual.get(var);

                assertNotNull(c2);

                final IV iv2 = (IV) c2.get();

                assertEquals(iv, iv2);

                if (!iv2.hasValue())
                    fail("IVCache not set on decode: " + iv);

                assertEquals(iv.getValue(), iv2.getValue());

            }
            
        }
        
    }

    public void test_encodeEmpty() {

        final IBindingSet expected = new ListBindingSet();

        doEncodeDecodeTest(expected);

    }

    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmpty() {

        final IBindingSet expected = new ListBindingSet();
        expected.set(Var.var("x"), new Constant<IV>(
                new XSDNumericIV<BigdataLiteral>(12)));

        doEncodeDecodeTest(expected);
    }

    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmpty2() {

        final IBindingSet expected = new ListBindingSet();
        expected.set(Var.var("x"), new Constant<IV>(
                new XSDNumericIV<BigdataLiteral>(12)));
        expected.set(Var.var("y"), new Constant<IV>(
                new FullyInlineURIIV<BigdataURI>(new URIImpl(
                        "http://www.bigdata.com"))));

        doEncodeDecodeTest(expected);

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

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(99)));

            doEncodeDecodeTest(expected);
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

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(99)));
            expected.set(Var.var("y"), new Constant<IV>(
                    new FullyInlineURIIV<BigdataURI>(new URIImpl(
                            "http://www.bigdata.com"))));

            doEncodeDecodeTest(expected);
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

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(99)));
            expected.set(Var.var("y"), new Constant<IV>(
                    new FullyInlineURIIV<BigdataURI>(new URIImpl(
                            "http://www.bigdata.com"))));

            doEncodeDecodeTest(expected);
        }

    }

    /**
     * Multiple solutions where an empty solution appears in the 1st solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutions4() {

        {
            final IBindingSet expected = new ListBindingSet();

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(12)));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(99)));
            expected.set(Var.var("y"), new Constant<IV>(
                    new FullyInlineURIIV<BigdataURI>(new URIImpl(
                            "http://www.bigdata.com"))));

            doEncodeDecodeTest(expected);
        }

    }

    /**
     * Multiple solutions where an empty solution appears in the last solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutions5() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(12)));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(
                    new XSDNumericIV<BigdataLiteral>(99)));
            expected.set(Var.var("y"), new Constant<IV>(
                    new FullyInlineURIIV<BigdataURI>(new URIImpl(
                            "http://www.bigdata.com"))));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();

            doEncodeDecodeTest(expected);
        }

    }

    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmptyWithCachedValue() {

        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("x"), new Constant<IV>(termId));

        doEncodeDecodeTest(expected);
    }

    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmptyWithCachedValues() {

        final IBindingSet expected = new ListBindingSet();
        expected.set(Var.var("x"), new Constant<IV>(termId));
        expected.set(Var.var("y"), new Constant<IV>(blobIV));

        doEncodeDecodeTest(expected);

    }

    /**
     * Test where an inline {@link IV} has its {@link IVCache} set.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/532">
     *      ClassCastException during hash join (can not be cast to TermId) </a>
     */
    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmptyWithCachedValuesAndInlineValues() {

        final IBindingSet expected = new ListBindingSet();
        expected.set(Var.var("x"), new Constant<IV>(termId));
        expected.set(Var.var("y"), new Constant<IV>(inlineIV));

        doEncodeDecodeTest(expected);

    }

    /**
     * Variant where the inline {@link IV} does NOT have its {@link IVCache}
     * set.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/532">
     *      ClassCastException during hash join (can not be cast to TermId) </a>
     */
    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmptyWithCachedValuesAndInlineValues2() {

        final IBindingSet expected = new ListBindingSet();
        expected.set(Var.var("x"), new Constant<IV>(termId));
        expected.set(Var.var("y"), new Constant<IV>(inlineIV2));

        doEncodeDecodeTest(expected);

    }

    /**
     * Multiple solutions where a variable does not appear in the 2nd solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutionsWithCachedValues() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));
            expected.set(Var.var("y"), new Constant<IV>(blobIV));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));

            doEncodeDecodeTest(expected);
        }

    }

    /**
     * Multiple solutions where a new variables appears in the 2nd solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutionsWithCachedValues2() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));
            expected.set(Var.var("y"), new Constant<IV>(blobIV));

            doEncodeDecodeTest(expected);
        }

    }

    /**
     * Multiple solutions where an empty solution appears in the middle of the
     * sequence.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutionsWithCachedValues3() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));
            expected.set(Var.var("y"), new Constant<IV>(blobIV));

            doEncodeDecodeTest(expected);
        }

    }

    /**
     * Multiple solutions where an empty solution appears in the 1st solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutionsWithCachedValue4() {

        {
            final IBindingSet expected = new ListBindingSet();

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));
            expected.set(Var.var("y"), new Constant<IV>(blobIV));

            doEncodeDecodeTest(expected);
        }

    }

    /**
     * Multiple solutions where an empty solution appears in the last solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutionsWithCachedValue5() {

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));
            expected.set(Var.var("y"), new Constant<IV>(blobIV));

            doEncodeDecodeTest(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();

            doEncodeDecodeTest(expected);
        }

    }

    /**
     * Unit test of a solution with 3 bindings.
     */
    public void test_solutionWithThreeBindings1() {

        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("x"), new Constant<IV<?, ?>>(termId2));
        expected.set(Var.var("y"), new Constant<IV<?, ?>>(blobIV));
        expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));

        doEncodeDecodeTest(expected);
        
    }

    /**
     * Unit test of a solution with 3 bindings, some of which do not have an
     * {@link IVCache} association. This test was added when some
     * {@link IVCache} associations were observed to be associated with the
     * wrong variables.
     */
    public void test_solutionWithThreeBindingsSomeNotCached1() {

        final TermId<BigdataLiteral> termIdNoCache = new TermId<BigdataLiteral>(
                VTE.LITERAL, 912/* termId */);

        {
            final IBindingSet expected = new ListBindingSet();

            expected.set(Var.var("y"), new Constant<IV<?, ?>>(termIdNoCache));
            expected.set(Var.var("x"), new Constant<IV<?, ?>>(termId2));
            expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));

            doEncodeDecodeTest(expected);
        }
        
        {
            final IBindingSet expected = new ListBindingSet();

            expected.set(Var.var("x"), new Constant<IV<?, ?>>(termId2));
            expected.set(Var.var("y"), new Constant<IV<?, ?>>(termIdNoCache));
            expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));

            doEncodeDecodeTest(expected);
        }
        
        {
            final IBindingSet expected = new ListBindingSet();

            expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));
            expected.set(Var.var("x"), new Constant<IV<?, ?>>(termId2));
            expected.set(Var.var("y"), new Constant<IV<?, ?>>(termIdNoCache));

            doEncodeDecodeTest(expected);
        }
        
    }

    /**
     * Unit test of a solution with 3 bindings, some of which do not have an
     * {@link IVCache} association and some of which have an inline IV. This
     * test was added when it was observed that we were pushing inline IVs into
     * the cache for the {@link IVBindingSetEncoderWithIVCache}.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/532">
     *      ClassCastException during hash join (can not be cast to TermId) </a>
     */
    public void test_solutionWithThreeBindingsSomeNotCachedSomeInline() {

        final TermId<BigdataLiteral> termIdNoCache = new TermId<BigdataLiteral>(
                VTE.LITERAL, 912/* termId */);

        {
            final IBindingSet expected = new ListBindingSet();

            expected.set(Var.var("y"), new Constant<IV<?, ?>>(termIdNoCache));
            expected.set(Var.var("x"), new Constant<IV<?, ?>>(inlineIV));
            expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));

            doEncodeDecodeTest(expected);
        }
        
        {
            final IBindingSet expected = new ListBindingSet();

            expected.set(Var.var("x"), new Constant<IV<?, ?>>(termId2));
            expected.set(Var.var("y"), new Constant<IV<?, ?>>(termIdNoCache));
            expected.set(Var.var("z"), new Constant<IV<?, ?>>(inlineIV2));

            doEncodeDecodeTest(expected);
        }
        
        {
            final IBindingSet expected = new ListBindingSet();

            expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));
            expected.set(Var.var("x"), new Constant<IV<?, ?>>(inlineIV2));
            expected.set(Var.var("y"), new Constant<IV<?, ?>>(termIdNoCache));

            doEncodeDecodeTest(expected);
        }
        
    }

    /**
     * Unit test of a solution with 3 bindings in a different order.
     */
    public void test_solutionWithThreeBindings2() {

        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));
        expected.set(Var.var("x"), new Constant<IV<?, ?>>(termId2));
        expected.set(Var.var("y"), new Constant<IV<?, ?>>(blobIV));

        doEncodeDecodeTest(expected);
        
    }

    protected BlobIV<BigdataLiteral> getVeryLargeLiteral() {
        
        final int len = 1024000;

        final StringBuilder sb = new StringBuilder(len);

        for (int i = 0; i < len; i++) {

            sb.append(Character.toChars('A' + (i % 26)));

        }

        final String s = sb.toString();

        final Random r = new Random();
        
        final int hash = r.nextInt();

        final short collisionCounter = (short) r.nextInt(12);

        final BlobIV<BigdataLiteral> blobIV2 = new BlobIV<BigdataLiteral>(
                VTE.LITERAL, hash, collisionCounter);

        blobIV2.setValue(valueFactory.createLiteral(s));

        return blobIV2;

    }

    /**
     * Unit test of a solution having a {@link BlobIV} with a
     * {@link BigdataLiteral} which is very large.
     */
    public void test_solutionWithVeryLargeObject() {

        final BlobIV<BigdataLiteral> blobIV2 = getVeryLargeLiteral();
        
        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("x"), new Constant<IV<?, ?>>(blobIV2));

        doEncodeDecodeTest(expected);
        
    }

    /**
     * Unit test of a solution having a {@link BlobIV} with a
     * {@link BigdataLiteral} which is very large plus a few other bindings.
     */
    public void test_solutionWithVeryLargeObject2() {

        final BlobIV<BigdataLiteral> blobIV2 = getVeryLargeLiteral();
        
        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("x"), new Constant<IV<?, ?>>(blobIV2));
        expected.set(Var.var("y"), new Constant<IV<?, ?>>(blobIV));
        expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));

        doEncodeDecodeTest(expected);
        
    }

    /**
     * Unit test of a solution having a {@link BlobIV} with a
     * {@link BigdataLiteral} which is very large plus a few other bindings
     * (different order from the test above).
     */
    public void test_solutionWithVeryLargeObject3() {

        final BlobIV<BigdataLiteral> blobIV2 = getVeryLargeLiteral();
        
        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));
        expected.set(Var.var("x"), new Constant<IV<?, ?>>(blobIV2));
        expected.set(Var.var("y"), new Constant<IV<?, ?>>(blobIV));

        doEncodeDecodeTest(expected);
        
    }

    public void test_solutionWithSameValueBoundTwice() {

        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("y"), new Constant<IV<?, ?>>(termId));
        expected.set(Var.var("z"), new Constant<IV<?, ?>>(termId));

        doEncodeDecodeTest(expected);

    }
    
    /**
     * Unit test with one mock IV.
     * <p>
     * Note: {@link TermId#mockIV(VTE)} is used to generate "mock" {@link IV}s
     * by operators which produce values (such as SUBSTR()) that are not in the
     * database. The termId for all "mock" {@link IV} is <code>0L</code>. While
     * {@link TermId#equals(Object)} takes the {@link IVCache} association into
     * account, the association is not yet available when we are de-serializing
     * an encoded solution and is not part of the key when the key is
     * constructed using {@link IVUtility#encode(IKeyBuilder, IV)}. In each case
     * this can lead to incorrectly resolving two "mock" {@link IV}s to the same
     * value in an internal case.
     * 
     * @see <a
     *      href="https://sourceforge.net/apps/trac/bigdata/ticket/475#comment:14"
     *      > Optimize serialization for query messages on cluster </a>
     */
    public void test_solutionWithOneMockIV() {

        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("y"), new Constant<IV<?, ?>>(termId));
        expected.set(Var.var("x"), new Constant<IV<?, ?>>(termId2));
        expected.set(Var.var("z"), new Constant<IV<?, ?>>(mockIV1));

        doEncodeDecodeTest(expected);

    }

    /**
     * Unit test with all mock IVs.
     */
    public void test_solutionWithAllMockIVs() {

        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("y"), new Constant<IV<?, ?>>(mockIV1));
        expected.set(Var.var("x"), new Constant<IV<?, ?>>(mockIV2));
        expected.set(Var.var("z"), new Constant<IV<?, ?>>(mockIV3));

        doEncodeDecodeTest(expected);

    }

    /**
     * Unit test with all mix of MockIVs, TermIds, and BlobIVs.
     */
    public void test_solutionWithMockIVAndOthersToo() {

        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("a"), new Constant<IV<?, ?>>(termId));
        expected.set(Var.var("y"), new Constant<IV<?, ?>>(mockIV1));
        expected.set(Var.var("c"), new Constant<IV<?, ?>>(blobIV));
        expected.set(Var.var("x"), new Constant<IV<?, ?>>(mockIV2));
        expected.set(Var.var("b"), new Constant<IV<?, ?>>(termId2));
        expected.set(Var.var("z"), new Constant<IV<?, ?>>(mockIV3));
        expected.set(Var.var("d"), new Constant<IV<?, ?>>(mockIVCarryingUri));
        expected.set(Var.var("e"), new Constant<IV<?, ?>>(mockIVCarryingBNode));

        doEncodeDecodeTest(expected);

    }

}
