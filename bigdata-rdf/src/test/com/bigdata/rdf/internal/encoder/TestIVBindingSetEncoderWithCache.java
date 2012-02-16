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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * Test suite for {@link IVBindingSetEncoderWithIVCache}. This class supports an
 * {@link IV} to {@link BigdataValue} cache which provides lookup to resolve the
 * observed associations as reported by {@link IVCache#getValue()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIVBindingSetEncoderWithCache extends TestCase2 {

    /**
     * 
     */
    public TestIVBindingSetEncoderWithCache() {
    }

    public TestIVBindingSetEncoderWithCache(String name) {
        super(name);
    }

    private String namespace = getName();

    private BigdataValueFactory valueFactory = BigdataValueFactoryImpl
            .getInstance(namespace);

    /**
     * Backing store for caches.
     */
    private IRawStore store = new SimpleMemoryRawStore();

    /**
     * Empty operator - will use defaults for various annotations.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private BOp op = new BOpBase(new BOp[] {}/* args */,
            (Map<String, Object>) (Map) Collections.singletonMap(
                    IPredicate.Annotations.RELATION_NAME,
                    new String[] { namespace })/* anns */);

    /**
     * A {@link TermId} whose {@link IVCache} is set.
     */
    private TermId<BigdataLiteral> termId;

    /**
     * A {@link BlobIV} whose {@link IVCache} is set.
     */
    private BlobIV<BigdataLiteral> blobIV;

    /**
     * The encoder.
     */
    private IVBindingSetEncoderWithIVCache encoder = new IVBindingSetEncoderWithIVCache(
            store, false/* filter */, op);

    protected void setUp() throws Exception {
        
        super.setUp();
        
        termId = new TermId<BigdataLiteral>(VTE.LITERAL, 12/* termId */);
        termId.setValue(valueFactory.createLiteral("abc"));

        blobIV = new BlobIV<BigdataLiteral>(VTE.LITERAL, -218/* hash */,
                (short) 0/* collisionCounter */);
        blobIV.setValue(valueFactory.createLiteral("bigfoo"));

    }
    
    protected void tearDown() throws Exception {
        
        super.tearDown();
        
        // Clear references.
        encoder.release();
        encoder = null;
        namespace = null;
        valueFactory = null;
        termId = null;
        blobIV = null;
        store = null;
        op = null;
        
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
    public void test_encodeNonEmptyWithCachedValue() {

        final IBindingSet expected = new ListBindingSet();

        expected.set(Var.var("x"), new Constant<IV>(termId));

        doEncodeDecodeTest(expected);
    }

    @SuppressWarnings("rawtypes")
    public void test_encodeNonEmpty2() {

        final IBindingSet expected = new ListBindingSet();
        expected.set(Var.var("x"), new Constant<IV>(termId));
        expected.set(Var.var("y"), new Constant<IV>(blobIV));

        doEncodeDecodeTest(expected);

    }

    /**
     * Multiple solutions where a variable does not appear in the 2nd solution.
     */
    @SuppressWarnings("rawtypes")
    public void test_multipleSolutions() {

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
    public void test_multipleSolutions2() {

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
    public void test_multipleSolutions3() {

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

    @SuppressWarnings("rawtypes")
    private void doEncodeDecodeTest(final IBindingSet expected) {

        final byte[] data = encoder.encodeSolution(expected);

        // Vector updates against the cache.
        encoder.flush();

        // Decode.
        final IBindingSet actual = encoder
                .decodeSolution(data, 0/* fromOffset */,
                        data.length/* toOffset */, true/* resolveCachedValues */);

//        // Resolve cached BigdataValue objects for the IVs.
//        encoder.resolveCachedValues(actual);

        assertEquals(expected, actual);

        final Iterator<Entry<IVariable, IConstant>> itr = expected.iterator();

        while (itr.hasNext()) {

            final Entry<IVariable, IConstant> e = itr.next();

            final IConstant c = e.getValue();

            final IV iv = (IV) c.get();

            if (iv.hasValue()) {

                final IVariable var = e.getKey();

                final IConstant c2 = actual.get(var);

                assertNotNull(c2);

                final IV iv2 = (IV) c2.get();

                assertEquals(iv, iv2);

                assertTrue(iv2.hasValue());

                assertEquals(iv.getValue(), iv2.getValue());

            }
            
        }
        
    }

}
