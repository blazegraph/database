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
 * Created on Jun 17, 2011
 */

package com.bigdata.rdf.internal;

import java.util.Arrays;

import junit.framework.TestCase2;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.test.MockTermIdFactory;
import com.bigdata.util.BytesUtil.UnsignedByteArrayComparator;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractEncodeDecodeKeysTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractEncodeDecodeKeysTestCase() {
        super();
    }

    /**
     * @param name
     */
    public AbstractEncodeDecodeKeysTestCase(String name) {
        super(name);
    }

    private MockTermIdFactory termIdFactory;

    /**
     * Decode a key from one of the statement indices. The components of the key
     * are returned in the order in which they appear in the key. The caller
     * must reorder those components using their knowledge of which index is
     * being decoded in order to reconstruct the corresponding RDF statement.
     * The returned array will always have 4 components. However, the last key
     * component will be <code>null</code> if there are only three components in
     * the <i>key</i>.
     * 
     * @param key
     *            The key.
     * 
     * @return An ordered array of the {@link IV}s for that key.
     */
    public static IV[] decodeStatementKey(final byte[] key, final int arity) {
    
        return IVUtility.decode(key, arity);
        
    }

    /**
     * Encodes an array of {@link IV}s and then decodes them and
     * verifies that the decoded values are equal-to the original values.
     * 
     * @param e
     *            The array of the expected values.
     */
    protected static IV<?, ?>[] doEncodeDecodeTest(final IV<?, ?>[] e) {
    
        /*
         * Encode.
         */
        final byte[] key;
        final IKeyBuilder keyBuilder = new KeyBuilder();
        {
    
            keyBuilder.reset();
            
            for (int i = 0; i < e.length; i++) {
    
                e[i].encode(keyBuilder);
    
            }
    
            key = keyBuilder.getKey();
        }
    
        /*
         * Decode
         */
        final IV<?, ?>[] a = decodeStatementKey(key, e.length);
        {
    
            for (int i = 0; i < e.length; i++) {
    
                final IV<?,?> expected = e[i];
    
                final IV<?,?> actual = a[i];
                
                if (!expected.equals(actual)) {
    
                    fail("values differ @ index=" + Integer.toString(i)
                            + " : expected=" + expected + ", actual=" + actual);
    
                }
    
                if (expected.hashCode() != actual.hashCode()) {
    
                    fail("hashCodes differ @ index=" + Integer.toString(i)
                            + " : expected=" + expected + "(hash="
                            + expected.hashCode() + ")" + ", actual=" + actual
                            + "(hash=" + actual.hashCode() + ")");
    
                }
    
            }
    
        }
        
        /*
         * Round-trip serialization.
         */
        {
    
            for (int i = 0; i < e.length; i++) {
    
                final IV<?,?> expected = e[i];
    
                final byte[] data = SerializerUtil.serialize(expected);
    
                final IV<?, ?> actual = (IV<?, ?>) SerializerUtil
                        .deserialize(data);
    
                if (!expected.equals(actual)) {
    
                    fail("Round trip serialization problem: expected="
                            + expected + ", actual=" + actual);
    
                }
    
                if (expected.hashCode() != actual.hashCode()) {
    
                    fail("hashCodes differ @ index=" + Integer.toString(i)
                            + " : expected=" + expected + "(hash="
                            + expected.hashCode() + ")" + ", actual=" + actual
                            + "(hash=" + actual.hashCode() + ")");
    
                }
    
            }
            
        }
    
        /*
         * clone(true)
         */
        {
    
            for (int i = 0; i < e.length; i++) {
    
                final IV<?,?> expected = e[i];
    
                final IV<?, ?> actual = expected.clone(true);
    
                if (!expected.equals(actual)) {
    
                    fail("Clone problem: expected=" + expected + ", actual="
                            + actual);

                }
    
                if (expected.hashCode() != actual.hashCode()) {
    
                    fail("hashCodes differ @ index=" + Integer.toString(i)
                            + " : expected=" + expected + "(hash="
                            + expected.hashCode() + ")" + ", actual=" + actual
                            + "(hash=" + actual.hashCode() + ")");
    
                }
    
            }
            
        }
    
        /*
         * clone(false)
         */
        {
    
            for (int i = 0; i < e.length; i++) {
    
                final IV<?,?> expected = e[i];
    
                final IV<?, ?> actual = expected.clone(false);
    
                if (!expected.equals(actual)) {
    
                    fail("Clone problem: expected=" + expected + ", actual="
                            + actual);

                }
    
                if (expected.hashCode() != actual.hashCode()) {
    
                    fail("hashCodes differ @ index=" + Integer.toString(i)
                            + " : expected=" + expected + "(hash="
                            + expected.hashCode() + ")" + ", actual=" + actual
                            + "(hash=" + actual.hashCode() + ")");
    
                }
    
            }
            
        }
    
        return a;
        
    }

    protected void setUp() throws Exception {
    	
        super.setUp();
    	
        termIdFactory = new MockTermIdFactory();
        
    }

    protected void tearDown() throws Exception {
    	
        super.tearDown();
        
    	termIdFactory = null;
    	
    }

    /**
     * Factory for mock {@link IV}s.
     */
    protected IV<?,?> newTermId(final VTE vte) {

        return termIdFactory.newTermId(vte);
        
    }

    /**
     * Verify the comparator for some set of {@link IV}s.
     * 
     * @param e
     */
    protected static void doComparatorTest(final IV<?,?>[] e) {
    
        final BlobsIndexHelper h = new BlobsIndexHelper();
        
        final IKeyBuilder keyBuilder = h.newKeyBuilder();
        
        final byte[][] keys = new byte[e.length][];
        for (int i = 0; i < e.length; i++) {
        
            final IV<?, ?> iv = e[i];
            
            // Encode as key.
            final byte[] key = IVUtility.encode(keyBuilder.reset(), iv).getKey();
            
            // Decode the key.
            final IV<?, ?> actualIV = IVUtility.decode(key);
    
            // Must compare as equal() for the rest of this logic to work.
            assertEquals(iv, actualIV);
            
            // Save the key.
            keys[i] = key;
            
        }
    
        // Clone the caller's array to avoid side effects.
        final IV<?, ?>[] a = e.clone();
    
        // Place into order according to the IV's Comparable implementation.
        Arrays.sort(a);
    
        // Place into unsigned byte[] order.
        Arrays.sort(keys, UnsignedByteArrayComparator.INSTANCE);
        
        for (int i = 0; i < e.length; i++) {
    
            // unsigned byte[] ordering.
            final IV<?,?> expectedIV = IVUtility.decode(keys[i]);
            
            // IV's Comparable ordering.
            final IV<?,?> actualIV = a[i];
            
            if (!expectedIV.equals(actualIV)) {
    
                /*
                 * The IV's Comparable does not agree with the required unsigned
                 * byte[] ordering semantics.
                 */
                
                fail("Order differs at index=" + i + ": expectedIV="
                        + expectedIV + ", actualIV=" + actualIV);
                
            }
            
        }
    
    }

}
