/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Oct 2, 2008
 */

package com.bigdata.search;

import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.search.FullTextIndex.Options;

/**
 * Unit tests for key formation for the {@link FullTextIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo write tests in which the docId is a negative long integer.
 */
public class TestKeyBuilder extends TestCase2 {

    /**
     * 
     */
    public TestKeyBuilder() {
    }

    /**
     * @param arg0
     */
    public TestKeyBuilder(String arg0) {
        super(arg0);
    }

    /**
     * Return a suitable {@link IKeyBuilder}.
     * <p>
     * Note: Just like the {@link FullTextIndex}, this overrides the collator
     * strength property to use the configured value or the default for the text
     * indexer rather than the standard default. This is done because you
     * typically want to recognize only Primary differences for text search
     * while you often want to recognize more differences when generating keys
     * for a B+Tree.
     */
    protected IKeyBuilder getKeyBuilder() {

        final Properties properties = getProperties();

        properties.setProperty(KeyBuilder.Options.STRENGTH, properties
            .getProperty(Options.INDEXER_COLLATOR_STRENGTH,
                    Options.DEFAULT_INDEXER_COLLATOR_STRENGTH));

        /*
         * Note: The choice of the language and country for the collator
         * should not matter much for this purpose since the total ordering
         * is not used except to scan all entries for a given term, so the
         * relative ordering between terms does not matter.
         */

        keyBuilder = KeyBuilder.newUnicodeInstance(properties);
     
        return keyBuilder;
        
    }
    private IKeyBuilder keyBuilder;
    
    /**
     * @todo this test needs to populate an index with terms that would match on
     *       a prefix match and then verify that they do match and that terms
     *       that are not prefix matches do not match.
     */
    public void test_prefixMatch_unicode() {

    }
    
    /**
     * Unit test verifies the relative sort order of a term and its successor,
     * of a prefix of that term and its successor, and that the prefix and the
     * successor of the prefix are ordered before and after the term and its
     * successor respectively.
     */
    public void test_keyOrder() {
        
        final IKeyBuilder keyBuilder = getKeyBuilder();
        
        final long docId = 0L;
        
        final int fieldId = 0;

        
        // the full term.
        final byte[] k0 = FullTextIndex.getTokenKey(keyBuilder, "brown",
                false/* successor */, docId, fieldId);
        
        // the successor of the full term.
        final byte[] k0s = FullTextIndex.getTokenKey(keyBuilder, "brown",
                true/* successor */, docId, fieldId);

        // verify sort key order for the full term and its successor.
        assertTrue(BytesUtil.compareBytes(k0, k0s) < 0);

        
        // a prefix of that term.
        final byte[] k1 = FullTextIndex.getTokenKey(keyBuilder, "bro",
                false/* successor */, docId, fieldId);
        
        // the successor of that prefix.
        final byte[] k1s = FullTextIndex.getTokenKey(keyBuilder, "bro",
                true/* successor */, docId, fieldId);
        
        // verify sort key order for prefix and its successor.
        assertTrue(BytesUtil.compareBytes(k0, k0s) < 0);
        
        // verify that the prefix ordered before the full term.
        assertTrue(BytesUtil.compareBytes(k1, k0) < 0);
        
        // verify that the successor of the prefix orders after the successor of the full term.
        assertTrue(BytesUtil.compareBytes(k1s, k0s) > 0);
        
    }

    /**
     * Succeeds iff a LT b.
     * 
     * @param a
     * @param b
     */
    protected void LT(final byte[] a, final byte[] b) {

        final int cmp = BytesUtil.compareBytes(a, b);

        if (cmp < 0)
            return;

        fail("cmp=" + cmp + ", a=" + BytesUtil.toString(a) + ", b="
                + BytesUtil.toString(b));

    }

    /**
     * Succeeds iff a GT b.
     * 
     * @param a
     * @param b
     */
    protected void GT(final byte[] a, final byte[] b) {

        final int cmp = BytesUtil.compareBytes(a, b);

        if (cmp > 0)
            return;

        fail("cmp=" + cmp + ", a=" + BytesUtil.toString(a) + ", b="
                + BytesUtil.toString(b));

    }
    
    /**
     * @todo this test needs to populate an index with terms that would match if
     *       we were allowing a prefix match and then verify that the terms are
     *       NOT matched. it should also verify that terms that are exact
     *       matches are matched.
     * 
     * @todo also test ability to extract the docId and fieldId from the key.
     * 
     * @todo refactor into an {@link ITupleSerializer}.
     * 
     * @todo make the fieldId optional in the key. this needs to be part of the
     *       state of the {@link ITupleSerializer}.
     */
    public void test_exactMatch_unicode() {
        
        final IKeyBuilder keyBuilder = getKeyBuilder();
        
        final long docId = 0L;
        
        final int fieldId = 0;

        
        // the full term.
        final byte[] termSortKey = FullTextIndex.getTokenKey(keyBuilder, "brown",
                false/* successor */, docId, fieldId);
        
        // the successor of the full term allowing prefix matches.
        final byte[] termPrefixMatchSuccessor = FullTextIndex.getTokenKey(keyBuilder, "brown",
                true/* successor */, docId, fieldId);

//        // the successor of the full term for an exact match.
//        final byte[] termExactMatchSuccessor = FullTextIndex.getTokenKey(
//                keyBuilder, "brown \0", true/* successor */, docId, fieldId);
//
//        /*
//         * verify sort key order for the full term and its prefix match
//         * successor.
//         */
//        LT(termSortKey, termPrefixMatchSuccessor);

//        /*
//         * verify sort key for the full term orders before its exact match
//         * successor.
//         */
//        LT(termSortKey, termExactMatchSuccessor);

        // term that is longer than the full term.
        final byte[] longerTermSortKey = FullTextIndex.getTokenKey(keyBuilder,
                "browns", false/* successor */, docId, fieldId);

        // verify sort order for the full term and the longer term.
        LT(termSortKey, longerTermSortKey);

        /*
         * verify longer term is less than the prefix match successor of the
         * full term.
         */
        LT(longerTermSortKey, termPrefixMatchSuccessor);

//        /*
//         * verify longer term is greater than the exact match successor of the
//         * full term.
//         */
//        GT(longerTermSortKey, termExactMatchSuccessor);

        fail("finish test");

    }

}
