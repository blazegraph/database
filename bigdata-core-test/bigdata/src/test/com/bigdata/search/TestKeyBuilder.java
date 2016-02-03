/*

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
 * Created on Oct 2, 2008
 */

package com.bigdata.search;

import java.util.Properties;

import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.search.FullTextIndex.Options;
import com.bigdata.util.BytesUtil;

/**
 * Unit tests for key formation for the {@link FullTextIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestKeyBuilder extends AbstractSearchTest {

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

	IndexMetadata getIndexMetadata() {
		return getNdx().getIndex().getIndexMetadata();
	}
    private IKeyBuilder keyBuilder;
    
    /**
     * Unit test verifies the relative sort order of a term and its successor,
     * of a prefix of that term and its successor, and that the prefix and the
     * successor of the prefix are ordered before and after the term and its
     * successor respectively.
     */
    public void test_keyOrder() {


        // The default Strength should be Primary.
        assertEquals(
                StrengthEnum.Primary,
                StrengthEnum
                        .valueOf(FullTextIndex.Options.DEFAULT_INDEXER_COLLATOR_STRENGTH));

        init(
        // Use the default Strength.
                KeyBuilder.Options.STRENGTH,
                FullTextIndex.Options.DEFAULT_INDEXER_COLLATOR_STRENGTH,
        // Use English.
                KeyBuilder.Options.USER_LANGUAGE, "en");
            
            final FullTextIndexTupleSerializer<Long> tupleSer = (FullTextIndexTupleSerializer<Long>) getIndexMetadata()
                    .getTupleSerializer();
            
            if(log.isInfoEnabled())
                log.info(tupleSer.toString());
            
//            assertEquals("en", ((DefaultKeyBuilderFactory) tupleSer
//                    .getKeyBuilderFactory()).getLocale().getLanguage());
//            
//            assertEquals(
//                    FullTextIndex.Options.DEFAULT_INDEXER_COLLATOR_STRENGTH,
//                    ((DefaultKeyBuilderFactory) tupleSer.getKeyBuilderFactory())
//                            .getLocale().getLanguage());
            
            doKeyOrderTest(getNdx(), -1L/* docId */, 0/* fieldId */, true/* fieldsEnabled */);
            doKeyOrderTest(getNdx(), 0L/* docId */,  0/* fieldId */, true/* fieldsEnabled */);
            doKeyOrderTest(getNdx(), 1L/* docId */, 12/* fieldId */, true/* fieldsEnabled */);

            doKeyOrderTest(getNdx(), -1L/* docId */, 0/* fieldId */, false/* fieldsEnabled */);
            doKeyOrderTest(getNdx(), 0L/* docId */, 0/* fieldId */, false/* fieldsEnabled */);
            doKeyOrderTest(getNdx(), 1L/* docId */, 0/* fieldId */, false/* fieldsEnabled */);
            
    }

    protected void doKeyOrderTest(final FullTextIndex<Long> ndx,
            final long docId, final int fieldId, final boolean fieldsEnabled) {

//        final boolean doublePrecision = false;
//        
//        final IKeyBuilder keyBuilder = getKeyBuilder();
//        
//        final IRecordBuilder<Long> tokenKeyBuilder = new DefaultRecordBuilder<Long>(
//                fieldsEnabled,//
//                doublePrecision,//
//                new DefaultDocIdExtension()//
//        );

        final ITupleSerializer<ITermDocKey<Long>, ITermDocVal> tupleSer = ndx
                .getIndex().getIndexMetadata().getTupleSerializer();
        
        // the full term.
        final byte[] k0 = tupleSer.serializeKey(new ReadOnlyTermDocKey<Long>(
                "brown", docId, fieldId));

        // the successor of the full term.
        final byte[] k0s;{
            final IKeyBuilder keyBuilder = tupleSer.getKeyBuilder();
            keyBuilder.reset();
            keyBuilder.appendText("brown", true/* unicode */, true/*successor*/);
            k0s = keyBuilder.getKey();
//            SuccessorUtil.tupleSer.serializeKey(new ReadOnlyTermDocKey<Long>("brown",
//                docId, fieldId));

        }
        
        // verify sort key order for the full term and its successor.
        assertTrue(BytesUtil.compareBytes(k0, k0s) < 0);

        // a prefix of that term.
        final byte[] k1 = tupleSer.serializeKey(new ReadOnlyTermDocKey<Long>(
                "bro", docId, fieldId));

        // the successor of that prefix.
        final byte[] k1s; {
            //k1s = tokenKeyBuilder.getKey(keyBuilder, "bro", true/* successor */, docId, fieldId);
            final IKeyBuilder keyBuilder = tupleSer.getKeyBuilder();
            keyBuilder.reset();
            keyBuilder.appendText("bro", true/* unicode */, true/*successor*/);
            k1s = keyBuilder.getKey();
        }
        
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

    static private class ReadOnlyTermDocKey<V extends Comparable<V>> implements
            ITermDocKey<V> {

        private final String token;

        private final V docId;

        private final int fieldId;
        
//        private final double termWeight;

        private ReadOnlyTermDocKey(final String token, final V docId,
                final int fieldId) {
            this.token = token;
            this.docId = docId;
            this.fieldId = fieldId;
        }

        private ReadOnlyTermDocKey(final String token, final V docId,
                final int fieldId, final double termWeight) {
            this.token = token;
            this.docId = docId;
            this.fieldId = fieldId;
//            this.termWeight = termWeight;
        }

        public String getToken() {
            return token;
        }

        public V getDocId() {
            return docId;
        }

        public int getFieldId() {
            return fieldId;
        }

        public double getLocalTermWeight() {
            return 0;
        }

    }
}
