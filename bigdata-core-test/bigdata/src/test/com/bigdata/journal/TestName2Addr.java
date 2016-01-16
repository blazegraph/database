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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.htree.HTree;
import com.bigdata.util.BytesUtil;

/**
 * Test suite for the {@link Name2Addr index} used to name index names to the
 * named indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestName2Addr extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestName2Addr() {
    }

    /**
     * @param arg0
     */
    public TestName2Addr(String arg0) {
        super(arg0);
    }

    /*
     * Note: You have to do this in Name2Addr since it explicitly DOES NOT use
     * any properties from the configuration file or the environment.
     */
//    @Override
//    public Properties getProperties() {
//        
//        final Properties p = new Properties(super.getProperties());
//        
//        p.setProperty(KeyBuilder.Options.COLLATOR,CollatorEnum.ASCII.name());
//        
//        return p;
//    }
    
    public void test_name2Addr_keyEncoding() {

        final String sharedPrefix = "kb";

        final String name1 = "kb.red";

        final String name2 = "kb.blue";

        Journal journal = new Journal(getProperties());

        try {

            final IIndex n2a = journal.getName2Addr();

            assertKeyEncoding(sharedPrefix, n2a);

            assertKeyEncoding(name1, n2a);

            assertKeyEncoding(name2, n2a);

        } finally {

            journal.destroy();

        }

    }

    private void assertKeyEncoding(final String name, final IIndex n2a) {

        @SuppressWarnings("rawtypes")
        final ITupleSerializer tupleSer = n2a.getIndexMetadata()
                .getTupleSerializer();

        final IKeyBuilder keyBuilder = tupleSer.getKeyBuilder();

        final IKeyBuilder keyBuilder2 = n2a.getIndexMetadata().getKeyBuilder();

        // The same key builder.
        assertTrue(keyBuilder == keyBuilder2);

        final byte[] a = keyBuilder.reset().append(name).getKey();

        final byte[] b = tupleSer.serializeKey(name);

        assertTrue(BytesUtil.compareBytes(a, b) == 0);

        if (log.isInfoEnabled())
            log.info("name=" + name + ", key=" + BytesUtil.toString(a));

    }
    
    /**
     * Test the ability to register and use named index, including whether the
     * named index is restart safe.
     */
    public void test_namedIndexScan() {

        final String sharedPrefix = "kb";
        
        final String name1 = "kb.red";
        
        final String name2 = "kb.blue";

        Journal journal = new Journal(getProperties());

        try {

            assertEquals(0L, journal.getLastCommitTime());

            assertEquals(
                    0,
                    getIndexNames(journal, null/* prefix */,
                            journal.getLastCommitTime()).size());

            assertEquals(
                    0,
                    getIndexNames(journal, ""/* prefix */,
                            journal.getLastCommitTime()).size());

            assertEquals(
                    0,
                    getIndexNames(journal, sharedPrefix/* prefix */,
                            journal.getLastCommitTime()).size());

            assertEquals(
                    0,
                    getIndexNames(journal, name1/* prefix */,
                            journal.getLastCommitTime()).size());

            assertEquals(
                    0,
                    getIndexNames(journal, name2/* prefix */,
                            journal.getLastCommitTime()).size());

            assertEquals(
                    0,
                    getIndexNames(journal, name1/* prefix */,
                            ITx.READ_COMMITTED).size());

            assertEquals(
                    0,
                    getIndexNames(journal, name2/* prefix */,
                            ITx.READ_COMMITTED).size());

            final UUID indexUUID = UUID.randomUUID();

            assertNull(journal.getIndex(name1));
            assertNull(journal.getIndex(name2));

            final HTreeIndexMetadata md1 = new HTreeIndexMetadata(
                    name1, indexUUID);

            final HTreeIndexMetadata md2 = new HTreeIndexMetadata(
                    name2, indexUUID);

            journal.register(name1, md1);

            assertNotNull(journal.getUnisolatedIndex(name1));
            assertNull(journal.getUnisolatedIndex(name2));

            journal.register(name2, md2);

            assertNotNull(journal.getUnisolatedIndex(name1));
            assertNotNull(journal.getUnisolatedIndex(name2));

            HTree htree1 = (HTree) journal.getUnisolatedIndex(name1);
            HTree htree2 = (HTree) journal.getUnisolatedIndex(name2);

            // different reference.
            assertTrue(htree1 != htree2);
            
            // Did not commit.
            assertEquals(0L, journal.getLastCommitTime());

            // Visible in unisolated view.
            assertEquals(
                    2,
                    getIndexNames(journal, null/* prefix */,
                            journal.getLastCommitTime()).size());

            assertEquals(
                    2,
                    getIndexNames(journal, ""/* prefix */,
                            journal.getLastCommitTime()).size());

            assertEquals(
                    2,
                    getIndexNames(journal, sharedPrefix/* prefix */,
                            journal.getLastCommitTime()).size());

            assertEquals(
                    1,
                    getIndexNames(journal, name1/* prefix */,
                            journal.getLastCommitTime()).size());

            assertEquals(
                    1,
                    getIndexNames(journal, name2/* prefix */,
                            journal.getLastCommitTime()).size());

            // Not visible in the read-committed view.
            assertEquals(
                    0,
                    getIndexNames(journal, name1/* prefix */,
                            ITx.READ_COMMITTED).size());

            assertEquals(
                    0,
                    getIndexNames(journal, name2/* prefix */,
                            ITx.READ_COMMITTED).size());

            /*
             * commit the journal
             */
            final long commitTime1 = journal.commit();

            final long lastCommitTime1 = journal.getLastCommitTime();

            assertEquals(commitTime1, lastCommitTime1);

            assertNotSame(0L, commitTime1);

            // Visible in committed view.
            assertEquals(2,
                    getIndexNames(journal, null/* prefix */, commitTime1)
                            .size());

            assertEquals(2, getIndexNames(journal, ""/* prefix */, commitTime1)
                    .size());

            assertEquals(
                    2,
                    getIndexNames(journal, sharedPrefix/* prefix */,
                            commitTime1).size());

            // Visible in the read-committed view.
            assertEquals(
                    1,
                    getIndexNames(journal, name1/* prefix */,
                            ITx.READ_COMMITTED).size());
            assertEquals(
                    1,
                    getIndexNames(journal, name2/* prefix */,
                            ITx.READ_COMMITTED).size());

            // Visible in read-historical committed view.
            assertEquals(
                    1,
                    getIndexNames(journal, name1/* prefix */,
                            commitTime1).size());
            assertEquals(
                    1,
                    getIndexNames(journal, name2/* prefix */,
                            commitTime1).size());

            if (journal.isStable()) {

                /*
                 * re-open the journal and test restart safety.
                 */
                journal = reopenStore(journal);

                htree1 = (HTree) journal.getUnisolatedIndex(name1);
                htree2 = (HTree) journal.getUnisolatedIndex(name2);

                assertNotNull("htree", htree1);
                assertNotNull("htree", htree2);

                // different reference.
                assertTrue(htree1 != htree2);
                
            }

        } finally {

            journal.destroy();

        }

    }

    /**
     * Return a set of all named indices on the journal.
     * 
     * @param jnl
     *            The journal.
     * @param prefix
     *            The prefix for those named indices (may be <code>null</code>
     *            or empty).
     * @param timestamp
     *            A timestamp which represents either a possible commit time on
     *            the store or a read-only transaction identifier.
     * 
     * @return The matching index names. The names will be reported in the
     *         returned set in the same order in which they are reported by the
     *         Journal.
     */
    private static Set<String> getIndexNames(final Journal jnl,
            final String prefix, final long timestamp) {

        if (log.isInfoEnabled())
            log.info("prefix=" + prefix + ", timestamp="
                    + TimestampUtility.toString(timestamp));

        final Set<String> names = new LinkedHashSet<String>();

        final Iterator<String> itr = jnl.indexNameScan(prefix, timestamp);

        while (itr.hasNext()) {

            final String name = itr.next();

            names.add(name);
            
        }
        
        return names;
        
    }
        
}
