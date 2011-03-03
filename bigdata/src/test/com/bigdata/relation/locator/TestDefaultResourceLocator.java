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
 * Created on Jul 9, 2008
 */

package com.bigdata.relation.locator;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.AbstractResource;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.NT;

/**
 * Test suite for location relations, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo make this a proxy test case and run for {@link Journal} and
 *       {@link IBigdataFederation}
 */
public class TestDefaultResourceLocator extends TestCase2 {

    public TestDefaultResourceLocator() {
        super();
    }
    
    public TestDefaultResourceLocator(String name) {
        super(name);
    }

    public Properties getProperties() {
        
        Properties properties = super.getProperties();
        
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
                .toString());
        
        return properties;
        
    }
    
    /**
     * @todo locate a relation.
     * 
     * @todo locate a relation, write on it, verify read-committed view not yet
     *       visible, then commit and verify the read committed view is visible.
     * 
     * @todo same as above, but the relation is on a temporary store that is
     *       added to the set of index managers against which relations are
     *       resolved.
     * 
     */
    public void test_locateRelation() {
        
        final Properties properties = getProperties();
        
        final Journal store = new Journal( properties );

        final String namespace = "test";
        
        try {

            // instantiate relation.
            MockRelation mockRelation = new MockRelation(store, namespace,
                    ITx.UNISOLATED, properties);
            
            /*
             * the index for the relation does not exist yet. we verify
             * that, and then create the index.
             */

            assertNull(mockRelation.getIndex());

            // create indices.
            mockRelation.create();

            assertNotNull(mockRelation.getIndex());

            {
                /*
                 * Should be locatable now since the writes on the global row
                 * store are unisolated (ah!, but only if you are using the
                 * concurrency control API)
                 */
                assertNotNull(store.getResourceLocator().locate(namespace,
                        ITx.UNISOLATED));

                // a request for the unisolated view gives us the same instance.
                assertTrue(((MockRelation) store.getResourceLocator().locate(
                        namespace, ITx.UNISOLATED)) == mockRelation);

                /*
                 * the read-committed relation is also locatable since its in
                 * the global row store but it will not see the indices until
                 * (a) they have been created; and (b) there has been a commit
                 * of the journal.
                 */
                assertNotNull(store.getResourceLocator().locate(namespace,
                        ITx.READ_COMMITTED));

                // a request for the read committed view is not the same
                // instance as the unisolated view.
                assertTrue(((MockRelation) store.getResourceLocator().locate(
                        namespace, ITx.READ_COMMITTED)) != mockRelation);

            }
            
            {

                // a request for the unisolated view shows that the index
                // exists.
                assertNotNull(((MockRelation) store.getResourceLocator()
                        .locate(namespace, ITx.UNISOLATED)).getIndex());

                // a request for the unisolated view gives us the same instance.
                assertTrue(((MockRelation) store.getResourceLocator().locate(
                        namespace, ITx.UNISOLATED)) == mockRelation);

                /*
                 * @todo The read-committed view still does not see the relation
                 * since there has not been a commit yet after the index was
                 * created.
                 */
                if(false) {

                assertNull(((MockRelation) store.getResourceLocator().locate(
                        namespace, ITx.READ_COMMITTED)));
            
                final MockRelation readCommittedView1 = (MockRelation) store
                        .getResourceLocator().locate(namespace,
                                ITx.READ_COMMITTED);

                // same view before a commit.
                assertTrue(readCommittedView1 == (MockRelation) store
                        .getResourceLocator().locate(namespace,
                                ITx.READ_COMMITTED));

                
                // commit
                store.commit();

                /*
                 * should be a new read-committed view
                 * 
                 * FIXME cache must be defeated for read-committed!!! at least
                 * if there HAS been a commit
                 */
                final MockRelation readCommittedView2 = (MockRelation) store
                        .getResourceLocator().locate(namespace,
                                ITx.READ_COMMITTED);
        
                // different view after a commit.
                assertTrue(readCommittedView1 != readCommittedView2);
                
                /*
                 * The index is now visible to the read committed view.
                 */
                assertNull(readCommittedView2.getIndex());
                
                // still not visible to the old view.
                assertNull(readCommittedView1.getIndex());
                
                // another request gives us the same view
                assertTrue(readCommittedView2 == (MockRelation) store
                        .getResourceLocator().locate(namespace,
                                ITx.READ_COMMITTED));
                
                }

            }
            
        } finally {
            
            store.destroy();
            
        }
        
    }

    /**
     * Unit test for property caching for locatable resources.
     */
    public void test_propertyCache() {
        
        final Properties properties = getProperties();
        
        final Journal store = new Journal( properties );

        final String namespace = "test";
        
        try {

            // write a small record onto the journal and force a commit.
            {
                final ByteBuffer b = ByteBuffer.allocate(4);
                b.putInt(0);
                b.flip();
                store.write(b);
                assertNotSame(0L,store.commit());
            }
            
            // verify resource can not be located yet.
            {
                // resource does not exist in UNISOLATED view.
                assertNull(store.getResourceLocator().locate(namespace,
                        ITx.UNISOLATED));

                // resource does not exist at lastCommitTime.
                assertNull(store.getResourceLocator().locate(namespace,
                        store.getLastCommitTime()));

                // resource does not exist at read-only tx.
                {
                    final long tx = store.newTx(ITx.READ_COMMITTED);
                    try {
                        assertNull(store.getResourceLocator().locate(namespace,
                                store.getLastCommitTime()));
                    } finally {
                        store.abort(tx);
                    }
                }
            }

            // instantiate relation.
            MockRelation mockRelation = new MockRelation(store, namespace,
                    ITx.UNISOLATED, properties);

            // verify resource still can not be located.
            {
                // resource does not exist in UNISOLATED view.
                assertNull(store.getResourceLocator().locate(namespace,
                        ITx.UNISOLATED));

                // resource does not exist at lastCommitTime.
                assertNull(store.getResourceLocator().locate(namespace,
                        store.getLastCommitTime()));

                // resource does not exist at read-only tx.
                {
                    final long tx = store.newTx(ITx.READ_COMMITTED);
                    try {
                        assertNull(store.getResourceLocator().locate(namespace,
                                store.getLastCommitTime()));
                    } finally {
                        store.abort(tx);
                    }
                }
            }
            
            // create the resource, which writes the properties into the GRS.
            mockRelation.create();

            /*
             */
            {

                /*
                 * The UNISOLATED view of the resource should be locatable now
                 * since the writes on the global row store are unisolated.
                 */
                assertNotNull(store.getResourceLocator().locate(namespace,
                        ITx.UNISOLATED));

                // a request for the unisolated view gives us the same instance.
                assertTrue(store.getResourceLocator().locate(namespace,
                        ITx.UNISOLATED) == mockRelation);

                /*
                 * The read-committed view of the resource is also locatable.
                 */
                assertNotNull(store.getResourceLocator().locate(namespace,
                        ITx.READ_COMMITTED));

                /*
                 * The read committed view is not the same instance as the
                 * unisolated view.
                 */
                assertTrue(((MockRelation) store.getResourceLocator().locate(
                        namespace, ITx.READ_COMMITTED)) != mockRelation);

            }

            // commit time immediately proceeding this commit.
            final long priorCommitTime = store.getLastCommitTime();
            
            // commit, noting the commit time.
            final long lastCommitTime = store.commit();

            if(log.isInfoEnabled()) {
                log.info("priorCommitTime=" + priorCommitTime);
                log.info("lastCommitTime =" + lastCommitTime);
            }
            
            /*
             * Now create a few transactions against the newly created resource
             * and verify that the views returned for those transactions are
             * distinct, but that they share the same set of default Properties
             * (e.g., the propertyCache is working).
             * 
             * @todo also test a read-historical read !
             */
            final long tx1 = store.newTx(store.getLastCommitTime()); // read-only tx
            final long tx2 = store.newTx(store.getLastCommitTime()); // read-only tx
            final long ts1 = store.getLastCommitTime() - 1; // historical read
            try {

                assertTrue(tx1 != tx2);
                assertTrue(ts1 != tx1);
                assertTrue(ts1 != tx2);

                /*
                 * @todo There might not be enough commit latency to have
                 * lastCommitTime - 1 be GT priorCommitTime. If this happens
                 * either resolve issue 145 or add some latency into the test.
                 * 
                 * @see http://sourceforge.net/apps/trac/bigdata/ticket/145
                 */
                assertTrue(ts1 > priorCommitTime);

                // unisolated view.
                final AbstractResource<?> view_un = (AbstractResource<?>) store
                        .getResourceLocator().locate(namespace, ITx.UNISOLATED);
                assertNotNull(view_un);

                // tx1 view.
                final AbstractResource<?> view_tx1 = (AbstractResource<?>) store
                        .getResourceLocator().locate(namespace, tx1);
                assertNotNull(view_tx1);

                // tx2 view.
                final AbstractResource<?> view_tx2 = (AbstractResource<?>) store
                        .getResourceLocator().locate(namespace, tx2);
                assertNotNull(view_tx2);

                // all views are distinct.
                assertTrue(view_un != view_tx1);
                assertTrue(view_un != view_tx2);
                assertTrue(view_tx1 != view_tx2);

                // each view has its correct timestamp.
                assertEquals(ITx.UNISOLATED, view_un.getTimestamp());
                assertEquals(tx1, view_tx1.getTimestamp());
                assertEquals(tx2, view_tx2.getTimestamp());

                // each view has its own Properties object.
                final Properties p_un = view_un.getProperties();
                final Properties p_tx1 = view_tx1.getProperties();
                final Properties p_tx2 = view_tx2.getProperties();
                assertTrue(p_un != p_tx1);
                assertTrue(p_un != p_tx2);
                assertTrue(p_tx1 != p_tx2);

                /*
                 * Verify that the [propertyCache] is working.
                 * 
                 * Note: Unfortunately, I have not been able to devise any means
                 * of testing the [propertyCache] without exposing that as a
                 * package private object.
                 */
                final DefaultResourceLocator<?> locator = (DefaultResourceLocator<?>) store
                        .getResourceLocator();

                // Not cached for the UNISOLATED view (mutable views can not be
                // cached).
                assertNull(locator.propertyCache.get(new NT(namespace,
                        ITx.UNISOLATED)));

//                if (true) {
//                    final Iterator<Map.Entry<NT, WeakReference<Map<String, Object>>>> itr = locator.propertyCache
//                            .entryIterator();
//                    while (itr.hasNext()) {
//                        final Map.Entry<NT, WeakReference<Map<String, Object>>> e = itr
//                                .next();
//                        System.err.println(e.getKey() + " => "
//                                + e.getValue().get());
//                    }
//                }

                // Not cached for the actual tx ids or read-only timestamp.
                assertNull(locator.propertyCache.get(new NT(namespace,tx1)));
                assertNull(locator.propertyCache.get(new NT(namespace,tx2)));
                assertNull(locator.propertyCache.get(new NT(namespace,ts1)));

                /*
                 * Cached for the last commit time, which should have been used
                 * to hand back the Properties for {tx1, tx2, ts1}.
                 */
                assertNotNull(locator.propertyCache.get(new NT(namespace,
                        lastCommitTime)));
                
                // nothing for the prior commit time.
                assertNull(locator.propertyCache.get(new NT(namespace,
                        priorCommitTime)));

            } finally {
                store.abort(tx1);
                store.abort(tx2);
            }
            
        } finally {
            
            store.destroy();
            
        }

    }
    
    @SuppressWarnings("unchecked")
    private static class MockRelation extends AbstractRelation {

        static final private String indexName = "foo";
        
        private IIndex ndx;
        
        /**
         * @param indexManager
         * @param namespace
         * @param timestamp
         * @param properties
         */
        public MockRelation(IIndexManager indexManager, String namespace, Long timestamp,
                Properties properties) {
            
            super(indexManager, namespace, timestamp, properties);
            
        }

        public void create() {
            
            super.create();
            
            if (getIndex() == null) {

                getIndexManager().registerIndex(
                        new IndexMetadata(getNamespace() + indexName, UUID
                                .randomUUID()));

                log.info("Created index.");

                getIndex();
                
            }
            
        }

        public void destroy() {
            
            super.destroy();
            
            if (getIndex() != null) {

                getIndexManager().dropIndex(getNamespace() + indexName);

                log.info("Dropped index.");
                
            }

        }

        private IIndex getIndex() {

            if (ndx == null) {

                ndx = getIndex(getNamespace() + indexName);

            }

            if (ndx == null) {

                log.info("Index not found.");

            }

            return ndx;

        }
        
        @Override
        public String getFQN(IKeyOrder keyOrder) {
            return null;
        }

        public long delete(IChunkedOrderedIterator itr) {
            return 0;
        }

        public long insert(IChunkedOrderedIterator itr) {
            return 0;
        }

        public Set getIndexNames() {
            return null;
        }

        public IKeyOrder getPrimaryKeyOrder() {
            return null;
        }

        public Iterator getKeyOrders() {
            return null;
        }

        public IKeyOrder getKeyOrder(IPredicate p) {
            return null;
        }
        
        public Object newElement(List a, IBindingSet bindingSet) {
            return null;
        }
    
        public Class<ISPO> getElementClass() {
            return null;
        }

    } // class MockRelation
    
}
