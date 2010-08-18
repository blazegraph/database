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

import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.concurrent.DaemonThreadFactory;

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

        final ExecutorService executorService = Executors.newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());
        
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
            
            executorService.shutdownNow();
            
        }
        
    }

    private static class MockRelation extends AbstractRelation {

        final private String indexName = "foo";
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
            // TODO Auto-generated method stub
            return null;
        }

        public long delete(IChunkedOrderedIterator itr) {
            // TODO Auto-generated method stub
            return 0;
        }

        public long insert(IChunkedOrderedIterator itr) {
            // TODO Auto-generated method stub
            return 0;
        }

        public IAccessPath getAccessPath(IPredicate predicate) {
            // TODO Auto-generated method stub
            return null;
        }

        public long getElementCount(boolean exact) {
            // TODO Auto-generated method stub
            return 0;
        }

        public Set getIndexNames() {
            // TODO Auto-generated method stub
            return null;
        }

        public Object newElement(IPredicate predicate, IBindingSet bindingSet) {
            // TODO Auto-generated method stub
            return null;
        }
        
        public Class<ISPO> getElementClass() {

            return null;

        }
    
    }
    
}
