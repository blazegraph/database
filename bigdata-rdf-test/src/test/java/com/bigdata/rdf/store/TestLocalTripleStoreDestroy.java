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
 * Created on Feb 20, 2012
 */

package com.bigdata.rdf.store;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.lexicon.LexiconKeyOrder;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.RelationSchema;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.SparseRowStore;

/**
 * Test suite to verify the semantics of destroying a {@link LocalTripleStore},
 * including verifying that the indices are deleted (gone from Name2Addr), that
 * the locator is cleared from the {@link DefaultResourceLocator}, and that the
 * entries from the {@link AbstractTripleStore} are removed from the global row
 * store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLocalTripleStoreDestroy extends ProxyTestCase {

    /**
     * 
     */
    public TestLocalTripleStoreDestroy() {
    }

    /**
     * @param name
     */
    public TestLocalTripleStoreDestroy(String name) {
        super(name);
    }

    /**
     * Verify that a newly created {@link AbstractTripleStore} which has never
     * been committed may be destroyed, that the entries in the global row store
     * are removed, the resource locator cache is cleared, and the indices for
     * the triple store are no longer discoverable using the
     * {@link IIndexManager}.
     */
    public void test_destroyNoCommit() {

        final String namespace = "kb";
        final String namespaceLexiconRelation = namespace + ".lex";
        final String namespaceSPORelation = namespace + ".spo";
        final String lexiconRelationIndexName = namespaceLexiconRelation + "."
                + LexiconKeyOrder.TERM2ID.getIndexName();
        final String primaryStatementIndexName = namespaceSPORelation + "."
                + SPOKeyOrder.SPO.getIndexName();
        
        final Properties properties = new Properties();
        properties.setProperty(com.bigdata.journal.Options.CREATE_TEMP_FILE, "true");
        properties.setProperty(AbstractTripleStore.Options.TRIPLES_MODE,"true");
        
        final AbstractTripleStore store = getStore(properties);

        final IIndexManager indexManager = store.getIndexManager();

        try {

            final long lastCommitTime = store.getIndexManager().getLastCommitTime();
            
            // Note: Will be in lexical order for Unicode.
            assertEquals(
                    new String[] { namespace },
                    getNamespaces(indexManager, ITx.UNISOLATED).toArray(
                            new String[] {}));
            // Note found before the create.
            assertEquals(
                    new String[] {},
                    getNamespaces(indexManager, lastCommitTime - 1).toArray(
                            new String[] {}));

            assertTrue(store == indexManager.getResourceLocator().locate(
                    store.getNamespace(), ITx.UNISOLATED));
            assertTrue(store.getLexiconRelation() == indexManager
                    .getResourceLocator().locate(namespaceLexiconRelation,
                            ITx.UNISOLATED));
            assertTrue(store.getSPORelation() == indexManager
                    .getResourceLocator().locate(namespaceSPORelation,
                            ITx.UNISOLATED));
            assertNotNull(indexManager.getIndex(lexiconRelationIndexName,
                    ITx.UNISOLATED));
            assertNotNull(indexManager.getIndex(primaryStatementIndexName,
                    ITx.UNISOLATED));

            /*
             * Destroy the triple store.
             */
            store.destroy();

            // Did not go through a commit on the LTS.
            assertEquals(lastCommitTime, store.getIndexManager()
                    .getLastCommitTime());

            // global row store entry is gone.
            assertTrue(getNamespaces(indexManager, ITx.UNISOLATED).isEmpty());

            // but not in the last commited view.
            assertFalse(getNamespaces(indexManager, lastCommitTime).isEmpty());
            
            // resources can not be located.
            assertTrue(null == indexManager.getResourceLocator().locate(
                    namespace, ITx.UNISOLATED));
            assertTrue(null == indexManager.getResourceLocator().locate(
                    namespaceLexiconRelation, ITx.UNISOLATED));
            assertTrue(null == indexManager.getResourceLocator().locate(
                    namespaceSPORelation, ITx.UNISOLATED));

            // indicies are gone.
            assertNull(indexManager.getIndex(lexiconRelationIndexName,
                    ITx.UNISOLATED));
            assertNull(indexManager.getIndex(primaryStatementIndexName,
                    ITx.UNISOLATED));
            // but not at the last commit time.
            assertNotNull(indexManager.getIndex(primaryStatementIndexName,
                    lastCommitTime));

            /*
             * Commit.
             */
            store.commit();
            
            // No longer present at the last commit time.
            assertTrue(getNamespaces(indexManager,
                    store.getIndexManager().getLastCommitTime()).isEmpty());
            
        } finally {

            indexManager.destroy();

        }
        
    }

    /**
     * Verify that a newly created and committed {@link AbstractTripleStore} may
     * be destroyed, that the entries in the global row store are removed, the
     * resource locator cache is cleared, and the indices for the triple store
     * are no longer discoverable using the {@link IIndexManager}. The committed
     * view of the triple store and indices should remain visible and locatable.
     */
    public void test_destroyOne() {

        final String namespace = "kb";
        final String namespaceLexiconRelation = namespace + ".lex";
        final String namespaceSPORelation = namespace + ".spo";
        final String lexiconRelationIndexName = namespaceLexiconRelation + "."
                + LexiconKeyOrder.TERM2ID.getIndexName();
        final String primaryStatementIndexName = namespaceSPORelation + "."
                + SPOKeyOrder.SPO.getIndexName();
        
        final Properties properties = new Properties();
        properties.setProperty(com.bigdata.journal.Options.CREATE_TEMP_FILE, "true");
        properties.setProperty(AbstractTripleStore.Options.BUFFER_MODE,BufferMode.DiskWORM.toString());
        properties.setProperty(AbstractTripleStore.Options.TRIPLES_MODE,"true");
        
        final AbstractTripleStore store = getStore(properties);

        final IIndexManager indexManager = store.getIndexManager();

        try {

            // make the tripleStore dirty so commit() will do something.
            store.addTerm(store.getValueFactory().createLiteral("bigdata"));
            
            // Note: Will be in lexical order for Unicode.
            final String[] namespaces = getNamespaces(indexManager,
                    ITx.UNISOLATED).toArray(new String[] {});

            assertEquals(new String[] { namespace }, namespaces);

            assertTrue(store == indexManager.getResourceLocator().locate(
                    store.getNamespace(), ITx.UNISOLATED));
            assertTrue(store.getLexiconRelation() == indexManager
                    .getResourceLocator().locate(namespaceLexiconRelation,
                            ITx.UNISOLATED));
            assertTrue(store.getSPORelation() == indexManager
                    .getResourceLocator().locate(namespaceSPORelation,
                            ITx.UNISOLATED));
            assertNotNull(indexManager.getIndex(lexiconRelationIndexName,
                    ITx.UNISOLATED));
            assertNotNull(indexManager.getIndex(primaryStatementIndexName,
                    ITx.UNISOLATED));

            final long commitTime = store.commit();
            assertTrue(commitTime > 0);
            
            /*
             * Destroy the triple store.
             */
            store.destroy();

            // global row store entry is gone.
            assertTrue(getNamespaces(indexManager,ITx.UNISOLATED).isEmpty());

            // resources can not be located.
            assertTrue(null == indexManager.getResourceLocator().locate(
                    namespace, ITx.UNISOLATED));
            assertTrue(null == indexManager.getResourceLocator().locate(
                    namespaceLexiconRelation, ITx.UNISOLATED));
            assertTrue(null == indexManager.getResourceLocator().locate(
                    namespaceSPORelation, ITx.UNISOLATED));

            // indicies are gone.
            assertNull(indexManager.getIndex(lexiconRelationIndexName,
                    ITx.UNISOLATED));
            assertNull(indexManager.getIndex(primaryStatementIndexName,
                    ITx.UNISOLATED));
            
            // The committed version of the triple store remains visible.
            assertNotNull(indexManager.getResourceLocator().locate(namespace,
                    commitTime-1));
            
            /*
             * Commit the destroy.
             */
            store.commit();
            

            // global row store entry is gone.
            assertTrue(getNamespaces(indexManager,ITx.UNISOLATED).isEmpty());

            // resources can not be located.
            assertTrue(null == indexManager.getResourceLocator().locate(
                    namespace, ITx.UNISOLATED));
            assertTrue(null == indexManager.getResourceLocator().locate(
                    namespaceLexiconRelation, ITx.UNISOLATED));
            assertTrue(null == indexManager.getResourceLocator().locate(
                    namespaceSPORelation, ITx.UNISOLATED));

            // indicies are gone.
            assertNull(indexManager.getIndex(lexiconRelationIndexName,
                    ITx.UNISOLATED));
            assertNull(indexManager.getIndex(primaryStatementIndexName,
                    ITx.UNISOLATED));
            
            // The committed version of the triple store remains visible.
            assertNotNull(indexManager.getResourceLocator().locate(namespace,
                    commitTime-1));
        } finally {

            indexManager.destroy();

        }
        
    }

    /**
     * Return a list of the namespaces for the {@link AbstractTripleStore}s
     * registered against the bigdata instance.
     */
    static private List<String> getNamespaces(final IIndexManager indexManager,
            final long timestamp) {
    
        // the triple store namespaces.
        final List<String> namespaces = new LinkedList<String>();

        final SparseRowStore grs = indexManager.getGlobalRowStore(timestamp);

        if (grs == null) {

            return namespaces;

        }
        
        // scan the relation schema in the global row store.
        @SuppressWarnings("unchecked")
        final Iterator<ITPS> itr = (Iterator<ITPS>) grs
                .rangeIterator(RelationSchema.INSTANCE);

        while (itr.hasNext()) {

            // A timestamped property value set is a logical row with
            // timestamped property values.
            final ITPS tps = itr.next();

            // If you want to see what is in the TPS, uncomment this.
//          System.err.println(tps.toString());
            
            // The namespace is the primary key of the logical row for the
            // relation schema.
            final String namespace = (String) tps.getPrimaryKey();

            // Get the name of the implementation class
            // (AbstractTripleStore, SPORelation, LexiconRelation, etc.)
            final String className = (String) tps.get(RelationSchema.CLASS)
                    .getValue();
            
            if (className == null) {
                // Skip deleted triple store entry.
                continue;
            }

            try {
                final Class<?> cls = Class.forName(className);
                if (AbstractTripleStore.class.isAssignableFrom(cls)) {
                    // this is a triple store (vs something else).
                    namespaces.add(namespace);
                }
            } catch (ClassNotFoundException e) {
                log.error(e,e);
            }

        }

        return namespaces;

    }
    
    /**
     * Verify the namespace prefix for the triple store is imposed correctly in
     * {@link AbstractResource#destroy()}. Create two KBs such that the
     * namespace for one instance is a prefix of the namespace for the other
     * instance, e.g.,
     * 
     * <pre>
     * kb
     * kb1
     * </pre>
     * 
     * Verify that destroying <code>kb</code> does not cause the indices for
     * <code>kb1</code> to be destroyed.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/743">
     *      AbstractTripleStore.destroy() does not filter for correct prefix
     *      </a>
     */
    public void test_destroyTwo() {

        final String namespace = "kb";
        final String namespaceLexiconRelation = namespace + ".lex";
        final String namespaceSPORelation = namespace + ".spo";
        final String lexiconRelationIndexName = namespaceLexiconRelation + "."
                + LexiconKeyOrder.TERM2ID.getIndexName();
        final String primaryStatementIndexName = namespaceSPORelation + "."
                + SPOKeyOrder.SPO.getIndexName();

        final String namespace1 = "kb1";
        final String namespaceLexiconRelation1 = namespace1 + ".lex";
        final String namespaceSPORelation1 = namespace1 + ".spo";
        final String lexiconRelationIndexName1 = namespaceLexiconRelation1
                + "." + LexiconKeyOrder.TERM2ID.getIndexName();
        final String primaryStatementIndexName1 = namespaceSPORelation1 + "."
                + SPOKeyOrder.SPO.getIndexName();
        
        final Properties properties = new Properties();
        properties.setProperty(com.bigdata.journal.Options.CREATE_TEMP_FILE, "true");
        properties.setProperty(AbstractTripleStore.Options.BUFFER_MODE,BufferMode.DiskWORM.toString());
        properties.setProperty(AbstractTripleStore.Options.TRIPLES_MODE,"true");
        
        final AbstractTripleStore kb = getStore(properties);

        final IIndexManager indexManager = kb.getIndexManager();

        try {

            assertEquals(namespace, kb.getNamespace());

            final AbstractTripleStore kb1 = new LocalTripleStore(indexManager,
                    namespace1, ITx.UNISOLATED, properties);
            kb1.create();

            // make the tripleStore dirty so commit() will do something.
            kb.addTerm(kb.getValueFactory().createLiteral("bigdata"));
            kb1.addTerm(kb.getValueFactory().createLiteral("bigdata"));

            // Verify post-conditions of the created KBs.
            {

                /*
                 * Verify that both triple store declarations exist in the GRS.
                 * 
                 * Note: Will be in lexical order for Unicode.
                 */
                final String[] namespaces = getNamespaces(indexManager,ITx.UNISOLATED)
                        .toArray(new String[] {});
                assertEquals(new String[] { namespace, namespace1 }, namespaces);

                /*
                 * Verify that the unislolated versions of each triple stores is
                 * the same reference that we obtained above when that triple
                 * store was created.
                 */
                assertTrue(kb == indexManager.getResourceLocator().locate(
                        kb.getNamespace(), ITx.UNISOLATED));
                assertTrue(kb1 == indexManager.getResourceLocator().locate(
                        kb1.getNamespace(), ITx.UNISOLATED));

                /* Verify lexicon relations exist. */
                assertTrue(kb.getLexiconRelation() == indexManager
                        .getResourceLocator().locate(namespaceLexiconRelation,
                                ITx.UNISOLATED));
                assertTrue(kb1.getLexiconRelation() == indexManager
                        .getResourceLocator().locate(namespaceLexiconRelation1,
                                ITx.UNISOLATED));

                /* Verify SPO relations exist. */
                assertTrue(kb.getSPORelation() == indexManager
                        .getResourceLocator().locate(namespaceSPORelation,
                                ITx.UNISOLATED));
                assertTrue(kb1.getSPORelation() == indexManager
                        .getResourceLocator().locate(namespaceSPORelation1,
                                ITx.UNISOLATED));

                /* Verify lexicon index exists. */
                assertNotNull(indexManager.getIndex(lexiconRelationIndexName,
                        ITx.UNISOLATED));
                assertNotNull(indexManager.getIndex(lexiconRelationIndexName1,
                        ITx.UNISOLATED));

                /* Verify primary SPO index exists. */
                assertNotNull(indexManager.getIndex(primaryStatementIndexName,
                        ITx.UNISOLATED));
                assertNotNull(indexManager.getIndex(primaryStatementIndexName1,
                        ITx.UNISOLATED));

            }

            /* Commit. */
            final long commitTime = kb.commit();
            assertTrue(commitTime > 0);
            
            /*
             * Destroy the triple store whose namespace is a prefix of the 2nd
             * triple store namespace.
             */
            {
                kb.destroy();

                // global row store entry is gone.
                final String[] namespaces = getNamespaces(indexManager,ITx.UNISOLATED).toArray(
                        new String[] {});
                assertEquals(new String[] { namespace1 }, namespaces);

                // resources can not be located.
                assertTrue(null == indexManager.getResourceLocator().locate(
                        namespace, ITx.UNISOLATED));
                assertTrue(null == indexManager.getResourceLocator().locate(
                        namespaceLexiconRelation, ITx.UNISOLATED));
                assertTrue(null == indexManager.getResourceLocator().locate(
                        namespaceSPORelation, ITx.UNISOLATED));

                // indicies are gone.
                assertNull(indexManager.getIndex(lexiconRelationIndexName,
                        ITx.UNISOLATED));
                assertNull(indexManager.getIndex(primaryStatementIndexName,
                        ITx.UNISOLATED));

                // The committed version of the triple store remains visible.
                assertNotNull(indexManager.getResourceLocator().locate(
                        namespace, commitTime - 1));
            }
            
            /*
             * Verify that the other kb still exists, including its GRS
             * declaration and its indices.
             */
            {

                /*
                 * Verify that the triple store declaration exists in the GRS.
                 * 
                 * Note: Will be in lexical order for Unicode.
                 */
                final String[] namespaces = getNamespaces(indexManager,ITx.UNISOLATED).toArray(
                        new String[] {});
                assertEquals(new String[] { namespace1 }, namespaces);

                /*
                 * Verify that the unislolated versions of each triple stores is the
                 * same reference that we obtained above when that triple store was
                 * created.
                 */
                assertTrue(kb1 == indexManager.getResourceLocator().locate(
                        kb1.getNamespace(), ITx.UNISOLATED));

                /* Verify lexicon relations exist. */
                assertTrue(kb1.getLexiconRelation() == indexManager
                        .getResourceLocator().locate(namespaceLexiconRelation1,
                                ITx.UNISOLATED));

                /* Verify SPO relations exist. */
                assertTrue(kb1.getSPORelation() == indexManager
                        .getResourceLocator().locate(namespaceSPORelation1,
                                ITx.UNISOLATED));
                
                /* Verify lexicon index exists. */
                assertNotNull(indexManager.getIndex(lexiconRelationIndexName1,
                        ITx.UNISOLATED));
                
                /* Verify primary SPO index exists. */
                assertNotNull(indexManager.getIndex(primaryStatementIndexName1,
                        ITx.UNISOLATED));
                
            }
            
            /*
             * Destroy the other triple store.
             */
            {
                kb1.destroy();

                // global row store entry is gone.
                assertTrue(getNamespaces(indexManager,ITx.UNISOLATED).isEmpty());

                // resources can not be located.
                assertTrue(null == indexManager.getResourceLocator().locate(
                        namespace1, ITx.UNISOLATED));
                assertTrue(null == indexManager.getResourceLocator().locate(
                        namespaceLexiconRelation1, ITx.UNISOLATED));
                assertTrue(null == indexManager.getResourceLocator().locate(
                        namespaceSPORelation1, ITx.UNISOLATED));

                // indicies are gone.
                assertNull(indexManager.getIndex(lexiconRelationIndexName1,
                        ITx.UNISOLATED));
                assertNull(indexManager.getIndex(primaryStatementIndexName1,
                        ITx.UNISOLATED));

                // The committed version of the triple store remains visible.
                assertNotNull(indexManager.getResourceLocator().locate(
                        namespace1, commitTime - 1));
            }

        } finally {

            indexManager.destroy();

        }
        
    }

}
