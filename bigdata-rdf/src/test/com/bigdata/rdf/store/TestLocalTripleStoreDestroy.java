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
import com.bigdata.relation.RelationSchema;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.sparse.ITPS;

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

            // Note: Will be in lexical order for Unicode.
            final String[] namespaces = getNamespaces(indexManager).toArray(
                    new String[] {});

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

            /*
             * Destroy the triple store.
             */
            store.destroy();

            // global row store entry is gone.
            assertTrue(getNamespaces(indexManager).isEmpty());

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
            final String[] namespaces = getNamespaces(indexManager).toArray(
                    new String[] {});

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
            assertTrue(getNamespaces(indexManager).isEmpty());

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
    static private List<String> getNamespaces(final IIndexManager indexManager) {
    
        // the triple store namespaces.
        final List<String> namespaces = new LinkedList<String>();

        // scan the relation schema in the global row store.
        @SuppressWarnings("unchecked")
        final Iterator<ITPS> itr = (Iterator<ITPS>) indexManager
                .getGlobalRowStore().rangeIterator(RelationSchema.INSTANCE);

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
    
}
