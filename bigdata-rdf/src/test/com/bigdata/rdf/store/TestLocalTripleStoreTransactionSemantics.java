package com.bigdata.rdf.store;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;

/**
 * Test suite for the transaction semantics of the {@link LocalTripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLocalTripleStoreTransactionSemantics extends ProxyTestCase {

    public TestLocalTripleStoreTransactionSemantics() {
        
    }
    
    public TestLocalTripleStoreTransactionSemantics(String name) {

        super(name);

    }

    /**
     * Test the commit semantics in the context of a read-committed view of the
     * database.
     */
    public void test_commit() {

        LocalTripleStore store = (LocalTripleStore) getStore();

        try {

            // read-committed view of the same database.
            final AbstractTripleStore view = store.asReadCommittedView();

            final long s = 1, p = 2, o = 3;

            // add the statement.
            store.addStatements(new SPO[] { //
                    new SPO(s, p, o, StatementEnum.Explicit) //
                    },//
                    1);

            final boolean stmtInStore = store.hasStatement(s, p, o);

            log.info("stmtInStore: " + stmtInStore);

            final boolean stmtInView = view.hasStatement(s, p, o);

            log.info("stmtInView: " + stmtInView);

            // visible in the repo.
            assertTrue(stmtInStore);

            // not visible in the view.
            assertFalse(stmtInView);

            // commit the transaction.
            store.commit();

            // now visible in the view
            /*
             * Note: this will fail if the Journal#getIndex(name,timestamp) does
             * not return an index view with read-committed (vs read-consistent)
             * semantics. For the index view to have read-committed semantics
             * the view MUST update if there is an intervening commit. This is
             * currently handled by returning a ReadCommittedView for this case
             * rather than a BTree.
             */
            assertTrue(view.hasStatement(s, p, o));
            
        } finally {

            store.closeAndDelete();

        }
        
    }
    
    /**
     * Test of abort semantics.
     */
    public void test_abort() {

        class AbortException extends RuntimeException {
            private static final long serialVersionUID = 1L;
        }

        LocalTripleStore store = (LocalTripleStore) getStore();

        final long s = 1, p = 2, o = 3;

        try {

            // add the statement.
            store.addStatements(new SPO[] { //
                    new SPO(s, p, o, StatementEnum.Explicit) //
                    },//
                    1);

            // visible in the repo.
            assertTrue(store.hasStatement(s, p, o));

            throw new AbortException();

        } catch (AbortException ex) {

            // discard the write set.
            store.abort();

            // no longer visible in the repo.
            assertFalse(store.hasStatement(s, p, o));

        } catch (Throwable t) {

            log.error(t);

            // discard the write set.
            store.abort();

            fail("Unexpected exception: " + t, t);

        } finally {

            store.closeAndDelete();

        }

    }

}