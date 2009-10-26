package com.bigdata.rdf.store;

import java.io.IOException;
import java.util.Properties;

import org.openrdf.model.Statement;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test suite for the transaction semantics of the {@link LocalTripleStore}.
 * 
 * FIXME This test suite should be used for any backend as long as full tx are
 * supported on that backend. So, LTS, LDS and eventually EDS and JDS.
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

        final LocalTripleStore store = (LocalTripleStore) getStore();

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

            if(log.isInfoEnabled()) log.info("stmtInStore: " + stmtInStore);

            final boolean stmtInView = view.hasStatement(s, p, o);

            if(log.isInfoEnabled()) log.info("stmtInView: " + stmtInView);

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

            store.__tearDownUnitTest();

        }
        
    }
    
    /**
     * Test of abort semantics.
     */
    public void test_abort() {

        class AbortException extends RuntimeException {
            private static final long serialVersionUID = 1L;
        }

        final LocalTripleStore store = (LocalTripleStore) getStore();

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

            store.__tearDownUnitTest();

        }

    }

    /**
     * A unit test for some of the basic functionality for a triple store or
     * quad store using full transactions.
     * 
     * @todo test ability to obtain read-only tx for specific historical commit
     *       points and query the kb.
     * 
     * @todo test of isolatation of the kb create within the tx. should not be
     *       visible until the commit in read-only, unisolated, or other tx. a
     *       concurrent create should fail in validation, not before.
     * 
     * @todo test concurrent tx create of kb.
     * 
     * @todo test add/add and retract/retract conflict resolution for concurrent
     *       tx.
     * 
     * @todo test add/get terms from within tx and unisolated views.
     * 
     * @todo propagate the min/max tuple revision timestamp into the B+Tree
     *       nodes and support fast deltas between commits based on those
     *       revision timestamps.
     * 
     * @throws IOException
     */
    public void test_txIsolation() throws IOException {

        final Properties p = new Properties(getProperties());
        
        /*
         * Turn off inference and vocabulary so the initial kb view is empty.
         */
        p.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class
                .getName());
        
        p.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
                NoVocabulary.class.getName());

        // an unisolated view of the kb instance.
        final AbstractTripleStore initialKb = getStore(p);

        try {

            /*
             * Use a different namespace for the KB initial that we will test
             * against since we need to create it from within a transaction.
             */
            final String namespace = initialKb.getNamespace()+"_test";

            final IIndexManager indexManager = initialKb.getIndexManager();

            // @todo no way to get the txService here w/o a cast?
            final ITransactionService txService = ((Journal) indexManager)
                    .getTransactionManager().getTransactionService();

            // verify kb does not exist with read-historical tx.
            {

                // a tx reading from the most recent commit point on the db.
                final long tx0 = txService.newTx(ITx.READ_COMMITTED);

                // verify kb does not exist (can not be located).
                assertNull(indexManager.getResourceLocator().locate(namespace,
                        tx0));

                txService.abort(tx0);

            }

            // create the kb from within a tx.
            {

                final long txCreate = txService.newTx(ITx.UNISOLATED);

                // verify kb does not exist (can not be located).
                assertNull(indexManager.getResourceLocator().locate(namespace,
                        txCreate));

                // alternative ctor for unlocated kb instance : @todo
                // parameterize for LTS vs ScaleOut
                final AbstractTripleStore txCreateView = new LocalTripleStore(
                        indexManager, namespace, Long.valueOf(txCreate), p);

                // create the kb instance within the tx.
                txCreateView.create();

                // commit the tx.
                txService.commit(txCreate);

            }

            /*
             * Note: the lexicon is non-transactional. The URIs are defined here
             * for ease of reuse across the various code blocks below.
             */
            final AbstractTripleStore unisolatedStore = ((AbstractTripleStore) indexManager
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED));
            
            final BigdataValueFactory f = unisolatedStore.getValueFactory();

            final BigdataURI john = f.createURI("http://www.bigdata.com/john");
            final BigdataURI loves = f
                    .createURI("http://www.bigdata.com/loves");
            final BigdataURI mary = f.createURI("http://www.bigdata.com/mary");

            final BigdataValue[] terms = new BigdataValue[] {

            john, loves, mary

            };

            // add terms to the lexicon, causing their term identifiers to be
            // defined.
            unisolatedStore.getLexiconRelation()
                    .addTerms(terms, terms.length, false/* readOnly */);
            
            {
                
                // a tx reading from the most recent commit point on the db.
                final long tx1 = txService.newTx(ITx.READ_COMMITTED);

                final AbstractTripleStore tx1View = (AbstractTripleStore) indexManager
                        .getResourceLocator().locate(namespace, tx1);

                // no explicit statements in the kb.
                assertEquals(0L, tx1View.getExplicitStatementCount(null/* c */));

                // a read/write tx.
                final long tx2 = txService.newTx(ITx.UNISOLATED);

                // another read/write tx.
                final long tx3 = txService.newTx(ITx.UNISOLATED);
                
                final AbstractTripleStore tx2View = (AbstractTripleStore) indexManager
                        .getResourceLocator().locate(namespace, tx2);

                final AbstractTripleStore tx3View = (AbstractTripleStore) indexManager
                        .getResourceLocator().locate(namespace, tx3);

                {
                    // prepare the write set on [tx2].
                    final StatementBuffer<Statement> sb = new StatementBuffer<Statement>(
                            tx2View, 10);

                    // add to the tx write set.
                    sb.add(john, loves, mary);
                    sb.flush();

                }
                
                // not visible in the read-only tx.
                assertFalse(tx1View.hasStatement(john, loves, mary));

                // not visible in the other read-write tx.
                assertFalse(tx3View.hasStatement(john, loves, mary));
                
                // but it is visible in this tx.
                assertTrue(tx2View.hasStatement(john, loves, mary));

                // commit this tx.
                txService.commit(tx2);

                // still not visible in the read-only tx.
                assertFalse(tx1View.hasStatement(john, loves, mary));

                // still not visible in the other read-write tx.
                assertFalse(tx3View.hasStatement(john, loves, mary));

                // a new tx reading from the most recent commit point on the db.
                final long tx4 = txService.newTx(ITx.READ_COMMITTED);

                final AbstractTripleStore tx4View = (AbstractTripleStore) indexManager
                        .getResourceLocator().locate(namespace, tx4);

                // a new tx reading from the most recent commit point on the db.
                final long tx5 = txService.newTx(ITx.READ_COMMITTED);

                final AbstractTripleStore tx5View = (AbstractTripleStore) indexManager
                        .getResourceLocator().locate(namespace, tx5);

                // visible in the new read-only tx.
                assertTrue(tx4View.hasStatement(john, loves, mary));
                
                // visible in the new read-write tx.
                assertTrue(tx5View.hasStatement(john, loves, mary));

                /*
                 * Now add the same statement in tx3.  We should be able to
                 * reconcile the "write-write" conflict and commit this tx
                 * without trouble.
                 */
                {
                    // prepare the write set.
                    final StatementBuffer<Statement> sb = new StatementBuffer<Statement>(
                            tx3View, 10);

                    // add to the tx write set.
                    sb.add(john, loves, mary);
                    sb.flush();
                }
                txService.commit(tx3);

                // close the other tx.
                txService.abort(tx1);
                txService.abort(tx4);
                txService.abort(tx5);

            }
            
        } finally {

            initialKb.__tearDownUnitTest();

        }

    }

}
