package com.bigdata.rdf.sail;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.counters.CAT;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.util.InnerCause;
import com.bigdata.util.PseudoRandom;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * TestCase to test single writer/mutiple transaction committed readers with
 * SAIL interface.
 * 
 * @author Martyn Cutcher
 * 
 */
public class TestMROWTransactionsNoHistory extends TestMROWTransactions {

	/**
     * 
     */
	public TestMROWTransactionsNoHistory() {
	}

	/**
	 * @param arg0
	 */
	public TestMROWTransactionsNoHistory(String arg0) {
		super(arg0);
	}

    protected void setUp() throws Exception {
        super.setUp();
    }
    
    protected void tearDown() throws Exception {
        super.tearDown();
    }
    
	// similar to test_multiple_transactions but uses direct AbsractTripleStore
	// manipulations rather than RepositoryConnections
	public void test_multiple_csem_transaction_nohistory() throws Exception {
		
//		domultiple_csem_transaction(0);
		
		domultiple_csem_transaction2(0/* retentionMillis */,
				2/* nreaderThreads */, 1000/* nwriters */, 20 * 1000/* nreaders */);

	}

	public void test_multiple_csem_transaction_nohistory_oneReaderThread() throws Exception {

		domultiple_csem_transaction2(0/* retentionMillis */,
				1/* nreaderThreads */, 1000/* nwriters */, 20 * 1000/* nreaders */);

	}
	
	public void test_multiple_csem_transaction_nohistory_stress() throws Exception {

		for (int i = 0; i < 100; i++) {

			domultiple_csem_transaction2(0/* retentionMillis */,
					1/* nreaderThreads */, 10/* nwriters */, 200/* nreaders */);

		}
		
	}
	
	public void notest_stress_multiple_csem_transaction_nohistory() throws Exception {

		final int retentionMillis = 0;
		
		for (int i = 0; i< 50; i++) {
			
			domultiple_csem_transaction2(retentionMillis, 2/* nreaderThreads */,
					1000/* nwriters */, 20 * 1000/* nreaders */);

		}
		
	}
	
	public void test_multiple_csem_transaction_onethread_nohistory() throws Exception {

		domultiple_csem_transaction_onethread(0);
		
	}
	
// Open a read committed transaction
    //do reads
    //do write without closing read
    //commit write
    //close read
    //repeat
	public void notest_multiple_csem_transaction_onethread_nohistory_debug() throws Exception {
		PseudoRandom r = new PseudoRandom(2000);

		for (int run = 0; run < 200; run++) {
			final int uris = 1 + r.nextInt(599);
			final int preds = 1 + r.nextInt(49);
			try {
				System.err.println("Testing with " + uris + " uris, " + preds +  " preds");
				domultiple_csem_transaction_onethread(0, uris, preds);
			} catch (Exception e) {
				System.err.println("problem with " + uris + " uris, " + preds +  " preds");
				throw e;
			}
		}
	}
}
