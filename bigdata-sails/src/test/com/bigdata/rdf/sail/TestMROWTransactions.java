package com.bigdata.rdf.sail;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.btree.IndexMetadata;
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
import com.bigdata.util.concurrent.DaemonThreadFactory;

abstract public class TestMROWTransactions extends ProxyBigdataSailTestCase {

    private static final Logger txLog = Logger.getLogger("com.bigdata.txLog");
	
    TestMROWTransactions() {
	}

    TestMROWTransactions(String arg0) {
		super(arg0);
	}

	void domultiple_csem_transaction_onethread(final int retentionMillis) throws Exception {
    	
		domultiple_csem_transaction_onethread(retentionMillis, 2000, 50);
		
    }
    
	void domultiple_csem_transaction(final int retentionMillis) throws Exception {
		
		domultiple_csem_transaction2(retentionMillis, 2/* nreaderThreads */,
				1000/* nwriters */, 20 * 1000/* nreaders */);
		
	}
	
	/**
	 * 
	 * @param retentionMillis
	 *            The retention time (milliseconds).
	 * @param nreaderThreads
	 *            The #of threads running reader tasks. Increase nreaderThreads
	 *            to increase chance startup condition and decrement to increase
	 *            chance of commit point with no open read-only transaction (no
	 *            sessions). Value is in [1:...].
	 * @param nwriters
	 *            The #of writer tasks (there is only one writer thread).
	 * @param nreaders
	 *            The #of reader tasks.
	 * 
	 * @throws Exception
	 */
	void domultiple_csem_transaction2(final int retentionMillis,
			final int nreaderThreads, final int nwriters, final int nreaders)
			throws Exception {

		/**
		 *  The most likely problem is related to the session protection in the
		 *  RWStore.  In development we saw problems when concurrent transactions
		 *  had reduced the open/active transactions to zero, therefore releasing
		 *  session protection.  If the protocol works correctly we should never
		 *  release session protection if any transaction has been initialized.
		 *  
		 *  The message of "invalid address" would be generated if an allocation
		 *  has been freed and is no longer protected from recycling when an
		 *  attempt is made to read from it.
		 *  
		 *  TODO Experiment with different values of [nthreads] for the with and
		 *  w/o history variations of this test.  Consider lifting that parameter
		 *  into the signature of this method.
		 */
		final int nuris = 2000; // number of unique subject/objects
		final int npreds = 50; // 
//		final PseudoRandom r = new PseudoRandom(2000);
//		r.next(1500);
		final Random r = new Random();

		final CAT writes = new CAT();
		final CAT reads = new CAT();
		final AtomicReference<Throwable> failex = new AtomicReference<Throwable>(null);
		// Set [true] iff there are no failures by the time we cancel the running tasks.
		final AtomicBoolean success = new AtomicBoolean(false);
        final BigdataSail sail = getSail(getProperties(retentionMillis));
		try {

	        sail.initialize();
	        final BigdataSailRepository repo = new BigdataSailRepository(sail);
	        final AbstractTripleStore origStore = repo.getDatabase();

	        final URI[] subs = new URI[nuris];
	        for (int i = 0; i < nuris; i++) {
	            subs[i] = uri("uri:" + i);
	        }
	        final URI[] preds = new URI[npreds];
	        for (int i = 0; i < npreds; i++) {
	            preds[i] = uri("pred:" + i);
	        }

	        // Writer task adds nwrites statements then commits
			class Writer implements Callable<Long> {
				final int nwrites;

				Writer(final int nwrites) {
					this.nwrites = nwrites;
				}

				public Long call() throws Exception {
                    try {
                        final boolean isQuads = origStore.isQuads();
                        // Thread.sleep(r.nextInt(2000) + 500);
                        try {

                            for (int i = 0; i < nwrites; i++) {
                                origStore
                                        .addStatement(
                                                subs[r.nextInt(nuris)],
                                                preds[r.nextInt(npreds)],
                                                subs[r.nextInt(nuris)],
                                                isQuads ? subs[r.nextInt(nuris)]
                                                        : null);
                                writes.increment();
//                                System.out.print('.');
                            }
//                            System.out.println("\n");

                        } finally {
                            origStore.commit();
                            if(log.isInfoEnabled()) {
                                log.info("Commit");
                            }
                        }
                    } catch (Throwable ise) {
                        if (!InnerCause.isInnerCause(ise,
                                InterruptedException.class)) {
                            if (failex
                                    .compareAndSet(null/* expected */, ise/* newValue */)) {
                                log.error("firstCause:" + ise, ise);
                            } else {
                                if (log.isInfoEnabled())
                                    log.info("Other error: " + ise, ise);
                            }
                        } else {
                            // Ignore.
                        }
                    }
                    return null;
                }

			}

			// ReaderTask makes nreads and closes
			class Reader implements Callable<Long> {
				final int nreads;

				Reader(final int nwrites) {
					this.nreads = nwrites;
				}

                public Long call() throws Exception {
                    try {
                        final Long txId = ((Journal) origStore
                                .getIndexManager()).newTx(ITx.READ_COMMITTED);

                        try {
                        	txLog.info("Reading with tx: " + txId);
                        	
                            final AbstractTripleStore readstore = (AbstractTripleStore) origStore
                                    .getIndexManager().getResourceLocator()
                                    .locate(origStore.getNamespace(), txId);

                            for (int i = 0; i < nreads; i++) {
                                final BigdataStatementIterator stats = readstore
                                        .getStatements(subs[r.nextInt(nuris)],
                                                null, null);
                                try {
	                                while (stats.hasNext()) {
	                                    stats.next();
	                                    reads.increment();
	                                }
                                } finally {
                                	stats.close();
                                }
                            }

                        	txLog.info("Finished with tx: " + txId);
                        } catch (IllegalStateException ise) {
                        	txLog.info("IllegalStateException tx: " + txId);
                        	failex.compareAndSet(null, ise);
                        } catch (Exception e) {
                        	txLog.info("UnexpectedException tx: " + txId);
                        	failex.compareAndSet(null, e);
                        	throw e;
                        } finally {
                        	txLog.info("Aborting tx: " + txId);
                            ((Journal) origStore.getIndexManager()).abort(txId);
                        }
                    } catch (Throwable ise) {
                        if (!InnerCause.isInnerCause(ise,
                                InterruptedException.class)) {
                            if (failex
                                    .compareAndSet(null/* expected */, ise/* newValue */)) {
                                log.error("firstCause:" + ise, ise);
                            } else {
                                if (log.isInfoEnabled())
                                    log.info("Other error: " + ise, ise);
                            }
                        } else {
                            // Ignore.
                        }
                    }
                    return null;
                }

            }

            ExecutorService writers = null;
            ExecutorService readers = null;
            try {
                
                writers = Executors.newSingleThreadExecutor(DaemonThreadFactory
                        .defaultThreadFactory());

                readers = Executors.newFixedThreadPool(nreaderThreads,
                        DaemonThreadFactory.defaultThreadFactory());

                // let's schedule a few writers and readers (more than needed)
                // writers.submit(new Writer(5000000/* nwrite */));
				Future<Long> lastWriterFuture = null;
				Future<Long> lastReaderFuture = null;
				for (int i = 0; i < 1000; i++) {
					lastWriterFuture = writers
							.submit(new Writer(500/* nwrite */));
                    for (int rdrs = 0; rdrs < 20; rdrs++) {
						lastReaderFuture = readers
								.submit(new Reader(60/* nread */));
                    }
                }

				// let the writers run riot for a time, checking for failure
				while (failex.get() == null
						&& !(lastWriterFuture.isDone() || lastReaderFuture
								.isDone())) {
					Thread.sleep(1000/* ms */);
				}
//                for (int i = 0; i < 600; i++) {
//                	Thread.sleep(1000);
//                	if (failex.get() != null)
//                		break;
//                }
				if (failex.get() == null) {
					/*
					 * Note whether or not there are failures before we
					 * interrupt the running tasks.
					 */
                	success.set(true);
                }
                writers.shutdownNow();
                readers.shutdownNow();
                writers.awaitTermination(5, TimeUnit.SECONDS);
                readers.awaitTermination(5, TimeUnit.SECONDS);
				if (!success.get()) {
                    final Throwable ex = failex.get();
                    if (ex != null) {
                        fail("Test failed: firstCause=" + ex, ex);
                    }
                }
                if (log.isInfoEnabled())
                    log.info("Statements written: " + writes.get() + ", read: "
                            + reads.get());
            } finally {
                if (writers != null)
                    writers.shutdownNow();
                if (readers != null)
                    readers.shutdownNow();
	        }
		} finally {

			sail.__tearDownUnitTest();

		}

	}

	void domultiple_csem_transaction_onethread(final int retention, final int nuris, final int npreds) throws Exception {

//	    final PseudoRandom r = new PseudoRandom(20000 /*10000*/);
	    final Random r = new Random();
	    
        final CAT writes = new CAT();
        final CAT reads = new CAT();
//        final AtomicReference<Throwable> failex = new AtomicReference<Throwable>(null);
        // Set [true] iff there are no failures by the time we cancel the
        // running tasks.
//        final AtomicBoolean success = new AtomicBoolean(false);
        final BigdataSail sail = getSail(getProperties(retention));
        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            final AbstractTripleStore origStore = repo.getDatabase();

            final URI[] subs = new URI[nuris];
            for (int i = 0; i < nuris; i++) {
                subs[i] = uri("uri:" + i);
            }
            final URI[] preds = new URI[npreds + 20];
            for (int i = 0; i < npreds; i++) {
                preds[i] = uri("pred:" + i);
            }
            final int nwrites = 600;
            final int nreads = 50;
            final int ntrials = 20;
            final boolean isQuads = origStore.isQuads();

            for (int loop = 0; loop < ntrials; loop++) {
                final Long txId = ((Journal) origStore.getIndexManager())
                        .newTx(ITx.READ_COMMITTED);
                try {
                	// System.err.println("READ_STATE: " + txId);
                    final AbstractTripleStore readstore = (AbstractTripleStore) origStore
                            .getIndexManager().getResourceLocator()
                            .locate(origStore.getNamespace(), txId);
                    for (int i = 0; i < nreads; i++) {
                        final BigdataStatementIterator stats = readstore
//                        .getStatements(subs[nuris/2 + loop], null,
//                                null);
                        .getStatements(subs[r.nextInt(nuris)], null,
                                null);
                        try {
	                        while (stats.hasNext()) {
	                            stats.next();
	                            reads.increment();
	                        }
                        } finally {
                        	stats.close();
                        }
                    }

                    // Thread.sleep(r.nextInt(1000) + 500);
                    try {

                        for (int i = 0; i < nwrites; i++) {
                            origStore.addStatement(subs[r.nextInt(nuris)],
                                    preds[r.nextInt(npreds)],
                                    subs[r.nextInt(nuris)],
                                    isQuads ? subs[r.nextInt(nuris)] : null);
//                            origStore.addStatement(subs[nuris/2 + loop],
//                                    preds[npreds/2 + loop],
//                                    subs[nuris/2 - loop],
//                                    isQuads ? subs[nuris/2 + loop] : null);
                            writes.increment();
                            // System.out.print('.');
                        }
                        // System.out.println("\n");

                    } finally {
                        origStore.commit();
                    	log.warn("Commit: " + loop);
//                        if (log.isInfoEnabled())
//                            log.info("Commit");
                    }
                    // Close Read Connection
                    ((Journal) readstore.getIndexManager()).abort(txId);

                } catch (Throwable ise) {
                    log.error("firstCause:" + ise, ise);
                    throw new Exception(ise);
                }
            }

        } finally {

            sail.__tearDownUnitTest();

        }

    }
    
	protected URI uri(String s) {
		return new URIImpl(BD.NAMESPACE + s);
	}

	@Override
	public Properties getProperties() {

		Properties props = super.getProperties();

		props.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");
		props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
		props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
		props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
		props.setProperty(BigdataSail.Options.JUSTIFY, "false");
		props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
//		props.setProperty(Options.WRITE_CACHE_BUFFER_COUNT, "3");

		// ensure using RWStore
		props.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
//		props.setProperty(RWStore.Options.MAINTAIN_BLACKLIST, "false");
//		props.setProperty(RWStore.Options.OVERWRITE_DELETE, "true");
		// props.setProperty(Options.CREATE_TEMP_FILE, "false");
		// props.setProperty(Options.FILE, "/Volumes/SSDData/csem.jnl");
		
		props.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "500");
		props.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_SCAN, "10");

		return props;

	}
	
	protected Properties getProperties(int retention) {
		final Properties props = getProperties();
		props.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE, "" + retention);

		return props;
	}

}
