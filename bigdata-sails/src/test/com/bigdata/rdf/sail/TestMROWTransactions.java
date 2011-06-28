package com.bigdata.rdf.sail;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
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
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * TestCase to test single writer/mutiple transaction committed readers with
 * SAIL interface.
 * 
 * @author Martyn Cutcher
 * 
 */
public class TestMROWTransactions extends ProxyBigdataSailTestCase {

	/**
     * 
     */
	public TestMROWTransactions() {
	}

	/**
	 * @param arg0
	 */
	public TestMROWTransactions(String arg0) {
		super(arg0);
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
		// props.setProperty(Options.CREATE_TEMP_FILE, "false");
		// props.setProperty(Options.FILE, "/Volumes/SSDData/csem.jnl");

		return props;

	}

    protected void setUp() throws Exception {
        super.setUp();
    }
    
    protected void tearDown() throws Exception {
        super.tearDown();
    }
    
	private URI uri(String s) {
		return new URIImpl(BD.NAMESPACE + s);
	}

	private BNode bnode(String id) {
		return new BNodeImpl(id);
	}

	private Statement stmt(Resource s, URI p, Value o) {
		return new StatementImpl(s, p, o);
	}

	private Statement stmt(Resource s, URI p, Value o, Resource c) {
		return new ContextStatementImpl(s, p, o, c);
	}

//	public void test_multiple_transaction() throws Exception {
//
//		final int nthreads = 10; // 
//		final int nuris = 2000; // 
//		final int npreds = 50; // 
//		final Random r = new Random();
//
//		ExecutorService writers = Executors.newSingleThreadExecutor(DaemonThreadFactory.defaultThreadFactory());
//		ExecutorService readers = Executors.newFixedThreadPool(nthreads, DaemonThreadFactory.defaultThreadFactory());
//
//		final BigdataSail sail = getSail();
//		final URI[] subs = new URI[nuris];
//		for (int i = 0; i < nuris; i++) {
//			subs[i] = uri("uri:" + i);
//		}
//		final URI[] preds = new URI[npreds];
//		for (int i = 0; i < npreds; i++) {
//			preds[i] = uri("pred:" + i);
//		}
//		final AtomicInteger writes = new AtomicInteger();
//		final AtomicInteger reads = new AtomicInteger();
//		try {
//			sail.initialize();
//			final BigdataSailRepository repo = new BigdataSailRepository(sail);
//
//			// Writer task adds nwrites statements then commits
//			class Writer implements Callable<Long> {
//				final int nwrites;
//
//				Writer(final int nwrites) {
//					this.nwrites = nwrites;
//				}
//
//				public Long call() throws Exception {
//					final RepositoryConnection tx1 = repo.getReadWriteConnection();
//					try {
//						tx1.setAutoCommit(false);
//
//						for (int i = 0; i < nwrites; i++) {
//							tx1.add(stmt(subs[r.nextInt(500)], preds[r.nextInt(20)], subs[r.nextInt(500)]));
//							writes.incrementAndGet();
//						}
//						tx1.commit();
//
//					} finally {
//						tx1.close();
//					}
//
//					return null;
//				}
//
//			}
//
//			// ReaderTask makes nreads and closes
//			class Reader implements Callable<Long> {
//				final int nreads;
//
//				Reader(final int nwrites) {
//					this.nreads = nwrites;
//				}
//
//				public Long call() throws Exception {
//					final RepositoryConnection tx1 = repo.getReadOnlyConnection();
//					try {
//
//						for (int i = 0; i < nreads; i++) {
//							RepositoryResult<Statement> stats = tx1.getStatements(subs[r.nextInt(500)], null, null, true);
//							while (stats.hasNext()) {
//								stats.next();
//								reads.incrementAndGet();
//							}
//						}
//
//					} finally {
//						tx1.close();
//					}
//
//					return null;
//				}
//
//			}
//
//			// let's schedule a few writers and readers
//			for (int i = 0; i < 500; i++) {
//				writers.submit(new Writer(500));
//				for (int rdrs = 0; rdrs < 20; rdrs++) {
//					readers.submit(new Reader(50));
//				}
//			}
//
//			Thread.sleep(60 * 1000);
//			writers.shutdownNow();
//			readers.shutdownNow();
//			writers.awaitTermination(5, TimeUnit.SECONDS);
//			readers.awaitTermination(5, TimeUnit.SECONDS);
//			System.out.println("Statements written: " + writes.get() + ", read: " + reads.get());
//		} finally {
//
//			sail.__tearDownUnitTest();
//
//		}
//
//	}

	// similar to test_multiple_transactions but uses direct AbsractTripleStore
	// manipulations rather than RepositoryConnections
	public void test_multiple_csem_transaction() throws Exception {

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
		 */
		final int nthreads = 5; // up count to increase chance startup condition
								// decrement to increase chance of idle (no sessions)
		final int nuris = 2000; // number of unique subject/objects
		final int npreds = 50; // 
		final Random r = new Random();

		final CAT writes = new CAT();
		final CAT reads = new CAT();
		final AtomicReference<Throwable> failex = new AtomicReference<Throwable>(null);
        final BigdataSail sail = getSail();
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
                        Thread.sleep(r.nextInt(2000) + 500);
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
                            final AbstractTripleStore readstore = (AbstractTripleStore) origStore
                                    .getIndexManager().getResourceLocator()
                                    .locate(origStore.getNamespace(), txId);

                            for (int i = 0; i < nreads; i++) {
                                final BigdataStatementIterator stats = readstore
                                        .getStatements(subs[r.nextInt(nuris)],
                                                null, null);
                                while (stats.hasNext()) {
                                    stats.next();
                                    reads.increment();
                                }
                            }

                        } catch (IllegalStateException ise) {
                        	failex.compareAndSet(null, ise);
                        } finally {
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

                readers = Executors.newFixedThreadPool(nthreads,
                        DaemonThreadFactory.defaultThreadFactory());

                // let's schedule a few writers and readers (more than needed)
                // writers.submit(new Writer(5000000/* nwrite */));
                for (int i = 0; i < 5000; i++) {
                    writers.submit(new Writer(500/* nwrite */));
                    for (int rdrs = 0; rdrs < 20; rdrs++) {
                        readers.submit(new Reader(60/* nread */));
                    }
                }

                // let the writers run riot for a time, checking for failure
                for (int i = 0; i < 60; i++) {
                	Thread.sleep(1000);
                	if (failex.get() != null)
                		break;
                }
                writers.shutdownNow();
                readers.shutdownNow();
                writers.awaitTermination(5, TimeUnit.SECONDS);
                readers.awaitTermination(5, TimeUnit.SECONDS);
                {
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

}
