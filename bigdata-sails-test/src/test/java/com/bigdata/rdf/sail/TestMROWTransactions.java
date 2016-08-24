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
 * Created on Dec 19, 2006
 */
package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.SailException;

import com.bigdata.btree.AbstractNode;
import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.CAT;
import com.bigdata.journal.BufferMode;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.util.DaemonThreadFactory;
import com.bigdata.util.InnerCause;

/**
 * TestCase to test single writer/mutiple transaction committed readers with
 * SAIL interface.
 * 
 * @author Martyn Cutcher
 */
abstract public class TestMROWTransactions extends ProxyBigdataSailTestCase {

//    private static final Logger txLog = Logger.getLogger("com.bigdata.txLog");

    TestMROWTransactions() {
    }

    TestMROWTransactions(final String arg0) {
        super(arg0);
    }

//    void domultiple_csem_transaction_onethread(final int retentionMillis) throws Exception {
//
//        domultiple_csem_transaction_onethread(retentionMillis, 2000, 50);
//
//    }
//
//    void domultiple_csem_transaction(final int retentionMillis) throws Exception {
//
//        domultiple_csem_transaction2(retentionMillis, 2/* nreaderThreads */,
//                1000/* nwriters */, 20 * 1000/* nreaders */);
//
//    }

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
     * @param isolatableIndice
     *            When <code>true</code> the writers will use read/write
     *            transactions. Otherwise they will use the unisolated
     *            connection.
     * @throws Exception
     */
    void domultiple_csem_transaction2(final int retentionMillis,
            final int nreaderThreads, final int nwriters, final int nreaders,
            final boolean isolatableIndices) throws Exception {
        if (log.isInfoEnabled()) {
            log.info("=================================================================================");
            log.info("retentionMillis=" + retentionMillis + ", nreaderThreads="
                    + nreaderThreads + ", nwriters=" + nwriters + ", nreaders="
                    + nreaders + ", isolatableIndices=" + isolatableIndices);
            log.info("=================================================================================");
        }

        final BigdataSail sail = getSail(getProperties(retentionMillis,
                isolatableIndices));
        
        domultiple_csem_transaction2(sail, nreaderThreads, nwriters, nreaders, true);
    }
    
    static void domultiple_csem_transaction2( final BigdataSail sail,
            final int nreaderThreads, final int nwriters, final int nreaders, final boolean teardown) throws Exception {   

        /**
         * The most likely problem is related to the session protection in the
         * RWStore. In development we saw problems when concurrent transactions
         * had reduced the open/active transactions to zero, therefore releasing
         * session protection. If the protocol works correctly we should never
         * release session protection if any transaction has been initialized.
         * 
         * The message of "invalid address" would be generated if an allocation
         * has been freed and is no longer protected from recycling when an
         * attempt is made to read from it.
         * 
         * TODO Experiment with different values of [nthreads] for the with and
         * w/o history variations of this test. Consider lifting that parameter
         * into the signature of this method.
         */
        final int nuris = 2000; // number of unique subject/objects
        final int npreds = 50; //
        // final PseudoRandom r = new PseudoRandom(2000);
        // r.next(1500);
        final Random r = new Random();

//        final int maxAborts = 100;
        
        final CAT commits = new CAT();
        final CAT aborts = new CAT();
        final CAT nreadersDone = new CAT();
        final AtomicReference<Throwable> failex = new AtomicReference<Throwable>(null);
        // Set [true] iff there are no failures by the time we cancel the running tasks.
        final AtomicBoolean success = new AtomicBoolean(false);
        // log.warn("Journal: "+sail.getDatabase().getIndexManager()+", file="+((Journal)sail.getDatabase().getIndexManager()).getFile());
        try {

            sail.initialize();
            // TODO Force an initial commit?

//            final BigdataSailRepository repo = new BigdataSailRepository(sail);
//            final AbstractTripleStore origStore = repo.getDatabase();

            final URI[] subs = new URI[nuris];
            for (int i = 0; i < nuris; i++) {
                subs[i] = uri("uri:" + i);
            }
            final URI[] preds = new URI[npreds];
            for (int i = 0; i < npreds; i++) {
                preds[i] = uri("pred:" + i);
            }

            ExecutorService writers = null;
            ExecutorService readers = null;
            try {

                writers = Executors
                        .newSingleThreadExecutor(new DaemonThreadFactory(
                                "test-writer-pool"));

                readers = Executors.newFixedThreadPool(nreaderThreads,
                        new DaemonThreadFactory("test-reader-pool"));

                // let's schedule a few writers and readers (more than needed)
                // writers.submit(new Writer(5000000/* nwrite */));
                Future<Long> lastWriterFuture = null;
                @SuppressWarnings("unused")
                Future<Long> lastReaderFuture = null;

                for (int i = 0; i < nwriters; i++) {

                    lastWriterFuture = writers.submit(new Writer(r,
                            500/* nwrites */, sail, commits, aborts,
                            /*maxAborts,*/ failex, subs, preds));
                    
                }

                for (int rdrs = 0; rdrs < nreaders; rdrs++) {
                	
                	final int nreads = rdrs == 0 ? Integer.MAX_VALUE : 60; // reasonably long running hopefully

                    lastReaderFuture = readers.submit(new Reader(r,
                            nreads, nwriters, sail, failex,
                            commits, nreadersDone, subs));

                }
                
                // let the writers run riot for a time, checking for failure
                while (true) {
//                    final boolean bothDone = lastWriterFuture.isDone()
//                            && lastReaderFuture.isDone();
//                    if (bothDone)
//                        break;
                    if(lastWriterFuture.isDone()) {
                        // End test when the writers are done.
                        break;
                    }
                    if (failex.get() != null) {
                        // Something errored.
                        break;
                    }
                    Thread.sleep(250/* ms */);
                }
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
                        fail("Test failed: firstCause=" + ex
                                + ", nreaderThreads=" + nreaderThreads
                                + ", nwriters=" + nwriters + ", nreaders="
                                + nreaders + ", indexManager="
                                + sail.getIndexManager(), ex);
                    }
                }
                if (log.isInfoEnabled())
                    log.info("Writers committed: " + commits.get()
                            + ", writers aborted: " + aborts.get()
                            + ", readers done: " + nreadersDone.get());
            } finally {
                if (writers != null)
                    writers.shutdownNow();
                if (readers != null)
                    readers.shutdownNow();
            }
        } finally {
        	if (teardown) {
	            try {
	                sail.__tearDownUnitTest();
	            } catch (Throwable t) {
	                /*
	                 * FIXME The test helper tear down should not throw anything,
	                 * but it can do so if a tx has been asynchronously closed. This
	                 * has to do with the logic that openrdf uses to close open
	                 * transactions when the sail is shutdown by the caller.
	                 */
	                log.error("Problem with test shutdown: " + t, t);
	            }
        	}

        }

    }

    /** Writer task adds nwrites statements then commits */
    static private class Writer implements Callable<Long> {

        final Random r;
        final int nwrites;
        final BigdataSail sail;
        final CAT commits;
        final CAT aborts;
//        final int maxAborts;
        final AtomicReference<Throwable> failex;
        final int nuris;
        final int npreds;
        final URI[] subs;
        final URI[] preds;

        Writer(final Random r, final int nwrites,
                final BigdataSail sail, final CAT commits,
                final CAT aborts, //final int maxAborts,
                final AtomicReference<Throwable> failex, final URI[] subs,
                final URI[] preds) {

            this.r = r;
            this.nwrites = nwrites;
            this.sail = sail;
            this.commits = commits;
            this.aborts = aborts;
//            this.maxAborts = maxAborts;
            this.failex = failex;
            this.nuris = subs.length;
            this.npreds = preds.length;
            this.subs = subs;
            this.preds = preds;

        }

        @Override
        public Long call() throws Exception {
            // Thread.sleep(r.nextInt(2000) + 500);
            BigdataSailConnection con = null;
            boolean ok = false;
            try {
                con = sail.getConnection();
                final boolean isQuads = con.isQuads();
                for (int i = 0; i < nwrites; i++) {
                    con.addStatement(subs[r.nextInt(nuris)],
                            preds[r.nextInt(npreds)], subs[r.nextInt(nuris)],
                            isQuads ? subs[r.nextInt(nuris)] : null);
                    // System.out.print('.');
                }
                // System.out.println("\n");
                con.commit();
                ok = true;
                commits.increment();
                if (log.isInfoEnabled())
                    log.info("Commit #" + commits);

            } catch (Throwable ise) {
                if (InnerCause.isInnerCause(ise, InterruptedException.class)) {
                    // ignore
                } else if (InnerCause.isInnerCause(ise, MyBTreeException.class)
                        //&& aborts.get() < maxAborts
                        ) {
                    // ignore
                } else {
                    log.warn(ise, ise);
                    // Set the first cause (but not for the forced abort).
                    if (failex
                            .compareAndSet(null/* expected */, ise/* newValue */)) {
                        log.error("firstCause:" + ise, ise);
                    }
                }
            } finally {
                if (con != null) {
                    if (!ok) {
                        con.rollback();
                        aborts.increment();
                        log.error("Abort #" + aborts + " (with "
                                + commits.get() + " commits)");
                    }
                    con.close();
                }
            }
            return null;
        }

    } // Writer

    /** ReaderTask makes nreads and closes. */
    private static class Reader implements Callable<Long> {

        final Random r;
        final int nreads;
        final int nwriters;
        final BigdataSail sail;
        final AtomicReference<Throwable> failex;
        final CAT commits;
        final CAT nreadersDone;
        final int nuris;
        final URI[] subs;

        Reader(final Random r, final int nreads, final int nwriters,
                final BigdataSail sail,
                final AtomicReference<Throwable> failex, final CAT commits,
                final CAT nreadersDone, final URI[] subs) {
            this.r = r;
            this.nreads = nreads;
            this.nwriters = nwriters;
            this.sail = sail;
            this.failex = failex;
            this.commits = commits;
            this.nreadersDone = nreadersDone;
            this.nuris = subs.length;
            this.subs = subs;
        }

        @Override
        public Long call() throws Exception {
            BigdataSailConnection con = null;
            try {
                con = sail.getReadOnlyConnection();
                /*
                 * Note: This sleep makes it much easier to hit the bug
                 * documented here. However, the sleep can also cause the test
                 * to really stretch out. So the sleep is only used until the
                 * writers are done.
                 * 
                 * https://sourceforge.net/apps/trac/bigdata/ticket/467
                 */
                if (commits.get() < Math.max(nwriters, 5))
                    Thread.sleep(2000/* millis */);

                for (int i = 0; i < nreads; i++) {
                    final CloseableIteration<? extends Statement, SailException> stats = con
                            .getStatements(subs[r.nextInt(nuris)], (URI) null,
                                    (Value) null, (Resource) null);
                    try {
                        while (stats.hasNext()) {
                            stats.next();
                        }
                    } finally {
                        stats.close();
                    }
                }
            } catch (Throwable ise) {
                if (InnerCause.isInnerCause(ise, InterruptedException.class)) {
                    // Ignore.
                } else {
                    if (failex
                            .compareAndSet(null/* expected */, ise/* newValue */)) {
                        log.error("firstCause:" + ise, ise);
                    } else {
                        if (log.isInfoEnabled())
                            log.info("Other error: " + ise, ise);
                    }
                }
            } finally {
                if (con != null) {
                    con.rollback();
                    con.close();
                }
                nreadersDone.increment();
            }
            return null;
        }

    } // Reader

    
//    void domultiple_csem_transaction_onethread(final int retention, final int nuris, final int npreds) throws Exception {
//
//        // final PseudoRandom r = new PseudoRandom(20000 /*10000*/);
//        final Random r = new Random();
//
//        final CAT writes = new CAT();
//        final CAT reads = new CAT();
////        final AtomicReference<Throwable> failex = new AtomicReference<Throwable>(null);
//        // Set [true] iff there are no failures by the time we cancel the
//        // running tasks.
//        // final AtomicBoolean success = new AtomicBoolean(false);
//        final boolean isolatableIndices = false;
//        final BigdataSail sail = getSail(getProperties(retention,isolatableIndices));
//        try {
//
//            sail.initialize();
//            final BigdataSailRepository repo = new BigdataSailRepository(sail);
//            final AbstractTripleStore origStore = repo.getDatabase();
//
//            final URI[] subs = new URI[nuris];
//            for (int i = 0; i < nuris; i++) {
//                subs[i] = uri("uri:" + i);
//            }
//            final URI[] preds = new URI[npreds + 20];
//            for (int i = 0; i < npreds; i++) {
//                preds[i] = uri("pred:" + i);
//            }
//            final int nwrites = 600;
//            final int nreads = 50;
//            final int ntrials = 20;
//            final boolean isQuads = origStore.isQuads();
//
//            for (int loop = 0; loop < ntrials; loop++) {
//                final Long txId = ((Journal) origStore.getIndexManager())
//                        .newTx(ITx.READ_COMMITTED);
//                try {
//                    // System.err.println("READ_STATE: " + txId);
//                    final AbstractTripleStore readstore = (AbstractTripleStore) origStore
//                            .getIndexManager().getResourceLocator()
//                            .locate(origStore.getNamespace(), txId);
//                    for (int i = 0; i < nreads; i++) {
//                        final BigdataStatementIterator stats = readstore
//                        // .getStatements(subs[nuris/2 + loop], null,
//                        // null);
//                                .getStatements(subs[r.nextInt(nuris)], null,
//                                        null);
//                        try {
//                            while (stats.hasNext()) {
//                                stats.next();
//                                reads.increment();
//                            }
//                        } finally {
//                            stats.close();
//                        }
//                    }
//
//                    // Thread.sleep(r.nextInt(1000) + 500);
//                    try {
//
//                        for (int i = 0; i < nwrites; i++) {
//                            origStore.addStatement(subs[r.nextInt(nuris)],
//                                    preds[r.nextInt(npreds)],
//                                    subs[r.nextInt(nuris)],
//                                    isQuads ? subs[r.nextInt(nuris)] : null);
//                            // origStore.addStatement(subs[nuris/2 + loop],
//                            // preds[npreds/2 + loop],
//                            // subs[nuris/2 - loop],
//                            // isQuads ? subs[nuris/2 + loop] : null);
//                            writes.increment();
//                            // System.out.print('.');
//                        }
//                        // System.out.println("\n");
//
//                    } finally {
//                        origStore.commit();
//                        log.warn("Commit: " + loop);
//                        // if (log.isInfoEnabled())
//                        // log.info("Commit");
//                    }
//                    // Close Read Connection
//                    ((Journal) readstore.getIndexManager()).abort(txId);
//
//                } catch (Throwable ise) {
//                    log.error("firstCause:" + ise, ise);
//                    throw new Exception(ise);
//                }
//            }
//
//        } finally {
//
//            sail.__tearDownUnitTest();
//
//        }
//
//    }

    protected static URI uri(String s) {
        return new URIImpl(BD.NAMESPACE + s);
    }

    @Override
    public Properties getProperties() {

        final Properties props = super.getProperties();

        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        // props.setProperty(Options.WRITE_CACHE_BUFFER_COUNT, "3");

        // ensure using RWStore
        props.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
        // props.setProperty(RWStore.Options.MAINTAIN_BLACKLIST, "false");
        // props.setProperty(RWStore.Options.OVERWRITE_DELETE, "true");
        // props.setProperty(Options.CREATE_TEMP_FILE, "false");
        // props.setProperty(Options.FILE, "/Volumes/SSDData/csem.jnl");

        // props.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "20");
        // props.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_SCAN, "0");
        props.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "500");
        props.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_SCAN, "10");

        return props;

    }

    protected Properties getProperties(final int retention,
            final boolean isolatableIndices) {

        final Properties props = getProperties();
        
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        // props.setProperty(Options.WRITE_CACHE_BUFFER_COUNT, "3");

        // ensure using RWStore
        props.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
        // props.setProperty(RWStore.Options.MAINTAIN_BLACKLIST, "false");
        // props.setProperty(RWStore.Options.OVERWRITE_DELETE, "true");
        // props.setProperty(Options.CREATE_TEMP_FILE, "false");
        // props.setProperty(Options.FILE, "/Volumes/SSDData/csem.jnl");

        // props.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "20");
        // props.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_SCAN, "0");
        props.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "500");
        props.setProperty(IndexMetadata.Options.WRITE_RETENTION_QUEUE_SCAN, "10");

        setProperties(props, retention, isolatableIndices);
        
        return props;
    }

   static void setProperties(final Properties props, final int retention,
           final boolean isolatableIndices) {
        props.setProperty(BigdataSail.Options.ISOLATABLE_INDICES,
                Boolean.toString(isolatableIndices));

        props.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE,
                "" + retention);

        final boolean isQuads = Boolean.valueOf(props.getProperty(
                Options.QUADS_MODE, "false"));

        /**
         * Force override of the BTree on one index to occasionally prompt
         * errors during the test run.
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/855"> AssertionError:
         *      Child does not have persistent identity </a>.
         */
        if (!isolatableIndices) {
            /*
             * Note: if this is used with read/write tx for the updates then we
             * do not observe the desired exception in the Writer class when we
             * call con.commit(). This causes the test to fail, but it is
             * failing in an uninteresting manner. Hence, the forced abort of
             * the B+Tree update is only present at this time for the unisolated
             * indices. This is where the problem is reported for ticket #855.
             */
            final String name = isQuads ? "SPOC" : "SPO";
            props.setProperty("com.bigdata.namespace.kb.spo." + name
                    + ".com.bigdata.btree.BTree.className",
                    MyBTree.class.getName());
        }
    }
    
    

    /**
     * Helper class for force abort of a B+Tree write.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/855"> AssertionError: Child
     *      does not have persistent identity </a>.
     */
    public static class MyBTree extends BTree {

        private final Random r = new Random(12L);
        
        public MyBTree(IRawStore store, Checkpoint checkpoint,
                IndexMetadata metadata, boolean readOnly) {

            super(store, checkpoint, metadata, readOnly);

        }

        @Override
        protected long writeNodeOrLeaf(final AbstractNode<?> node) {
            
            if (node.isLeaf() && r.nextInt(500) == 0) {

                throw new MyBTreeException("Forcing abort: " + this);

            }

            final long addr = super.writeNodeOrLeaf(node);
            
            return addr;
            
        }

    }
    /** Marker exception for a force abort of a B+Tree write. */
    private static class MyBTreeException extends RuntimeException {

        public MyBTreeException(final String string) {
            super(string);
        }

        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        
    }

    /** utilities for main subclass support **/
	static long getLongArg(final String[] args, final String arg, final long def) {
		final String sv = getArg(args, arg, null);
		
		return sv == null ? def : Long.parseLong(sv);
	}
	
	static String getArg(final String[] args, final String arg, final String def) {
		for (int p = 0; p < args.length; p+=2) {
			if (arg.equals(args[p]))
				return args[p+1];
		}
		
		return def;
	}
    
	/**
	 * Command line variant to allow stress testing without JUnit support
	 * 
	 * Invokes the same domultiple_csem_transaction2 method.
	 * 
	 * A property file is required.  Note that if a file is specified then
	 * it will be re-opened and not removed for each run as specified by
	 * nruns.
	 * 
	 * Optional arguments
	 * -nruns - number of runs through the test
	 * -nreaderthreads - reader threads
	 * -nwriters - writer tasks
	 * -nreaders - reader tasks
	 */
	public static void main(String[] args) throws Exception {

		final String propertyFile = getArg(args, "-propertyfile", null);
		if (propertyFile == null) {
			System.out.println("-propertyfile <properties> must be specified");
			return;
		}
			
		
		final Properties props = new Properties();
		
		props.load(new FileInputStream(propertyFile));
		
		final AtomicReference<BigdataSail> sail = new AtomicReference<BigdataSail>(new BigdataSail(props));

		final int nreaderThreads = (int) getLongArg(args, "-nreaderthreads", 20); // 20

		final long nwriters = getLongArg(args, "-nwriters", 100); // 1000000;

		final long nreaders = getLongArg(args, "-nreaders", 400); // 100000;

		final long nruns = getLongArg(args, "-nruns", 1); // 1000;
		
		final Thread sailShutdown = new Thread() {
			public void run() {
				final Random r = new Random();
				while(true) {
					try {
						Thread.sleep(r.nextInt(50000));
						if (sail.get().isOpen()) {
							log.warn("SHUTDOWN NOW");
							sail.get().shutDown();
						}
					} catch (InterruptedException e) {
						break;
					} catch (SailException e) {
						log.warn(e);
					}
				}
			}
		};
		
		sailShutdown.start();

		for (int i = 0; i < nruns; i++) {
			try {
				domultiple_csem_transaction2(sail.get(), (int) nreaderThreads,
						(int) nwriters, (int) nreaders, false /*no tear down*/);
				
				// reopen for second run - should be open if !teardown
				if (sail.get().isOpen())
					sail.get().shutDown();
			} catch (Throwable e) {
				log.warn("OOPS", e); // There will be a number of expected causes, eg IllegalStateException - service not available
			}
			
			sail.set(new BigdataSail(props));

			System.out.println("Completed run: " + i);
		}
		
		sailShutdown.interrupt();
	}

}
