/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jan 9, 2008
 */
package com.bigdata.rdf.store;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RioLoaderEvent;
import com.bigdata.rdf.rio.RioLoaderListener;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * A helper class designed to drive concurrent load of recursively process files
 * and directories (think LUBM). While closure is NOT maintained during the load
 * operation, you can perform a database at once closure afterwards if you are
 * bulk loading some dataset into an empty database.
 * 
 * @todo if this proves useful promote it into the main source code (it is in
 *       with the test suites right now).
 * 
 * @todo look into error handling (backing out from a partial load).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ConcurrentDataLoader {

    protected static final Logger log = Logger
            .getLogger(ConcurrentDataLoader.class);

    /**
     * The database on which the data will be written.
     */
    final AbstractTripleStore db;

    //        /**
    //         * A queue onto which are written the names of files to be loaded. The
    //         * capacity of this queue limits the read ahead in the file system.
    //         */
    //        final BlockingQueue<File> queue = new ArrayBlockingQueue<File>(1000);

    //        /**
    //         * Single-thread executor reading from the file system and populating
    //         * the #queue
    //         */
    //        final ExecutorService readService = Executors
    //                .newSingleThreadExecutor(DaemonThreadFactory
    //                        .defaultThreadFactory());

    /**
     * Thread pool provinding concurrent load services.
     */
    final ThreadPoolExecutor loadService;

    /**
     * The {@link Future}s from the {@link LoadTask}s.
     */
    final List<Future> futures = new LinkedList<Future>();
    
    /**
     * The #of threads given to the ctor (the pool can be using a different
     * number of threads since that is partly dynamic).
     */
    final int nthreads;
    
    /**
     * Capacity of the {@link StatementBuffer}.
     */
    final int bufferCapacity;

    /**
     * Validation of RDF by the RIO parser is disabled.
     */
    final boolean verifyData = false;

    /**
     * The kind of RDF interchange syntax that will be parsed.
     */
    final RDFFormat rdfFormat = RDFFormat.RDFXML;

    /**
     * 
     * @param db
     *            The database on which the data will be written.
     * @param nthreads
     *            The #of concurrent loaders.
     * @param bufferCapacity
     *            The capacity of the {@link StatementBuffer} (the #of
     *            statements that are buffered into a batch operation).
     * @param file
     *            The file or directory to be loaded.
     * @param filter
     *            An optional filter used to select the files to be loaded.
     * 
     * @todo explore interaction of the buffer size with the #of concurrent
     *       data load threads.
     */
    public ConcurrentDataLoader(AbstractTripleStore db, int nthreads, int bufferCapacity,
            File file, FilenameFilter filter) {

        this.db = db;

        this.nthreads = nthreads;
        
        this.bufferCapacity = bufferCapacity;
        
        loadService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                nthreads, DaemonThreadFactory.defaultThreadFactory());

        //            // limit the maximum #of concurrent loaders.
        //            loadService.setMaximumPoolSize(maxPoolSize);

        try {

            process(file, filter);

        } catch (Throwable t) {

            log.fatal(t);

            try {

                shutdownNow();
                
            } catch (Throwable t2) {
                
                log.warn("Problems during shutdown: " + t2);
                
            }

            throw new RuntimeException(t);

        }

    }

    //        public void shutdown() throws InterruptedException {
    //            
    ////            readService.shutdown();
    ////
    ////            readService.awaitTermination(10L, TimeUnit.SECONDS);
    //            
    //            loadService.shutdown();
    //            
    //            loadService.awaitTermination(60L, TimeUnit.SECONDS);
    //            
    //        }

    private void shutdownNow() throws InterruptedException {

        //            readService.shutdownNow();
        //
        //            readService.awaitTermination(1L, TimeUnit.SECONDS);

        loadService.shutdownNow();

        loadService.awaitTermination(2L, TimeUnit.SECONDS);

    }

    /**
     * Process a file (recursive).
     * 
     * @param file
     *            Either a plain file or directory containing files to be
     *            processed.
     * @param filter
     *            An optional filter.
     *            
     * @throws IOException
     */
    private void process(File file, FilenameFilter filter) throws IOException, InterruptedException {

        final long begin = System.currentTimeMillis();

        AtomicInteger nloaded = new AtomicInteger(0);

        process2(file, filter, nloaded);

        log.info("All files scanned: " + nloaded);

        // normal termination.
        loadService.shutdown();

        /*
         * wait for the load tasks to complete.
         * 
         * @todo consume the futures, reporting on the load results to a file.
         */
        while (true) {

            log.info("Awaiting termination: completed="
                    + loadService.getCompletedTaskCount() + ", active="
                    + loadService.getActiveCount() + ", pending="
                    + loadService.getTaskCount());

            if (loadService.awaitTermination(60L/* 1 minute */,
                    TimeUnit.SECONDS)) {

                log.info("Load terminated normally.");

                db.commit();
                
                break;

            }

        }

        final long elapsed = System.currentTimeMillis() - begin;

        final int nstmts = db.getStatementCount();
        
        final double tps = (long) (((double) nstmts) / ((double) elapsed) * 1000d);

        /*
         * Examine the futures and report any errors.
         */
        int ndone = 0;
        int nok = 0;
        int nerr = 0;
        
        for (Future f : futures) {

            if (f.isDone()) {

                ndone++;

                try {
                    f.get();

                    nok++;

                } catch (ExecutionException ex) {
                    
                    nerr++;
                    
                    log.warn("Error: " + ex);
                    
                }

            }
            
        }
                
        log.info("Finished: #loaded=" + nloaded + " files in " + elapsed
                + " ms, #stmts=" + nstmts + ", rate=" + tps + " (#threads="
                + nthreads + ", largestPoolSize="
                + loadService.getLargestPoolSize() + ", bufferCapacity="
                + bufferCapacity + ", #done=" + ndone + ", #ok=" + nok
                + ", #err=" + nerr + ")");
        
    }

    private void process2(File file, FilenameFilter filter, AtomicInteger nloaded) {

        if (file.isDirectory()) {

            log.info("Scanning directory: " + file);

            File[] files = filter == null ? file.listFiles() : file
                    .listFiles(filter);

            for (File f : files) {

                process2(f, filter, nloaded);

            }

        } else {

            /*
             * Processing a standard file.
             */

            log.info("Scanning file: " + file);

            nloaded.incrementAndGet();

            futures.add(loadService.submit(new LoadTask(file)));

        }

    }

    class LoadTask implements Runnable {

        final File file;

        public LoadTask(File file) {

            this.file = file;

        }

        public void run() {

            log.info("Processing file: " + file);

            try {

                loadData2(file.toString(), ""/* baseURL */, rdfFormat);

            } catch (IOException e) {

                log.error("file=" + file + ", error=" + e);

                throw new RuntimeException(e);

            }

        }

        /**
         * Load an RDF resource into the database.
         * 
         * @todo reuse statement buffers (thread local?)
         * 
         * @todo we can't disable flush after each load unless we reuse
         *       statement buffers and force all statement buffers to the
         *       database when the total load process is done.
         */
        protected LoadStats loadData2(String resource, String baseURL,
                RDFFormat rdfFormat) throws IOException {

            final long begin = System.currentTimeMillis();

            StatementBuffer buffer = new StatementBuffer(null, db,
                    bufferCapacity);

            LoadStats stats = new LoadStats();

            log.info("loading: " + resource);

            PresortRioLoader loader = new PresortRioLoader(buffer);

            //                // disable auto-flush - caller will handle flush of the buffer.
            //                loader.setFlush(false);

            loader.addRioLoaderListener(new RioLoaderListener() {

                public void processingNotification(RioLoaderEvent e) {

                    log.info("file=" + file + ", " + e.getStatementsProcessed()
                            + " stmts added in " + (e.getTimeElapsed() / 1000d)
                            + " secs, rate= " + e.getInsertRate());

                }

            });

            InputStream rdfStream = getClass().getResourceAsStream(resource);

            if (rdfStream == null) {

                /*
                 * If we do not find as a Resource then try the file system.
                 */

                rdfStream = new FileInputStream(resource);
                //                    rdfStream = new BufferedInputStream(new FileInputStream(resource));

            }

            /* 
             * Obtain a buffered reader on the input stream.
             */

            // @todo reuse the backing buffer to minimize heap churn. 
            Reader reader = new BufferedReader(new InputStreamReader(rdfStream)
            //                       , 20*Bytes.kilobyte32 // use a large buffer (default is 8k)
            );

            try {

                loader.loadRdf(reader, baseURL, rdfFormat, verifyData);

                long nstmts = loader.getStatementsAdded();

                stats.toldTriples = nstmts;

                stats.loadTime = System.currentTimeMillis() - begin;
                //
                //                    if (closureEnum == ClosureEnum.Incremental
                //                            || (endOfBatch && closureEnum == ClosureEnum.Batch)) {
                //                        
                //                        /*
                //                         * compute the closure.
                //                         * 
                //                         * FIXME closure stats are not being reported out, e.g., to the DataLoader.
                //                         * 
                //                         * Also, batch closure logically belongs in the outer method.
                //                         */
                //                        
                //                        log.info("Computing closure.");
                //                        
                //                        stats.closureStats.add(doClosure());
                //                        
                //                    }
                //                    
                //                    // commit the data.
                //                    if(commitEnum==CommitEnum.Incremental) {
                //                        
                //                        log.info("Commit after each resource");
                //
                //                        long beginCommit = System.currentTimeMillis();
                //                        
                //                        database.commit();
                //
                //                        stats.commitTime = System.currentTimeMillis() - beginCommit;
                //
                //                        log.info("commit: latency="+stats.commitTime+"ms");
                //                        
                //                    }

                stats.totalTime = System.currentTimeMillis() - begin;

                log.info(stats.toString());

                return stats;

            } catch (Exception ex) {

                /*
                 * Note: discard anything in the buffer in case auto-flush is
                 * disabled. This prevents the buffer from retaining data after a
                 * failed load operation. The caller must still handle the thrown
                 * exception by discarding the writes already on the backing store
                 * (that is, by calling abort()).
                 */

                buffer.clear();

                log.error("file="+file+" : "+ex, ex);
                
                throw new RuntimeException("While loading: " + resource, ex);

            } finally {

                reader.close();

                rdfStream.close();

            }

        }

    };

}
