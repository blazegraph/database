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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
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
import com.bigdata.rdf.spo.ISPOBuffer;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * This is a utility class designed for concurrent load of recursively processed
 * files and directories.
 * <p>
 * Note: The concurrent data loader can combine small files within the same
 * thread in order to get better performance on ordered writes by NOT explicitly
 * flushing the {@link StatementBuffer}s in the {@link LoadTask}s - instead it
 * waits until all files have been processed and flushs the buffers when the
 * thread pool is shutdown.
 * <p>
 * Note: Distributed concurrent load may be realized using a pre-defined hash
 * function, e.g., of the file name, modulo the #of hosts on which the loader
 * will run in order to have each file processed by one out of N hosts in a
 * cluster.
 * <p>
 * Note: Closure is NOT maintained during the load operation. However, you can
 * perform a database at once closure afterwards if you are bulk loading some
 * dataset into an empty database.
 * 
 * @todo As an alternative to indexing the locally loaded data, we could just
 *       fill {@link StatementBuffer}s, convert to {@link ISPOBuffer}s (using
 *       the distributed terms indices), and then write out the long[3] data
 *       into a raw file. Once the local data have been converted to long[]s we
 *       can sort them into total SPO order (by chunks if necessary) and build
 *       the scale-out SPO index. The same process could then be done for each
 *       of the other access paths (OSP, POS).
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
     * The baseURL and "" if none is needed.
     */
    final String baseURL;
    
    /**
     * An attempt will be made to determine the interchange syntax using
     * {@link RDFFormat}. If no determination can be made then the loader will
     * presume that the files are in the format specified by this parameter (if
     * any). Files whose format can not be determined will be logged as errors.
     */
    final RDFFormat fallback;

    /**
     * When <code>true</code> the {@link StatementBuffer}s are flushed as
     * each file is loaded. When <code>false</code> the buffers are reused by
     * each file loaded by the same thread, flush when they overflow, and are
     * explictly flushed once no more files remain to be loaded.
     */
    final boolean autoFlush;

    final int nclients;
    
    final int clientNum;
    
    AtomicInteger nscanned = new AtomicInteger(0);

    AtomicInteger ntasked = new AtomicInteger(0);

    /**
     * Validation of RDF by the RIO parser is disabled.
     */
    final boolean verifyData = false;


    /**
     * A collection of buffers that need to be flushed once the {@link LoadTask}s
     * are over (iff {@link #autoFlush} is <code>false</code>).
     */
    protected Set<StatementBuffer> buffers;
    
    /**
     * Return the statement buffer to be used for a load task.
     */
    protected StatementBuffer getStatementBuffer() {

        /*
         * Note: this is a thread-local so the same buffer object is always
         * reused by the same thread.
         */

        return threadLocal.get();
        
    }
    
    private ThreadLocal<StatementBuffer> threadLocal = new ThreadLocal<StatementBuffer>() {

        protected synchronized StatementBuffer initialValue() {

            StatementBuffer buffer = new StatementBuffer(db, bufferCapacity);
            
            if (buffers != null) {

                buffers.add(buffer);

            }
            
            return buffer;

        }

    };
    
    /**
     * Create and run a concurrent data load operation.
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
     */
    public ConcurrentDataLoader(AbstractTripleStore db, int nthreads, int bufferCapacity,
            File file, FilenameFilter filter) {
        
        this(db, nthreads, bufferCapacity, file, filter, ""/* baseURL */,
                null/* fallback */, false/* autoFlush */, 1/* nclients */, 0/* clientNum */);

    }
    
    /**
     * Create and run a concurrent data load operation.
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
     * @param baseURL
     *            The baseURL and <code>""</code> if none is required.
     * @param fallback
     *            An attempt will be made to determine the interchange syntax
     *            using {@link RDFFormat}. If no determination can be made then
     *            the loader will presume that the files are in the format
     *            specified by this parameter (if any). Files whose format can
     *            not be determined will be logged as errors.
     * @param autoFlush
     *            When <code>true</code> the {@link StatementBuffer}s are
     *            flushed as each file is loaded. When <code>false</code> the
     *            buffers are reused by each file loaded by the same thread,
     *            flush when they overflow, and are explictly flushed once no
     *            more files remain to be loaded.
     * @param nclients
     *            The #of client processes that will share the data load
     *            process. Each client process MUST be started independently in
     *            its own JVM. All clients MUST have access to the files to be
     *            loaded.
     * 
     * @param clientNum
     *            The client host identifier in [0:nclients-1]. The clients will
     *            load files where
     *            <code>filename.hashCode() % nclients == clientNum</code>.
     *            If there are N clients loading files using the same pathname
     *            to the data then this will divide the files more or less
     *            equally among the clients. (If the data to be loaded are
     *            pre-partitioned then you do not need to specify either
     *            <i>nclients</i> or <i>clientNum</i>.)
     */
    public ConcurrentDataLoader(AbstractTripleStore db, int nthreads,
            int bufferCapacity, File file, FilenameFilter filter,
            String baseURL, RDFFormat fallback, boolean autoFlush,
            int nclients, int clientNum) {

        this.db = db;

        this.nthreads = nthreads;
        
        this.bufferCapacity = bufferCapacity;

        this.baseURL = baseURL;
        
        this.fallback = fallback;

        this.autoFlush = autoFlush;

        this.nclients = nclients;
        
        this.clientNum = clientNum;
        
        if (autoFlush) {

            buffers = null;
            
        } else {

            /*
             * A collection of buffers that we need to flush once the main load
             * is over.
             */
            buffers = Collections.synchronizedSet(new HashSet<StatementBuffer>(
                    nthreads + nthreads << 2));
            
        }
        
        /*
         * Setup the load service. We will run the tasks that read the data and
         * load it into the database on this service.
         * 
         * Note: we limit the #of tasks waiting in the queue so that we don't
         * let the file scan get too far ahead of the executing tasks. This
         * reduces the latency for startup and the memory overhead significantly
         * when reading a large collection of files.  There is a minimum queue
         * size so that we can be efficient for the file system reads.
         */
        
//        loadService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
//                nthreads, DaemonThreadFactory.defaultThreadFactory());
        
        final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(
                Math.max(100, nthreads * 2));
        
        loadService = new ThreadPoolExecutor(nthreads, nthreads,
                Integer.MAX_VALUE, TimeUnit.NANOSECONDS, queue,
                DaemonThreadFactory.defaultThreadFactory()); 

        try {

            /*
             * Scans the files, creating LoadTasks and then awaits completion of
             * the load tasks. The thread pool is shutdown once all tasks have
             * been run and some statistics are reported.
             */
            
            process(file, filter);

        } catch (Throwable t) {

            /*
             * Some uncorrectable problem during data load.
             */
            log.fatal(t);

            try {

                // immediate shutdown.
                loadService.shutdownNow();

                // wait for shutdown.
                while(!loadService.awaitTermination(2L, TimeUnit.SECONDS)) {
                    
                    log.warn("Shutdown taking too long.");
                    
                }
                
            } catch (Throwable t2) {
                
                log.warn("Problems during shutdown: " + t2);
                
            }

            // rethrow the exception.
            throw new RuntimeException(t);

        }

    }

    /**
     * Scans file(s) recursively starting with the named file, creates a
     * {@link LoadTask} for each file that passes the filter and then awaits
     * completion of the {@link LoadTask}s. The thread pool is shutdown once
     * all tasks have been run and some statistics are reported.
     * 
     * @param file
     *            Either a plain file or directory containing files to be
     *            processed.
     * @param filter
     *            An optional filter.
     * 
     * @throws IOException
     */
    private void process(File file, FilenameFilter filter) throws IOException,
            InterruptedException {

        final long begin = System.currentTimeMillis();

        process2(file, filter);

        log.info("All files scanned: nscanned="+nscanned+", ntasked=" + ntasked);

        // normal termination - all queued tasks will complete.
        loadService.shutdown();

        /*
         * wait for the load tasks to complete.
         */
        while (true) {

            log.info("Awaiting termination: completed="
                    + loadService.getCompletedTaskCount() + ", active="
                    + loadService.getActiveCount() + ", remaining="
                    + loadService.getQueue().size());

            if (loadService.awaitTermination(60L/* 1 minute */,
                    TimeUnit.SECONDS)) {

                log.info("Load tasks terminated normally.");
                
                break;

            }

        }

        /*
         * Flush all statement buffers through to the database.
         */
        if (!autoFlush) {

            final long beginFlush = System.currentTimeMillis();
            
            // #of buffers to be flushed.
            final int nflush = this.buffers.size();
            
            // array of all statement buffers.
            StatementBuffer[] buffers = this.buffers.toArray(new StatementBuffer[nflush]);

            log.info("Flushing "+nflush+" buffers to the database");
            
            final ThreadPoolExecutor tmpService = (ThreadPoolExecutor)Executors.newFixedThreadPool(nflush);

            final List<Future> futures = new ArrayList<Future>(nflush);
            
            for(StatementBuffer buffer : buffers) {
                
                final StatementBuffer b = buffer;
                
                futures.add(tmpService.submit(new Runnable() {
                    public void run() {
                        // flush the statement buffer when the task runs.
                        log.info("Flushing "+b.size()+" statements to the database.");
                        b.flush();
                    }
                }));
                
            }

            // normal shutdown.
            tmpService.shutdown();
            
            // wait for the flush tasks to complete.
            while (true) {

                log.info("Awaiting termination: completed="
                        + tmpService.getCompletedTaskCount() + ", active="
                        + tmpService.getActiveCount()+ ", remaining="
                        + tmpService.getQueue().size());

                if (tmpService.awaitTermination(60L/* 1 minute */,
                        TimeUnit.SECONDS)) {

                    log.info("Flush tasks terminated normally.");

                    break;

                }

            }

            /*
             * Look for errors in the flush tasks.
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

                        log.warn("Error: " + ex, ex);

                    }

                }

            }
            
            final long elapsedFlush = System.currentTimeMillis() - beginFlush;

            log.info("Finished flush tasks: #flushed=" + nflush
                    + " buffers in " + elapsedFlush + " ms (#threads=" + nthreads
                    + ", largestPoolSize=" + tmpService.getLargestPoolSize()
                    + ", bufferCapacity=" + bufferCapacity + ", #done=" + ndone
                    + ", #ok=" + nok + ", #err=" + nerr + ")");
        
        }

        /*
         * commit the database.
         */
        {

            log.info("Doing commit.");

            final long beginCommit = System.currentTimeMillis();
            
            db.commit();
            
            final long elapsedCommit = System.currentTimeMillis() - beginCommit;
            
            log.info("Commit latency=" + elapsedCommit + "ms");
            
        }

        /*
         * Examine the futures from the load tasks and report any errors.
         */
        {
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

            // total run time.
            final long elapsed = System.currentTimeMillis() - begin;

            final long nterms = db.getTermCount();

            final long nstmts = db.getStatementCount();
            
            final double tps = (long) (((double) nstmts) / ((double) elapsed) * 1000d);

            log.info("All done: #loaded=" + ntasked + " files in " + elapsed
                    + " ms, #terms=" + nterms + ", #stmts=" + nstmts
                    + ", rate=" + tps + " (#threads=" + nthreads + ", class="
                    + db.getClass().getSimpleName() + ", largestPoolSize="
                    + loadService.getLargestPoolSize() + ", bufferCapacity="
                    + bufferCapacity + ", autoFlush=" + autoFlush + ", #done="
                    + ndone + ", #ok=" + nok + ", #err=" + nerr + ")");

        }
        
    }

    /**
     * Scan the files, creating the tasks to be run and submitting them to the
     * {@link #loadService}.
     * 
     * @param file
     * @param filter
     * 
     * @throws InterruptedException
     *             if the thread is interrupted while queuing tasks.
     */
    private void process2(File file, FilenameFilter filter) throws InterruptedException {

        if (file.isDirectory()) {

            log.info("Scanning directory: " + file);

            File[] files = filter == null ? file.listFiles() : file
                    .listFiles(filter);

            for (File f : files) {

                process2(f, filter);

            }

        } else {

            /*
             * Processing a standard file.
             */

            log.info("Scanning file: " + file);

            nscanned.incrementAndGet();

            if(nclients>1) {

                /*
                 * More than one client will run so we need to allocate the
                 * files fairly to each client.
                 * 
                 * FIXME This is done by the #of files scanned modulo the #of
                 * clients. When that expression evaluates to the [clientNum]
                 * then the file will be allocated to this client. (The problem
                 * with this approach is that it is sensitive to the order in
                 * which the files are visited in the file systems. The
                 * hash(filename) approach is much more robust as long as the
                 * same pathname is used, perhaps an absolute pathname. I need
                 * to review this in practice since I have seen what appeared to
                 * be a strong bias in favor of one client when scanning the
                 * U1000 dataset on server1 and server2.)
                 */ 
                 /* 
//                 * This trick allocates files to clients based on the hash of the
//                 * pathname module the #of clients. If that expression does not
//                 * evaluate to the assigned clientNum then the file will NOT be
//                 * loaded by this host.
                 */
                    
                if ((nscanned.get() /* file.getPath().hashCode() */% nclients == clientNum)) {

                    log.info("Client" + clientNum + " tasked: " + file);

                    submitTask(file);

                }   
            
            } else {
                
                /*
                 * Only one client so it loads all of the files.
                 */

                submitTask(file);

            }
                
        }

    }

    /**
     * Submits a task to the {@link #loadService} to load the file into the
     * database.
     * 
     * @param file
     * 
     * @throws InterruptedException
     */
    private void submitTask(File file) throws InterruptedException {
        
        final LoadTask task = new LoadTask(file);

        while (true) {

            try {

                final Future f = loadService.submit(task);

                futures.add( f );

                ntasked.incrementAndGet();
                
                break;
                
            } catch (RejectedExecutionException ex) {

                log.info("loadService queue full"//
                        + ": queueSize="+ loadService.getQueue().size()//
                        + ", poolSize" + loadService.getPoolSize()//
                        + ", active="+ loadService.getActiveCount()//
                        + ", tasked="+ ntasked //
                        + ", completed="+ loadService.getCompletedTaskCount()//
                        );
                
                Thread.sleep(100/*ms*/);
                
            }

        }

    }
    
    /**
     * Tasks loads a single file.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class LoadTask implements Runnable {

        final File file;

        public LoadTask(File file) {

            this.file = file;

        }

        public void run() {

            log.info("Processing file: " + file);

            final RDFFormat rdfFormat = fallback == null //
                    ? RDFFormat.forFileName(file.getName()) //
                    : RDFFormat.forFileName(file.getName(), fallback)//
                    ;

            if (rdfFormat == null) {

                throw new RuntimeException(
                        "Could not determine interchange syntax: " + file);

            }

            try {

                loadData2(file.toString(), baseURL, rdfFormat);

            } catch (IOException e) {

                log.error("file=" + file + ", error=" + e);

                throw new RuntimeException(e);

            }

        }

        /**
         * Load an RDF resource into the database.
         */
        protected LoadStats loadData2(String resource, String baseURL,
                RDFFormat rdfFormat) throws IOException {

            final long begin = System.currentTimeMillis();

            StatementBuffer buffer = getStatementBuffer();
            
            LoadStats stats = new LoadStats();

            log.info("loading: " + resource);

            PresortRioLoader loader = new PresortRioLoader(buffer);

            if (!autoFlush) {

                /*
                 * disable auto-flush - caller will handle flush of the buffer.
                 */
                
                loader.setFlush(false);
                
            }

            /*
             * Note: The data logged by this listener reflects the throughput of
             * the parser only (vs the throughput to the database, which is
             * always much lower).
             */

            if (false) {

                loader.addRioLoaderListener(new RioLoaderListener() {

                    public void processingNotification(RioLoaderEvent e) {

                        log.info("parser: file=" + file + ", "
                                + e.getStatementsProcessed()
                                + " stmts parsed in "
                                + (e.getTimeElapsed() / 1000d)
                                + " secs, rate= " + e.getInsertRate());

                    }

                });
                
            }

            // open reader on the file.
            final InputStream rdfStream = new FileInputStream(resource);

            /*
             * Obtain a buffered reader on the input stream.
             * 
             * @todo reuse the backing buffer to minimize heap churn.
             */ 
            final Reader reader = new BufferedReader(new InputStreamReader(rdfStream)
            //                       , 20*Bytes.kilobyte32 // use a large buffer (default is 8k)
            );

            try {

                // run the parser.
                loader.loadRdf(reader, baseURL, rdfFormat, verifyData);

                long nstmts = loader.getStatementsAdded();

                stats.toldTriples = nstmts;

                stats.loadTime = System.currentTimeMillis() - begin;
                
                stats.totalTime = System.currentTimeMillis() - begin;

                /*
                 * This reports the load rate for the file, but this will only
                 * be representative of the real throughput if autoFlush is
                 * enabled (that is, if the statements for each file are flushed
                 * through to the database when that file is processed rather
                 * than being accumulated in a thread-local buffer).
                 */
                log.info(stats.toString());

                return stats;

            } catch (Exception ex) {

                log.error("file="+file+" : "+ex, ex);
                
                /*
                 * Note: discard anything in the buffer when auto-flush is
                 * enabled. This prevents the buffer from retaining data after a
                 * failed load operation.
                 * 
                 * @todo The caller must still handle the thrown exception by
                 * discarding the writes already on the backing store. Since
                 * this is a bulk load utility, that generally means dropping
                 * the database.
                 * 
                 * @todo We can't just clear the buffer if we have turned off
                 * auto-flush since that will discard buffer statements from the
                 * previous file processed by this thread. Proper error handling
                 * probably requires either discarding the entire bulk load,
                 * re-trying the failed file(s), or accepting that the data load
                 * may be incomplete (could work for some scenarios).
                 */

                if (autoFlush) {

                    buffer.clear();

                }

                throw new RuntimeException("While loading: " + resource, ex);

            } finally {

                reader.close();

                rdfStream.close();

            }

        }

    };

}
