package com.bigdata.rdf.load;

import java.io.File;
import java.io.FilenameFilter;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;

/**
 * Class for concurrent loading of files from a local file system or hash
 * partitioned loading of files from a shared volume.
 */
public class FileSystemLoader {
    
    protected static final Logger log = Logger.getLogger(FileSystemLoader.class);
    
    /**
     * The #of files scanned.
     */
    protected final AtomicInteger nscanned = new AtomicInteger(0);
    
    public int getScanCount() {
        
        return nscanned.get();
        
    }
    
    protected final ConcurrentDataLoader loader;
    
    /**
     * The #of clients when running as a distributed client (more than one
     * {@link ConcurrentDataLoader} instance, typically on more than one host).
     */
    protected final int nclients;
    
    /**
     * The identified assigned to this client in the half-open interval [0:{@link #nclients}).
     */
    protected final int clientNum;

    /**
     * Ctor used to load files from the local file system.
     * 
     * @param loader
     *            The {@link ConcurrentDataLoader}.
     */
    public FileSystemLoader(final ConcurrentDataLoader loader) {
        
        this(loader, 1/* nclients */, 0/* clientNum */);
        
    }
    
    /**
     * Ctor used when multiple clients will load files from a shared volume.
     * Each client will only process those files which are assigned to it by
     * hash partitioning so the clients will each do roughly the same amount
     * of work.
     * <p>
     * Note: The {@link File} is normalized using
     * {@link File#getAbsoluteFile()} when using more than one client. The
     * hash of the normalized path is used to determine which client will
     * load a given file. The absolute path is more robust than the
     * canonical path since the latter can reveal mount points and other
     * differences which the system administrator might be trying to hide.
     * However you should still verify that all files are being loaded per
     * expectations and that none are slipping through. For example, if one
     * host is Windows and another is Linux then the hashed paths might be
     * different and files might not be selected for loading by any client
     * or might be selected for loading by more than one client.
     * 
     * @param loader
     *            The {@link ConcurrentDataLoader}
     * @param nclients
     *            The #of client processes that will share the data load
     *            process. Each client process MUST be started independently
     *            in its own JVM. All clients MUST have access to the files
     *            to be loaded.
     * @param clientNum
     *            The client host identifier in [0:nclients-1]. The clients
     *            will load files where
     *            <code>filename.hashCode() % nclients == clientNum</code>.
     *            If there are N clients loading files using the same
     *            pathname to the data then this will divide the files more
     *            or less equally among the clients. (If the data to be
     *            loaded are pre-partitioned then you do not need to specify
     *            either <i>nclients</i> or <i>clientNum</i>.)
     * 
     */
    public FileSystemLoader(final ConcurrentDataLoader loader,
            final int nclients, final int clientNum) {

        if (loader == null)
            throw new IllegalArgumentException();

        if (nclients < 1)
            throw new IllegalArgumentException();

        if (clientNum < 0 || clientNum >= nclients)
            throw new IllegalArgumentException();

        this.loader = loader;
        
        this.nclients = nclients;
        
        this.clientNum = clientNum;
        
    }

    /**
     * Scans file(s) recursively starting with the named file, creates a
     * task using the {@link ITaskFactory} for each file that passes the
     * filter, and submits the task. When <i>file</i> is a directory, the
     * method returns once all file(s) in the directory and its children
     * have been submitted for processing.
     * 
     * @param file
     *            Either a plain file or directory containing files to be
     *            processed.
     * @param filter
     *            An optional filter.
     * @param taskFactory
     * 
     * @throws InterruptedException
     *             if the thread is interrupted while queuing tasks.
     */
    public void process(final File file, final FilenameFilter filter,
            final ITaskFactory taskFactory) throws InterruptedException {

        if (file == null)
            throw new IllegalArgumentException();

        if (taskFactory == null)
            throw new IllegalArgumentException();

        process2(file, filter, taskFactory);

    }
    
    private void process2(final File file, final FilenameFilter filter,
            final ITaskFactory taskFactory) throws InterruptedException {

        if (file.isDirectory()) {

            if (log.isInfoEnabled())
                log.info("Scanning directory: " + file);

            final File[] files = filter == null ? file.listFiles() : file
                    .listFiles(filter);

            for (final File f : files) {

                process2(f, filter, taskFactory);

            }

        } else {

            /*
             * Processing a standard file.
             */

            if (log.isInfoEnabled())
                log.info("Scanning file: " + file);

            try {

                submitFile(file, taskFactory);
                
            } catch (Exception ex) {
                
                log.error(file, ex);
                
            }

        }

    }

    // @todo setupCounters
    private void setupCounters() {

        CounterSet ourCounterSet = null;
        
        ourCounterSet.addCounter("#clients",
                new OneShotInstrument<Integer>(nclients));

        ourCounterSet.addCounter("clientNum",
                new OneShotInstrument<Integer>(clientNum));

        ourCounterSet.addCounter("#scanned", new Instrument<Long>() {

            @Override
            protected void sample() {

                setValue((long) nscanned.get());

            }
        });

    }
    
    /**
     * Submit the file for loading by this client iff its path is selected
     * by hash partitioning.
     * 
     * @param file
     * @param taskFactory
     * 
     * @throws InterruptedException
     */
    protected void submitFile(final File file,
            final ITaskFactory taskFactory) throws InterruptedException {
        
        if (nclients > 1) {

            /*
             * More than one client will run so we need to allocate the
             * files fairly to each client.
             * 
             * This trick allocates files to clients based on the hash of
             * the pathname module the #of clients. If that expression does
             * not evaluate to the assigned clientNum then the file will NOT
             * be loaded by this host.
             */
            
            final String normalizedPath = file.getAbsolutePath();
            
            if ((normalizedPath.hashCode() % nclients != clientNum)) {

                // will be handled by another client.
                return;
         
            }
            
        }

        if (log.isInfoEnabled())
            log.info("client#=" + clientNum + ", #scanned=" + nscanned
                    + ", file=" + file);

        try {

            loader.submitTask(file.toString(), taskFactory);
            
        } catch (Exception e) {
            
            log.error("Will not load: " + file, e);
            
        }

    }
    
}
