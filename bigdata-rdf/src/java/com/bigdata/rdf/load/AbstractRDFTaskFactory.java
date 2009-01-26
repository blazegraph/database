package com.bigdata.rdf.load;

import java.io.File;
import java.net.MalformedURLException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Factory for tasks for loading RDF resources into a database or validating
 * RDF resources against a database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo report the #of resources processed in each case.
 */
public class AbstractRDFTaskFactory<T extends Runnable> implements
        ITaskFactory<T> {

    protected static final Logger log = Logger
            .getLogger(RDFLoadTaskFactory.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * The database on which the data will be written.
     */
    final AbstractTripleStore db;

    /**
     * The timestamp set when {@link #notifyStart()} is invoked.
     */
    private long beginTime;

    /**
     * The timestamp set when {@link #notifyEnd()} is invoked.
     */
    private long endTime;
    
    /**
     * Notify that the factory will begin running tasks. This sets the
     * {@link #beginTime} used by {@link #elapsed()} to report the run time
     * of the tasks.
     */
    public void notifyStart() {
                    
        endTime = 0L;
        
        beginTime = System.currentTimeMillis();
        
    }

    /**
     * Notify that the factory is done running tasks (for now).  This
     * places a cap on the time reported by {@link #elapsed()}.
     * 
     * @todo Once we are done loading data the client should be told to
     *       flush its counters to the load balancer so that we have the
     *       final state snapshot once it is ready.
     */
    public void notifyEnd() {
        
        endTime = System.currentTimeMillis();

        assert beginTime <= endTime;
        
    }
    
    /**
     * The elapsed time, counting only the time between
     * {@link #notifyStart()} and {@link #notifyEnd()}.
     */
    public long elapsed() {

        if (endTime == 0L) {

            // Still running.
            return System.currentTimeMillis() - beginTime;

        } else {

            // Done.

            final long elapsed = endTime - beginTime;

            assert elapsed >= 0L;

            return elapsed;

        }
        
    }

    /**
     * An attempt will be made to determine the interchange syntax using
     * {@link RDFFormat}. If no determination can be made then the loader
     * will presume that the files are in the format specified by this
     * parameter (if any). Files whose format can not be determined will be
     * logged as errors.
     */
    final RDFFormat fallback;

    /**
     * Validation of RDF by the RIO parser is disabled unless this is true.
     */
    final boolean verifyData;

    /**
     * Delete files after successful processing when <code>true</code>.
     */
    final boolean deleteAfter;

    final IStatementBufferFactory bufferFactory;

    /**
     * #of told triples loaded into the database by successfully completed {@link ReaderTask}s.
     */
    final AtomicLong toldTriples = new AtomicLong(0);

    /**
     * Guess at the {@link RDFFormat}.
     * 
     * @param filename
     *            Some filename.
     * 
     * @return The {@link RDFFormat} -or- <code>null</code> iff
     *         {@link #fallback} is <code>null</code> and the no format
     *         was recognized for the <i>filename</i>
     */
    public RDFFormat getRDFFormat(String filename) {

        final RDFFormat rdfFormat = //
        fallback == null //
        ? RDFFormat.forFileName(filename) //
                : RDFFormat.forFileName(filename, fallback)//
        ;

        return rdfFormat;

    }

    protected AbstractRDFTaskFactory(AbstractTripleStore db,
            final boolean verifyData, final boolean deleteAfter,
            RDFFormat fallback, IStatementBufferFactory bufferFactory) {

        this.db = db;
        
        this.verifyData = verifyData;

        this.deleteAfter = deleteAfter;
        
        this.fallback = fallback;
        
        this.bufferFactory = bufferFactory;
        
    }

    public T newTask(final String resource) throws Exception {
        
        if(INFO)
            log.info("resource="+resource);
        
        final RDFFormat rdfFormat = getRDFFormat( resource );
        
        if (rdfFormat == null) {

            throw new RuntimeException(
                    "Could not determine interchange syntax - skipping : file="
                            + resource);

        }

        // Convert the file path to a URL.
        final String baseURL;
        try {

            baseURL = new File(resource).toURL().toString();

        } catch (MalformedURLException e) {

            throw new RuntimeException("resource=" + resource);

        }
        
        return (T) new ReaderTask(resource, baseURL, rdfFormat, verifyData,
                deleteAfter, bufferFactory, toldTriples);
        
    }
    
}