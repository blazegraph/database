package com.bigdata.rdf.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RDFParserOptions;

/**
 * Tasks either loads a RDF resource or verifies that the told triples found
 * in that resource are present in the database. The difference between data
 * load and data verify is just the behavior of the {@link IStatementBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SingleResourceReaderTask implements Runnable {

    protected static final Logger log = Logger.getLogger(SingleResourceReaderTask.class);

    /**
     * The resource to be loaded.
     */
    final String resource;
    
    /**
     * The base URL for that resource.
     */
    final String baseURL;
    
    /**
     * The RDF interchange syntax that the file uses.
     */
    final RDFFormat rdfFormat;

    /**
     * Validate the RDF interchange syntax when <code>true</code>.
     */
    final RDFParserOptions parserOptions;
    
    /**
     * Delete files after they have been successfully loaded when
     * <code>true</code>.
     */
    final boolean deleteAfter;
    
    final IStatementBufferFactory bufferFactory;
    
    final AtomicLong toldTriples;
    
    /**
     * The time when the task was first created.
     */
    final long createTime;
    
    public String toString() {
        
        return "LoadTask"//
        +"{ resource="+resource
        +", elapsed="+(System.currentTimeMillis()-createTime)//
        +"}"//
        ;
        
    }
    
    /**
     * 
     * Note: Updates to <i>toldTriples</i> MUST NOT occur unless the task
     * succeeds, otherwise tasks which error and then retry will cause
     * double-counting.
     * 
     * @param resource
     * @param baseURL
     * @param rdfFormat
     * @param verifyData
     * @param deleteAfter 
     * @param bufferFactory
     * @param toldTriples
     */
    public SingleResourceReaderTask(String resource, String baseURL, RDFFormat rdfFormat,
            final RDFParserOptions parserOptions, final boolean deleteAfter,
            IStatementBufferFactory bufferFactory, AtomicLong toldTriples) {

        if (resource == null)
            throw new IllegalArgumentException();

        if (baseURL == null)
            throw new IllegalArgumentException();

        if (rdfFormat == null)
            throw new IllegalArgumentException();

        if (parserOptions == null)
            throw new IllegalArgumentException();
        
        if (bufferFactory == null)
            throw new IllegalArgumentException();

        if (toldTriples == null)
            throw new IllegalArgumentException();

        this.resource = resource;
        
        this.baseURL = baseURL;

        this.rdfFormat = rdfFormat;

        this.parserOptions = parserOptions;

        this.deleteAfter = deleteAfter;
        
        this.bufferFactory = bufferFactory;
        
        this.toldTriples = toldTriples;
        
        this.createTime = System.currentTimeMillis();
        
    }

    public void run() {

        final LoadStats loadStats;
        try {

            loadStats = readData();

        } catch (Exception e) {

            /*
             * Note: no stack trace and only a warning - we will either
             * retry or declare the input as filed.
             */
            log.warn("resource=" + resource + ", error=" + e);

            throw new RuntimeException("resource=" + resource + " : " + e, e);

        }
        
        // Note: IFF the task succeeds!
        toldTriples.addAndGet(loadStats.toldTriples.get());

    }

    /**
     * Reads an RDF resource and either loads it into the database or
     * verifies that the triples in the resource are found in the database.
     */
    protected LoadStats readData() throws Exception {

        final long begin = System.currentTimeMillis();

        // get buffer - determines data load vs database validate.
        final IStatementBuffer<Statement> buffer = bufferFactory.newStatementBuffer();
        
        // make sure that the buffer is empty.
        buffer.reset();
        
        if (log.isInfoEnabled())
            log.info("loading: " + resource);

        final PresortRioLoader loader = new PresortRioLoader(buffer);

        // open reader on the file.
        final InputStream rdfStream = new FileInputStream(resource);

        // Obtain a buffered reader on the input stream.
        final Reader reader = new BufferedReader(new InputStreamReader(
                rdfStream));

        boolean success = false;
        
        try {

            final LoadStats stats = new LoadStats();

            // run the parser.
            // @todo reuse the same underlying parser instance?
            loader.loadRdf(reader, baseURL, rdfFormat, parserOptions);

            success = true;
            
            final long nstmts = loader.getStatementsAdded();

            final long now = System.currentTimeMillis();
            
            stats.toldTriples.set(nstmts);

            stats.totalTime.set( now - begin );

            stats.loadTime.set( now - begin );
            
            /*
             * This reports the load rate for the file, but this will only
             * be representative of the real throughput if autoFlush is
             * enabled (that is, if the statements for each file are flushed
             * through to the database when that file is processed rather
             * than being accumulated in a thread-local buffer).
             */
            if (log.isInfoEnabled())
                log.info(stats.toString());

            return stats;

        } catch (Exception ex) {

            /*
             * Note: discard anything in the buffer. This prevents the
             * buffer from retaining data after a failed load operation.
             */
            buffer.reset();
            
            // rethrow the exception.
            throw ex;

        } finally {

            reader.close();

            rdfStream.close();

            if (deleteAfter && success) {

                if (!new File(resource).delete()) {

                    log.warn("Could not delete: " + resource);

                }

            }
            
        }

    }

}
