package com.bigdata.rdf.load;

import java.io.File;
import java.io.Serializable;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.counters.CounterSet;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.load.RDFDataLoadMaster.JobState;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.rio.AsynchronousStatementBufferFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.FederationCallable;
import com.bigdata.service.IRemoteExecutor;
import com.bigdata.service.jini.JiniFederation;

/**
 * Task reads files from the file system, loads them into an
 * {@link ITripleStore}, and optionally deletes the source files once they are
 * restart safe on the database. This is a non-transactional bulk load using
 * unisolated writes. This task may be used in conjunction with any process that
 * writes files into a known directory on the hosts of a cluster.
 * <p>
 * Note: Counters reporting the progress of this task will be attached to the
 * service within which this task is executing.
 * <p>
 * Note: When loading files from a <em>local</em> file system, this task can
 * not handle the death of the service on which it is running.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <S>
 *            The generic for the {@link JobState}.
 * @param <V>
 *            The generic type of the client state (stored in zookeeper).
 */
public class RDFFileLoadTask<S extends JobState, V extends Serializable>
        extends FederationCallable<Void> implements Serializable {

    final protected transient static Logger log = Logger
            .getLogger(RDFFileLoadTask.class);

    /**
     * 
     */
    private static final long serialVersionUID = 6787939197771556658L;

    protected final S jobState;

    protected final int clientNum;

    public String toString() {

        return getClass().getName() + "{clientNum=" + clientNum + "}";

    }

    public RDFFileLoadTask(final S jobState, final int clientNum) {

        this.jobState = jobState;

        this.clientNum = clientNum;

    }

    /**
     * The federation object used by the {@link IRemoteExecutor} on which this
     * task is executing.
     */
    public JiniFederation getFederation() {

        return (JiniFederation) super.getFederation();

    }

    public Void call() throws Exception {

        final JiniFederation fed = getFederation();

        final AbstractTripleStore tripleStore = (AbstractTripleStore) fed
                .getResourceLocator()
                .locate(jobState.namespace, ITx.UNISOLATED);

        if (tripleStore == null) {

            throw new RuntimeException("KB not found: namespace="
                    + jobState.namespace);

        }

        final AsynchronousStatementBufferFactory<BigdataStatement,File> statementBufferFactory = new AsynchronousStatementBufferFactory<BigdataStatement,File>(
                (ScaleOutTripleStore) tripleStore,//
                jobState.producerChunkSize,//
                jobState.valuesInitialCapacity,//
                jobState.bnodesInitialCapacity,//
                RDFFormat.RDFXML, // @todo defaultFormat is not serializable.
                jobState.parserValidates,//
                jobState.deleteAfter,//
                jobState.parserPoolSize, // parserPoolSize 
                jobState.parserQueueCapacity, // parserQueueCapacity
                jobState.term2IdWriterPoolSize,//
                jobState.otherWriterPoolSize,//
                jobState.notifyPoolSize,//
                jobState.unbufferedStatementThreshold
                );

        try {

            /*
             * Note: Add the counters to be reported to the client's counter
             * set. The added counters will be reported when the client reports
             * its own counters.
             */
            final CounterSet serviceRoot = fed.getServiceCounterSet();

            final String relPath = jobState.jobName;

            // Create path to CDL counter set.
            final CounterSet tmp = serviceRoot.makePath(relPath);

            tmp.attach(statementBufferFactory.getCounters(), true/*replace*/);

            // load file(s).
            loadData(statementBufferFactory);
            
            if (log.isInfoEnabled())
                log.info("Will await completion of loader tasks.");

            statementBufferFactory.awaitAll();

            if (log.isInfoEnabled())
                log.info("Done.");
                
        } catch (Throwable t) {

            log.error("Task failed: " + t, t);
            
            try {
                statementBufferFactory
                        .cancelAll(true/* mayInterruptIfRunning */);
            } catch (Throwable t2) {
                log.warn(this, t2);
            }

            throw new RuntimeException(t);

        }

        return null;

    }

    /**
     * Hook to load data is invoked once by {@link #call()}.
     * 
     * @throws InterruptedException
     *             if interrupted (job canceled).
     * @throws Exception
     *             if something else goes wrong.
     */
    protected void loadData(//
            final AsynchronousStatementBufferFactory<BigdataStatement, File> factory)
            throws InterruptedException, Exception {

        factory.submitAll(jobState.dataDir, jobState.dataDirFilter,
                jobState.rejectedExecutionDelay);

    }

}
