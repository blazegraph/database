package com.bigdata.rdf.load;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.rdf.rio.IAsynchronousWriteStatementBufferFactory;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.service.ILoadBalancerService;

/**
 * Factory for tasks for loading RDF resources into a database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFLoadTaskFactory<S extends Statement,T extends Runnable> extends
        AbstractRDFTaskFactory<S,T> {
    
    /**
     * 
     * @param db
     * @param bufferCapacity
     * @param verifyData
     * @param deleteAfter
     *            if the file should be deleted once it has been loaded.
     * @param fallback
     *            An attempt will be made to determine the interchange syntax
     *            using {@link RDFFormat}. If no determination can be made then
     *            the loader will presume that the files are in the format
     *            specified by this parameter (if any). Files whose format can
     *            not be determined will be logged as errors.
     * 
     * @todo drop the writeBuffer arg.
     */
    public RDFLoadTaskFactory(final AbstractTripleStore db,
            final int bufferCapacity, final boolean verifyData,
            final boolean deleteafter, final RDFFormat fallback) {

        this(db, verifyData, deleteafter, fallback,
                new LoadStatementBufferFactory<S>(db, bufferCapacity));

    }

    /**
     * 
     * @param db
     * @param verifyData
     * @param deleteAfter
     *            if the file should be deleted once it has been loaded.
     * @param fallback
     *            An attempt will be made to determine the interchange syntax
     *            using {@link RDFFormat}. If no determination can be made then
     *            the loader will presume that the files are in the format
     *            specified by this parameter (if any). Files whose format can
     *            not be determined will be logged as errors.
     * @param factory
     *            Used to buffer and load statements.
     */
    public RDFLoadTaskFactory(final AbstractTripleStore db,
            final boolean verifyData, final boolean deleteafter,
            final RDFFormat fallback, IStatementBufferFactory factory) {

        super(db, verifyData, deleteafter, fallback, factory);

    }

    /**
     * Sets up some additional counters for reporting by the client to the
     * {@link ILoadBalancerService}.
     * 
     * @todo in the base class also?
     */
    public CounterSet getCounters() {

        final CounterSet counterSet = new CounterSet();

        /*
         * Elapsed ms since the start of the load up to and until the end of the
         * load.
         */
        counterSet.addCounter("elapsed", new Instrument<Long>() {

            @Override
            protected void sample() {

                final long elapsed = elapsed();

                setValue(elapsed);

            }
        });

        /*
         * Note: This is the #of told triples read by _this_ client.
         * 
         * When you are loading using multiple instances of the concurrent data
         * loader, then the total #of told triples is the aggregation across all
         * of those instances.
         */
        counterSet.addCounter("toldTriplesLoaded", new Instrument<Long>() {

            @Override
            protected void sample() {

                setValue(toldTriples.get());

            }
        });

        /*
         * Note: This is the told triples per second rate for _this_ client only
         * since it is based on the triples read by the threads for this client.
         * 
         * When you are loading using multiple instances of the concurrent data
         * loader, then the total told triples per second rate is the
         * aggregation across all of those instances.
         */
        counterSet.addCounter("toldTriplesPerSec", new Instrument<Long>() {

            @Override
            protected void sample() {

                final long elapsed = elapsed();

                final double tps = (long) (((double) toldTriples.get())
                        / ((double) elapsed) * 1000d);

                setValue((long) tps);

            }
        });

        if (bufferFactory instanceof IAsynchronousWriteStatementBufferFactory) {

            counterSet
                    .attach(((IAsynchronousWriteStatementBufferFactory) bufferFactory)
                            .getCounters());

        }

        return counterSet;

    }
    
    /**
     * Report totals.
     * <p>
     * Note: these totals reflect the actual state of the database, not just
     * the #of triples written by this client. Therefore if there are
     * concurrent writers then the apparent TPS here will be higher than was
     * reported by the counters for just this client -- all writers on the
     * database will have been attributed to just this client.
     */
    public String reportTotals() {

        // total run time.
        final long elapsed = elapsed();

        final long nterms = db.getTermCount();

        final long nstmts = db.getStatementCount();

        final double tps = (long) (((double) nstmts) / ((double) elapsed) * 1000d);

        return "Database: #terms=" + nterms + ", #stmts=" + nstmts
                + ", rate=" + tps + " in " + elapsed + " ms.";
            
    }
    
}
