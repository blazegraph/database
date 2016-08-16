package com.bigdata.rdf.sparql.ast.service.history;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KVO;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.changesets.ChangeAction;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sparql.ast.eval.CustomServiceFactoryBase;
import com.bigdata.rdf.sparql.ast.service.BigdataNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.AbstractRelation;

/**
 * This service tracks KB updates via an {@link IChangeLog} and is responsible
 * for maintaining an ordered index over the assertions that have been added to
 * or removed from a KB instance.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/607"> History
 *      Service</a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HistoryServiceFactory extends CustomServiceFactoryBase {

    static private transient final Logger log = Logger
            .getLogger(HistoryServiceFactory.class);

    private final BigdataNativeServiceOptions serviceOptions;

    public HistoryServiceFactory() {

        serviceOptions = new BigdataNativeServiceOptions();
        
        /*
         * TODO Review decision to make this a runFirst service. The rational is
         * that this service can only apply a very limited set of restrictions
         * during query, therefore it will often make sense to run it first.
         * However, the fromTime and toTime could be bound by the query and the
         * service can filter some things more efficiently internally than if we
         * generated a bunch of intermediate solutions for those things.
         */
        serviceOptions.setRunFirst(true);
        
    }

    @Override
    public IServiceOptions getServiceOptions() {

        return serviceOptions;

    }

    /**
     * TODO Implement: Query should support an index scan of a date range with
     * optional filters on the (s,p,o,c) and add/remove flags. It might make
     * more sense to index in (POS) order rather than SPO order so we can more
     * efficiently scan a specific predicate within some date range using an
     * advancer pattern.
     * <p>
     * The restrictions that this service COULD apply to the index scan are:
     * <dl>
     * <dt>fromTime</dt>
     * <dd>Inclusive lower bound.</dd>
     * <dt>toTime</dt>
     * <dd>Exclusive upper bound (e.g., the first commit point NOT to be
     * reported).</dd>
     * <dt>P</dt>
     * <dd>The {@link IV} for the predicate (this is the first statement key
     * component in the history index for both triples and quads mode KBs)</dd>
     * </dl>
     * In addition, it could filter on the remaining fields (that is, skip over
     * tuples that fail a filter):
     * <dl>
     * <dt>S, O [, C]</dt>
     * <dd>The {@link IV} for the subject, object, and (for quads mode, the
     * context).</dd>
     * <dt>action</dt>
     * <dd>The {@link ChangeAction}.</dd>
     * <dt>type</dt>
     * <dd>The {@link StatementTypeEnum}.</dd>
     * </dl>
     */
    @Override
    public ServiceCall<?> create(final ServiceCallCreateParams params) {

        throw new UnsupportedOperationException();

    }

    /**
     * Register an {@link IChangeLog} listener that will manage the maintenance
     * of the describe cache.
     */
    @Override
    public void startConnection(final BigdataSailConnection conn) {

//        final Properties properties = conn.getProperties();
        final AbstractTripleStore tripleStore = conn.getTripleStore();

        if (Boolean.valueOf(tripleStore.getProperty(
                BigdataSail.Options.HISTORY_SERVICE,
                BigdataSail.Options.DEFAULT_HISTORY_SERVICE))) {

            conn.addChangeLog(new HistoryChangeLogListener(conn));

        }

    }

    /**
     * Handles maintenance of the history index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static private class HistoryChangeLogListener implements IChangeLog {

        /** The vector size for updates. */
        private static final int threshold = 10000;
        /** The connection. */
        private final BigdataSailConnection conn;
        /** The KB instance. */
        private final AbstractTripleStore tripleStore;
        /**
         * The head of the index is pruned on update to remove entries that are
         * older than this age (milliseconds).
         */
        private final long minReleaseAge;
        /**
         * The first timestamp that WILL NOT be released.
         */
        private final long releaseTime;
        /**
         * The timestamp that will be associated with the {@link IChangeLog}
         * events in the index.
         */
        private volatile long revisionTimestamp;
        /** The set of IVs to be invalidated (lazily instantiated). */
        private Map<ISPO, IChangeRecord> changeSet;
        /** The history index. */
        private IIndex ndx = null;

        HistoryChangeLogListener(final BigdataSailConnection conn) {

            this.conn = conn;

            this.tripleStore = conn.getTripleStore();

            this.revisionTimestamp = getRevisionTimestamp(tripleStore);
            
            this.minReleaseAge = Long
                    .valueOf(tripleStore
                            .getProperty(
                                    BigdataSail.Options.HISTORY_SERVICE_MIN_RELEASE_AGE,
                                    BigdataSail.Options.DEFAULT_HISTORY_SERVICE_MIN_RELEASE_AGE));

            /*
             * TODO We should be able to reach the timestamp service from the
             * index manager. We want to use the globally agreed on clock for
             * the current time when making the decision to prune the head of
             * the index.
             */

            releaseTime = (System.currentTimeMillis() - minReleaseAge) + 1;

            if (log.isInfoEnabled()) {
                log.info("minReleaseAge=" + minReleaseAge + ", releaseTime="
                        + releaseTime);
            }

        }

        /**
         * Return the revision time that will be used for all changes written
         * onto the history index by this {@link IChangeLog} listener.
         * 
         * @see HistoryChangeRecord#getRevisionTime()
         */
        static private long getRevisionTimestamp(
                final AbstractTripleStore tripleStore) {

            final long revisionTimestamp;
            
            final IIndexManager indexManager = tripleStore.getIndexManager();

            revisionTimestamp = indexManager.getLastCommitTime() + 1;

//            if (indexManager instanceof IJournal) {
//
//                revisionTimestamp = indexManager.getLastCommitTime() + 1;
////                        ((IJournal) indexManager)
////                        .getLocalTransactionManager().nextTimestamp();
//                
//            } else if (indexManager instanceof IBigdataFederation) {
//                
//                try {
//                
//                    revisionTimestamp = ((IBigdataFederation<?>) indexManager)
//                            .getTransactionService().nextTimestamp();
//                    
//                } catch (IOException e) {
//                    
//                    throw new RuntimeException(e);
//                    
//                }
//
//            } else {
//            
//                throw new AssertionError("indexManager="
//                        + indexManager.getClass());
//                
//            }

            return revisionTimestamp;
        }

        @Override
        public void transactionBegin() {

            this.revisionTimestamp = getRevisionTimestamp(tripleStore);
            
        }

        @Override
        public void transactionPrepare() {

            flush();
            
        }

        /**
         * Vectors updates against the DESCRIBE cache.
         */
        @Override
        public void changeEvent(final IChangeRecord record) {

            if (changeSet == null) {

                // Lazy instantiation.
                changeSet = new HashMap<ISPO, IChangeRecord>();

                // Get the history index.
                ndx = getHistoryIndex(tripleStore);
                
                if (minReleaseAge > 0) {

                    pruneHistory();
                    
                }
                
            }

            final ISPO spo = record.getStatement();

            changeSet.put(spo, record);

            if (changeSet.size() > threshold) {

                flush();

            }

        }

        /**
         * Return the pre-existing history index.
         * 
         * @param tripleStore
         *            The KB.
         * @return The history index and never <code>null</code>.
         * 
         * @throws IllegalStateException
         *             if the index was not configured / does not exist.
         */
        private IIndex getHistoryIndex(final AbstractTripleStore tripleStore) {

            final SPORelation spoRelation = tripleStore.getSPORelation();

            final String fqn = AbstractRelation.getFQN(spoRelation,
                    SPORelation.NAME_HISTORY);

            ndx = spoRelation.getIndex(fqn);

            if (ndx == null)
                throw new IllegalStateException("Index not found: " + fqn);
            
            return ndx;

        }
        
        /**
         * Prune the head of the history index.
         * <p>
         * Note: Either this should be done as the first action or you must make
         * a note of the effective release time as the first action and then
         * apply that effective release time later. If you instead compute and
         * apply the effective release time later on, then there is the
         * possibility that you could prune out entries from the current
         * transaction!
         */
        private void pruneHistory() {

            final IKeyBuilder keyBuilder = ndx.getIndexMetadata().getKeyBuilder().reset();
            
            keyBuilder.append(releaseTime);
            
            final byte[] toKey = keyBuilder.getKey();

            long n = 0;
            
            final ITupleIterator<?> itr = ndx
                    .rangeIterator(null/* fromKey */, toKey,
                            0/* capacity */,
                            IRangeQuery.REMOVEALL/* flags */, null/* filterCtor */);
            
            while(itr.hasNext()) {
                
                itr.next();
                
                n++;
                
            }

            if (n > 0 && log.isInfoEnabled()) {
                log.info("pruned history: nremoved=" + n + ", minReleaseAge="
                        + minReleaseAge + ", releaseTime=" + releaseTime);
            }

        }
        
        @Override
        public void transactionCommited(long commitTime) {

            flush();

        }

        @Override
        public void transactionAborted() {

            reset();

        }

        /**
         * See {@link IChangeLog#close()}.
         */
        @Override
        public void close() {
            reset();
        }

        /** Reset the buffer. */
        private void reset() {

            changeSet = null;

        }

        /**
         * Incremental flush.
         */
        private void flush() {

            if (changeSet != null) {

                final int size = changeSet.size();

                final KVO<HistoryChangeRecord>[] b = new KVO[size];
                {
                    // Extract the new change records into an array.
                    final IChangeRecord[] a = changeSet.values().toArray(
                            new IChangeRecord[size]);

                    final HistoryIndexTupleSerializer tupSer = (HistoryIndexTupleSerializer) ndx
                            .getIndexMetadata().getTupleSerializer();

                    // Wrap each one with the revision time.
                    for (int i = 0; i < size; i++) {

                        final IChangeRecord r = a[i];

                        // attach the revision time.
                        final HistoryChangeRecord s = new HistoryChangeRecord(
                                r, revisionTimestamp);

                        final byte[] key = tupSer.serializeKey(s);

                        final byte[] val = tupSer.serializeVal(s);

                        b[i] = new KVO<HistoryChangeRecord>(key, val, s);

                    }
                    
                }

                // Sort to improve the index locality.
                java.util.Arrays.sort(b);

                // Write on the indices.
                for (int i = 0; i < size; i++) {

                    final KVO<HistoryChangeRecord> r = b[i];

                    ndx.insert(r.key, r.val);

                }

                reset();

            }

        }

    } // class HistoryChangeLogListener

} // class HistoryServiceFactory
