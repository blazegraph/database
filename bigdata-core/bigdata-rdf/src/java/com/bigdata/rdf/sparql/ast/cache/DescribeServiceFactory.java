package com.bigdata.rdf.sparql.ast.cache;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.rdf.changesets.ChangeAction;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sparql.ast.eval.CustomServiceFactoryBase;
import com.bigdata.rdf.sparql.ast.service.BigdataNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;

/**
 * This service tracks KB updates via an {@link IChangeLog} and is responsible
 * for DESCRIBE cache invalidation for resources for which an update has been
 * observed.
 * 
 * @see BD#DESCRIBE
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/584"> DESCRIBE
 *      cache </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DescribeServiceFactory extends CustomServiceFactoryBase {

    static private transient final Logger log = Logger
            .getLogger(DescribeServiceFactory.class);

    private final IServiceOptions serviceOptions;

    public DescribeServiceFactory() {

        this.serviceOptions = new BigdataNativeServiceOptions();

    }

    @Override
    public IServiceOptions getServiceOptions() {

        return serviceOptions;

    }

    /**
     * TODO Implement: The {@link DescribeServiceFactory} COULD be integrated
     * into query processing using a rewrite of a DESCRIBE or a star-join into
     * an invocation of this service.
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

        /**
         * TODO This really should not be using getCacheConnection() but rather
         * getExistingCacheConnection(). I need to figure out the pattern that
         * brings the cache connection into existence and who is responsible for
         * invoking it. The problem is that there are multiple entry points,
         * including AST evaluation, the DescribeServlet, and the test suite.
         * AST2BOpContext does this, but it is not always created before we need
         * the cache connection.
         */
        final ICacheConnection cacheConn = CacheConnectionFactory
                .getCacheConnection(conn.getBigdataSail().getQueryEngine());

        if (cacheConn == null) {

            // Cache is not enabled.
            return;

        }

        final AbstractTripleStore tripleStore = conn.getTripleStore();

        final IDescribeCache describeCache = cacheConn.getDescribeCache(
                tripleStore.getNamespace(), tripleStore.getTimestamp());

        if (describeCache == null) {

            // DESCRIBE cache is not enabled.
            return;

        }

        conn.addChangeLog(new DescribeCacheChangeLogListener(describeCache));

    }

    /**
     * Handles cache maintenance/invalidation.
     * <p>
     * There are several very different scenarios for cache maintenance:
     * <dl>
     * <dt>Invalidation only.</dt>
     * <dd>Changes for resources in the subject or object position cause the
     * resource to be invalidated in the cache.</dd>
     * <dt>Maintenance</dt>
     * <dd>The cache is actually a fully populated and maintained index. All
     * updates are propagated into the cache so it remains consistent with the
     * statement indices.</dd>
     * </dl>
     * In addition to these strategies, we could treat the cache as a partial
     * representation of the linked data available on the open web for the
     * resources and track metadata about the age of the resource description
     * for each linked data authority.
     * <p>
     * Another twist for invalidation would be to bound the cache capacity. That
     * would require us to also maintain metadata to support an eviction policy.
     * The easiest way to do that is to manage the raw journal entries on the
     * store and a LIRS/LRU eviction policy on the {@link IV}s, together with
     * the address of the raw record (Blob/Stream).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static private class DescribeCacheChangeLogListener implements IChangeLog {

        /** The vector size for updates. */
        private static final int threshold = 10000;
        /** An updatable view of the cache. */
        private final IDescribeCache cache;
        /** The set of IVs to be invalidated (lazily instantiated). */
        private Set<IV<?, ?>> ivs;
        /** The size of that set (tracked). */
        private int size = 0;

        DescribeCacheChangeLogListener(final IDescribeCache cache) {

            if (cache == null)
                throw new IllegalArgumentException();

            this.cache = cache;

        }

        /**
         * Vectors updates against the DESCRIBE cache.
         */
        @Override
        public void changeEvent(final IChangeRecord record) {

            if (record.getChangeAction() == ChangeAction.UPDATED) {

                /*
                 * This state change does not matter for cache maintenance
                 * unless we also plan to note the {Axiom, Inference, Explicit}
                 * state on the statements in the cache.
                 */

                return;

            }

            if (ivs == null) {

                // Lazy instantiation.
                ivs = new LinkedHashSet<IV<?, ?>>();

                size = 0;

            }

            final ISPO spo = record.getStatement();

            if (log.isTraceEnabled())
                log.trace("Invalidation notice: spo=" + spo);

            if (ivs.add(spo.s()))
                size++;

            if (ivs.add(spo.o()))
                size++;

            if (size > threshold) {
                flush();
            }

        }

        @Override
        public void transactionBegin() {

        }

        @Override
        public void transactionPrepare() {

            flush();

        }

        @Override
        public void transactionCommited(final long commitTime) {

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

        /**
         * Incremental flush (vectored cache invalidation notices).
         */
        private void flush() {

            if (ivs != null) {

                cache.invalidate(ivs);

                reset();

            }

        }

        /** Reset the buffer. */
        private void reset() {

            ivs = null;

            size = 0;

        }

    } // class DescribeCacheChangeLogListener

} // class DescribeServiceFactory
