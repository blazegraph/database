package com.bigdata.bop.engine;

import java.lang.ref.WeakReference;

/**
 * Class pairs together the immutable deadline associated with a query and the
 * {@link AbstractRunningQuery}. The natural ordering places instances of this
 * class into ascending deadline order. The deadline is simply the timestamp at
 * which the query deadline is expired. Therefore, the instances are simply
 * ordered by the time when their deadline will expire. The queries that will
 * expire soonest are first, those that can run longer come later. This ordering
 * is used for a priority queue.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/772"> Query
 *      timeout only checked at operator start/stop. </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
class QueryDeadline implements Comparable<QueryDeadline> {

    /**
     * The deadline for this query (in nanoseconds).
     */
    final long deadlineNanos;

    /**
     * A reference to the query.
     * <p>
     * Note: A {@link WeakReference} is used to avoid having the deadline queue
     * pin the {@link AbstractRunningQuery} objects.
     */
    private final WeakReference<AbstractRunningQuery> queryRef;

    /**
     * 
     * @param deadlineNanos
     *            The deadline for this query (in nanoseconds).
     * @param query
     *            The query.
     */
    public QueryDeadline(final long deadlineNanos, final AbstractRunningQuery query) {

        this.deadlineNanos = deadlineNanos;

        this.queryRef = new WeakReference<AbstractRunningQuery>(query);

    }

    /**
     * Comparator orders the queries based on increasing deadline. The query
     * with the soonest deadline will be ordered first. The query with the
     * greatest deadline will be ordered last. Queries that do not have an
     * explicit deadline are assigned a deadline of {@link Long#MAX_VALUE} and
     * will be ordered last.
     * <p>
     * Note: A natural order based on deadline was added to support timely
     * termination of compute bound queries that exceed their deadline.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/772">
     *      Query timeout only checked at operator start/stop. </a>
     */
    @Override
    public int compareTo(final QueryDeadline o) {
        final long d0 = this.deadlineNanos;
        final long d1 = o.deadlineNanos;
        if (d0 < d1)
            return -1;
        if (d0 > d1)
            return 1;
        return 0;
    }

    /**
     * Check the deadline on the query. If the query is not terminated and the
     * deadline has expired, then the query is terminated as a side-effect.
     * 
     * @param nowNanosIsIgnored
     *            A current timestamp.
     * 
     * @return <code>null</code> if the query is terminated and
     *         <code>this</code> if the query is not terminated.
     */
    QueryDeadline checkDeadline(final long nowNanosIsIgnored) {

        final AbstractRunningQuery q = queryRef.get();

        if (q == null) {

            /*
             * The weak reference to the query has been cleared. This query is
             * known to be terminated.
             */

            return null;

        }

        // Check the deadline.
        q.checkDeadline();

        if (q.isDone()) {

            // Query is terminated.
            return null;

        }

        // Query is running and deadline is not expired.
        return this;

    }

}