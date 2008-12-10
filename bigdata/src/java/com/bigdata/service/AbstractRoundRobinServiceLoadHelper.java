package com.bigdata.service;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

/**
 * A round robin implementation that may be used when there are no scores
 * available. Services are selected using a round robin policy. The class will
 * notice service joins and service leaves and will incorporate the new set of
 * services into its decision making while preserving a (mostly) round robin
 * behavior.
 * <p>
 * Note: This has internal state in order to provide a round-robin policy.
 * Therefore the load balancer MUST use a single instance of this class for
 * stateful behavior to be observed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRoundRobinServiceLoadHelper implements
        IServiceLoadHelper {

    protected static final Logger log = Logger
            .getLogger(AbstractRoundRobinServiceLoadHelper.class);

    protected static final boolean INFO = log.isInfoEnabled();

    // protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * Used to provide a round robin behavior.
     */
    private final AtomicInteger roundRobinIndex = new AtomicInteger();

    protected AbstractRoundRobinServiceLoadHelper() {
        
    }
    
    /**
     * Await the availability of at least the specified #of {@link IDataService}s.
     * 
     * @param minCount
     *            The minimum #of data services.
     * @param timeout
     *            The timeout (ms).
     * 
     * @return An array #of the {@link UUID}s of the {@link IDataService}s
     *         that have been discovered. Note that at least <i>minDataServices</i>
     *         elements will be present in this array but that ALL discovered
     *         data services may be reported.
     * 
     * @see AbstractScaleOutFederation#awaitServices(int, long)
     */
    abstract protected UUID[] awaitServices(int minCount, long timeout)
            throws InterruptedException, TimeoutException;

    /**
     * Issues {@link UUID}s using a round-robin over those that are joined. For
     * this purpose, the joined {@link DataService}s are appended to an ordered
     * set. The index of the most recently assigned service is maintained in a
     * counter. Services that leave are removed from the set, but we do not
     * bother to adjust the counter. We always return the {@link UUID} of the
     * service at index MOD N, where N is the #of services that are joined at
     * the time that this method looks at the set. We then post-increment the
     * counter.
     * <p>
     * The round-robin allocate strategy is a good choice where there is little
     * real data on the services, or when there is a set of services whose
     * scores place them into an equivalence class such that we have no
     * principled reason for preferring one service over another among those in
     * the equivalence class.
     * 
     * @throws TimeoutException
     * @throws InterruptedException
     * 
     * @todo develop the concept of equivalence classes further. divide the
     *       joined services into a set of equivalence classes. parameterize
     *       this method to accept an equivalence class. always apply this
     *       method when drawing from an equivalence class.
     * 
     * @see TestLoadBalancerRoundRobin
     */
    public UUID[] getUnderUtilizedDataServices(int minCount,
            int maxCount, UUID exclude) throws InterruptedException,
            TimeoutException {

        /*
         * Note: In order to reduce the state that we track I've coded this to
         * build the UUID[] array on demand from the joined services and then
         * sort the UUIDs. This gives us a single additional item of state over
         * that already recorded by the load balancer - the counter itself. This
         * is just a pragmatic hack to get a round-robin behavior into place.
         */

        if (INFO)
            log.info("minCount=" + minCount + ", maxCount=" + maxCount
                    + ", exclude=" + exclude);
        
        /*
         * Note: I've reduced this to only demand a single data service. If more
         * are available then they are returned. If none are available then we
         * can not proceed. If only one is available, then it will be assigned
         * for each of the requested minCount UUIDs.
         */
        final long timeout = 10000;// ms @todo config
        UUID[] a = awaitServices(1,
                        //minCount == 0 ? 1 : minCount/* minCount */,
                        timeout);

        if (a.length == 1 && exclude != null && a[0] == exclude) {

            /*
             * Make sure that we have at least one unexcluded service. @todo
             * unit test for this bit.
             */

            a = awaitServices(2 /* minCount */, timeout);

        }

        /*
         * Note: We sort the UUIDs in order to provide a (mostly) stable
         * ordering. The set of UUIDs CAN change through service joins and
         * service leaves. The [roundRobinIndex] maintains the position of the
         * last UUID returned from those available and we will start serving
         * again from that position.
         */
        Arrays.sort(a);

        final int n; // = Math.min(maxCount, a.length);
        if (minCount == 0 && maxCount == 0) {
            /*
             * When there are no constraints we want to make any recommendations
             * that we can.
             */
            n = a.length;
        } else if (a.length > minCount) {
            /*
             * No more than the maxCount (note that maxCount may be zero in
             * which case we want to have no more than the minCount).
             */
            n = Math.min(a.length, Math.max(minCount, maxCount));
        } else {
            // no less than the minCount.
            n = minCount;
        }
        
        if(INFO) {
            
            log.info("Have "+a.length+" data services and will make "+n+" assignments.");
            
        }
        
        final UUID[] b = new UUID[ n ];
        int i = 0;
        int nexcluded = 0; // set to ONE iff we see the excluded service.
        while (i + nexcluded < b.length) {
            
            final UUID uuid = a[roundRobinIndex.getAndIncrement() % a.length];
            
            assert uuid != null;
            
            if (uuid == exclude) {
                nexcluded = 1;
                continue;
            }

            b[i++] = uuid;

        }

        final UUID[] c;
        if (nexcluded == 0) {

            c = b;
            
        } else {

            // make dense.
            
            c = new UUID[n - 1];
            
            System.arraycopy(b, 0, c, 0, n - 1);
            
        }
        
        if(INFO) {
            
            log.info("Assigned UUIDs: "+Arrays.toString(c));
            
        }
        
        return c;
        
    }

}
