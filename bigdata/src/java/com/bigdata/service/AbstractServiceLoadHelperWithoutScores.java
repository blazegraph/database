package com.bigdata.service;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

/**
 * Implementation that may be used when service scores are not yet
 * available.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractServiceLoadHelperWithoutScores extends
        AbstractServiceLoadHelper {

    protected static final Logger log = Logger
            .getLogger(AbstractServiceLoadHelperWithoutScores.class);

    protected static final boolean INFO = log.isInfoEnabled();

    // protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * 
     * @param joinTimeout
     */
    protected AbstractServiceLoadHelperWithoutScores(final long joinTimeout) {

        super(joinTimeout);

    }

    /**
     * Computes the under-utilized services in the case where where <i>minCount</i>
     * is non-zero and we do not have pre-computed {@link #serviceScores} on hand. If
     * there are also no active services, then this awaits the join of at least
     * one service. Once it has at least one service that is not the optionally
     * <i>exclude</i>d service, it returns the "under-utilized" services.
     * 
     * @throws TimeoutException
     *             If the result could not be obtained before the timeout was
     *             triggered.
     * 
     * @throws InterruptedException
     */
    public UUID[] getUnderUtilizedDataServices(int minCount, int maxCount,
            UUID exclude) throws TimeoutException, InterruptedException {

        final long begin = System.currentTimeMillis();

        while (true) {

            final long elapsed = System.currentTimeMillis() - begin;

            if (elapsed > joinTimeout) {

                log.warn("Timeout waiting for service to join.");

                throw new TimeoutException();

            }

            // all services that we know about right now.
            final UUID[] knownServiceUUIDs = getActiveServices();

            if (knownServiceUUIDs.length == 0) {

                // await a join.
                awaitJoin(100, TimeUnit.MILLISECONDS);

                continue;

            }

            /*
             * Scan and see if we have anything left after verifying that
             * the service is still on our "live" list and after excluding
             * this the optional [exclude] service.
             */

            int nok = 0;

            for (int i = 0; i < knownServiceUUIDs.length; i++) {

                if (exclude != null && exclude.equals(knownServiceUUIDs[i])) {

                    knownServiceUUIDs[i] = null;

                    continue;

                }

                if (!isActiveDataService(knownServiceUUIDs[i])) {

                    knownServiceUUIDs[i] = null;

                    continue;

                }

                nok++;

            }

            if (nok <= 0) {

                // await a join.
                awaitJoin(100, TimeUnit.MILLISECONDS);

                continue;

            }

            /*
             * Now that we have at least one UUID, populate the return array.
             * 
             * Note: We return at least minCount UUIDs, even if we have to
             * return the same UUID in each slot.
             */

            final UUID[] uuids = new UUID[Math.max(minCount, nok)];

            int n = 0, i = 0;

            while (n < uuids.length) {

                final UUID tmp = knownServiceUUIDs[i++ % nok];

                if (tmp == null) {

                    continue;
                    
                }

                assert !tmp.equals(exclude);
                
                uuids[n++] = tmp;

            }

            return uuids;

        }

    }

}
