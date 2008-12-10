/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Dec 10, 2008
 */

package com.bigdata.service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;


/**
 * The default implementation used when scores are available.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractServiceLoadHelperWithScores extends
        AbstractServiceLoadHelper {

    protected static final Logger log = Logger
            .getLogger(AbstractServiceLoadHelperWithScores.class);

    protected static final boolean INFO = log.isInfoEnabled();

    // protected static final boolean DEBUG = log.isDebugEnabled();

    final protected UUID knownGood;

    final protected ServiceScore[] scores;

    /**
     * @param joinTimeout
     *            The maximum time in milliseconds
     * @param knownGood
     *            A service that is known to be active and NOT excluded from the
     *            request to be posed.
     * @param scores
     *            The current service scores (snapshot).
     */
    protected AbstractServiceLoadHelperWithScores(final long joinTimeout,
            final UUID knownGood, final ServiceScore[] scores) {

        super(joinTimeout);

        if (knownGood == null)
            throw new IllegalArgumentException();

        if (scores == null)
            throw new IllegalArgumentException();

        if (scores.length == 0)
            throw new IllegalArgumentException();

        this.knownGood = knownGood;

        this.scores = scores;

    }

    /**
     * Handles the case when we have per-service scores.
     * <p>
     * Note: Pre-condition: the service scores must exist and there must be at
     * least one active service with a score that is not excluded (the
     * {@link #knownGood} service).
     * 
     * @param minCount
     * @param maxCount
     * @param exclude
     * @return
     * 
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public UUID[] getUnderUtilizedDataServices(int minCount, int maxCount,
            UUID exclude) throws TimeoutException, InterruptedException {

        if (exclude != null && knownGood.equals(exclude)) {

            throw new IllegalArgumentException();

        }

        /*
         * Decide on the set of active services that we consider to be
         * under-utilized based on their scores. When maxCount is non-zero, this
         * set will be no larger than maxCount. When maxCount is zero the set
         * will contain all services that satisify the "under-utilized"
         * criteria.
         */
        final List<UUID> underUtilized = new ArrayList<UUID>(Math.max(minCount,
                maxCount));

        int nok = 0;
        for (int i = 0; i < scores.length; i++) {

            final ServiceScore score = scores[i];

            // excluded?
            if (score.serviceUUID.equals(exclude))
                continue;

            // not active?
            if (!isActiveDataService(score.serviceUUID))
                continue;

            if (isUnderUtilizedDataService(score, scores)) {

                underUtilized.add(score.serviceUUID);

                nok++;

            }

            if (maxCount > 0 && nok >= maxCount) {

                if (INFO)
                    log.info("Satisifed maxCount=" + maxCount);

                break;

            }

            if (minCount > 0 && maxCount == 0 && nok >= minCount) {

                if (INFO)
                    log.info("Satisifed minCount=" + minCount);

                break;

            }

        }

        if (INFO)
            log.info("Found " + underUtilized.size()
                    + " under-utilized and non-excluded services");

        if (minCount > 0 && underUtilized.isEmpty()) {

            /*
             * Since we did not find anything we default to the one service that
             * provided as our fallback. This service might not be
             * under-utilized and it might no longer be active, but it is the
             * service that we are going to return.
             */

            assert knownGood != null;

            log.warn("Will report fallback service: " + knownGood);

            underUtilized.add(knownGood);

        }

        /*
         * Populate the return array, choosing at least minCount services and
         * repeating services if necessary.
         * 
         * Note: We return at least minCount UUIDs, even if we have to return
         * the same UUID in each slot.
         */

        final UUID[] uuids = new UUID[Math.max(minCount, nok)];

        int n = 0, i = 0;

        while (n < uuids.length) {

            uuids[n++] = underUtilized.get(i++ % nok);

        }

        return uuids;

    }

}
