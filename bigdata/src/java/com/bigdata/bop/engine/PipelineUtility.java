/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 1, 2010
 */

package com.bigdata.bop.engine;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.NoSuchBOpException;

/**
 * Utility methods relevant to pipelined operator evaluation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PipelineUtility {

    private static final Logger log = Logger.getLogger(PipelineUtility.class);

    /**
     * Return <code>true</code> iff <i>availableChunkMap</i> map is ZERO (0) for
     * the given operator and its descendants AND the <i>runningCountMap</i> is
     * ZERO (0) for the operator and all descendants of the operator. For the
     * purposes of this method, only {@link BOp#args() operands} are considered
     * as descendants.
     * <p>
     * Note: The movement of the intermediate binding set chunks during query
     * processing forms an acyclic directed graph. We can decide whether or not
     * a {@link BOp} in the query plan can be triggered by the current activity
     * pattern by inspecting the {@link BOp} and its operands recursively.
     * 
     * @param bopId
     *            The identifier for an operator which appears in the query
     *            plan.
     * @param queryPlan
     *            The query plan.
     * @param queryIndex
     *            An index for the query plan as constructed by
     *            {@link BOpUtility#getIndex(BOp)}.
     * @param runningCountMap
     *            A map reporting the #of instances of each operator which are
     *            currently being evaluated (distinct evaluations are performed
     *            for each chunk and shard).
     * @param availableChunkCountMap
     *            A map reporting the #of chunks available for each operator in
     *            the pipeline (we only report chunks for pipeline operators).
     * 
     * @return <code>true</code> iff the {@link BOp} can not be triggered given
     *         the query plan and the activity map.
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws NoSuchBOpException
     *             if <i>bopId</i> is not found in the query index.
     */
    static public boolean isDone(final int bopId, final BOp queryPlan,
            final Map<Integer, BOp> queryIndex,
            final Map<Integer, AtomicLong> runningCountMap,
            final Map<Integer, AtomicLong> availableChunkCountMap) {

        if (queryPlan == null)
            throw new IllegalArgumentException();

        if (queryIndex == null)
            throw new IllegalArgumentException();

        if (availableChunkCountMap == null)
            throw new IllegalArgumentException();

        final BOp op = queryIndex.get(bopId);

        if (op == null)
            throw new NoSuchBOpException(bopId);

        final Iterator<BOp> itr = BOpUtility.preOrderIterator(op);

        while (itr.hasNext()) {

            final BOp t = itr.next();

            final Integer id = (Integer) t.getProperty(BOp.Annotations.BOP_ID);

            if (id == null)
                continue;

            {

                /*
                 * If the operator is running then it is, defacto, "not done."
                 * 
                 * If any descendants of the operator are running, then they
                 * could cause the operator to be re-triggered and it is "not
                 * done."
                 */

                final AtomicLong runningCount = runningCountMap.get(id);

                if (runningCount != null && runningCount.get() != 0) {

                    if (log.isInfoEnabled())
                        log.info("Operator can be triggered: op=" + op
                                + ", possible trigger=" + t + " is running.");

                    return false;

                }

            }

            {

                /*
                 * Any chunks available for the operator in question or any of
                 * its descendants could cause that operator to be triggered.
                 */

                final AtomicLong availableChunkCount = availableChunkCountMap
                        .get(id);

                if (availableChunkCount != null
                        && availableChunkCount.get() != 0) {

                    if (log.isInfoEnabled())
                        log.info("Operator can be triggered: op=" + op
                                + ", possible trigger=" + t + " has "
                                + availableChunkCount + " chunks available.");

                    return false;

                }

            }

        }

        if (log.isInfoEnabled())
            log.info("Operator can not be triggered: op=" + op);

        return true;

    }

}
