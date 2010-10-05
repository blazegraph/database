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
 * Created on Aug 20, 2008
 */

package com.bigdata.relation.rule.eval;

import org.apache.log4j.Logger;

import com.bigdata.bop.IPredicate;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * Default implementation.
 * 
 * @todo use a cache per relation and timestamp. Track the actual range counts
 *       for a predicate as bound in a query vs those predicated for the
 *       predicate. The actual counts can also vary if the backchainer is turned
 *       on since that is not reflected in the non-exact range count. We can
 *       store range counts efficiently in a B+Tree using a key formed from the
 *       {s:p:o} and using 0L for a variable.
 *       <p>
 *       perhaps it would be worth while to turn off the range count metadata
 *       for some of the indices, e.g., the reverse lexicon does not need this
 *       and perhaps the justifications index does not need it either. That is a
 *       performance boost of ~8% if I recall.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultRangeCountFactory implements IRangeCountFactory {

    protected static final transient Logger log = Logger
            .getLogger(DefaultRangeCountFactory.class);
    
    protected static final transient boolean DEBUG = log.isDebugEnabled();
    
    private final IJoinNexus joinNexus;
    
    public DefaultRangeCountFactory(final IJoinNexus joinNexus) {
        
        if (joinNexus == null)
            throw new IllegalArgumentException();
        
        this.joinNexus = joinNexus;
        
    }
    
    /**
     * Return the range count for the predicate. The range counts are requested
     * using the "non-exact" range count query, so the range counts are actually
     * the upper bound. However, if the upper bound is ZERO (0) then the range
     * count really is ZERO (0).
     * 
     * @param tailIndex
     *            The index of the predicate in the tail of the rule.
     * 
     * @return The range count for that tail predicate.
     */
    public long rangeCount(final IPredicate predicate) {

        final IRelation relation = joinNexus.getTailRelationView(predicate);

        final IAccessPath accessPath = joinNexus.getTailAccessPath(relation,
                predicate);

        final long rangeCount = accessPath.rangeCount(false/* exact */);

        if (DEBUG) {

            /*
             * @todo trace total time in range counts while generating the plan
             * or just track the total time to generate the plan, which should
             * be dominated by the range count time. if this adds too much
             * latency then consider other approaches to optimization.
             */
            log.debug("rangeCount=" + rangeCount + ", tail=" + predicate
                    + ", accessPath=" + accessPath);

        }

        return rangeCount;

    }

}
