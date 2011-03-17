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

package com.bigdata.bop.joinGraph;

import com.bigdata.bop.IPredicate;
import com.bigdata.relation.rule.IRule;

/**
 * An interface used by an {@link IEvaluationPlan} to obtain range counts for
 * the {@link IPredicate}s in an {@link IRule}. The use of this interface on
 * the one hand makes it possible to test {@link IEvaluationPlan}s without real
 * data and on the other hand makes it possible to cache range counts across
 * queries evaluated against the same state of the database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRangeCountFactory {

    /**
     * Return the range count for the predicate as bound. For indices that use
     * delete markers this will be an upper bound NOT an exact range count.
     * However, if the range count is reported as ZERO (0L) as an upper bound
     * this still indicates that there are no solutions for that predicate in
     * the data.
     * 
     * @param pred
     *            The predicate.
     * 
     * @return The range count.
     */
    public long rangeCount(IPredicate pred);
    
}
