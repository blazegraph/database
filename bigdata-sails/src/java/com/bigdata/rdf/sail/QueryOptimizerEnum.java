/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jan 2, 2011
 */
package com.bigdata.rdf.sail;

/**
 * The known query optimizers.
 * 
 * @see QueryHints#OPTIMIZER
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum QueryOptimizerEnum {
    /**
     * The query optimizer is disabled. The joins in the query will be evaluated
     * in the order in which they are given. This may be used to compensate when
     * the static query optimizer produces an inefficient join ordering.
     */
    None,
    /**
     * A query optimizer based on a static analysis of the query which relies on
     * fast range counts for the basic graph patterns to estimate the
     * cardinality of the different access paths. This optimizer is fast but it
     * can fail to order joins correctly as the error in the estimated
     * cardinality of joins can grow exponentially in the number of joins in the
     * query.
     */
    Static,
    /**
     * A runtime query optimizer based on sampling. The runtime query optimizer
     * samples each of the access paths and each of the joins and builds out
     * join paths in a breadth first manner until it finds a join ordering which
     * is known to dominate the other possible join orderings. The runtime query
     * optimizer takes into account the actual cardinality and correlation in
     * the query and the data selected by that query. The runtime query
     * optimizer can have slightly more overhead than the static query
     * optimizer, but it never produces a bad join ordering and often identifies
     * the <em>best</em> join ordering. For cases where the <code>static</code>
     * query optimizer produces a bad join ordering, the runtime query optimizer
     * can find join orderings which are orders of magnitude more efficient (10x
     * or 100x). For long running joins, this can translates into a savings of
     * minutes or hours.
     */
    Runtime;
    
    public static String queryHint(final QueryOptimizerEnum val) {
    	return "prefix "+QueryHints.NAMESPACE+": <"+QueryHints.PREFIX+QueryHints.OPTIMIZER+"="+val+"> ";
    }
}
