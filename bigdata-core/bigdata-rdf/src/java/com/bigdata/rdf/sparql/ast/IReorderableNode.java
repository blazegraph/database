/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Mar 10, 2012
 */

package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.sparql.ast.optimizers.StaticOptimizer;
import com.bigdata.rdf.store.ITripleStore;

/**
 * Interface for things which can be re-ordered by the static join
 * optimizer.  Currently limited to StatementPatternNodes and some 
 * instances of ArbitraryLengthPathNodes (those with a single statement pattern
 * inside).
 */
public interface IReorderableNode extends IGroupMemberNode, IBindingProducerNode, BOp {

	/**
	 * The decision about whether to re-order can no longer be made simply
	 * by examining the type - individual instances of a particular type
	 * may or may not be reorderable.
	 */
	boolean isReorderable();
	
	/**
	 * Return the estimated cardinality - either the range count of a 
	 * statement pattern or some computed estimated cardinality for a join
	 * group.
	 * @param opt This optimizer can be used to help work out the estimate
	 */
	long getEstimatedCardinality(StaticOptimizer opt);
	
}
