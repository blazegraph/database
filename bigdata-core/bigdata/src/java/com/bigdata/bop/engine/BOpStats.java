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
 * Created on Aug 26, 2010
 */

package com.bigdata.bop.engine;

import java.io.Serializable;

import com.bigdata.bop.BOp;
import com.bigdata.counters.CAT;

/**
 * Statistics associated with the evaluation of a {@link BOp}. These statistics
 * are per {@link BOp}. The top-level {@link BOp} will reflect the throughput
 * for the entire pipeline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BOpStats implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

	/**
	 * The elapsed time (milliseconds) for the corresponding operation. When
	 * aggregated, this will generally exceed the wall time since concurrent
	 * processes are nearly always used during query evaluation.
	 */
    final public CAT elapsed = new CAT();

    /**
     * The #of instances of a given operator which have been started (and
     * successully terminated) for a given query. This provides interesting
     * information about the #of task instances for each operator which were
     * required to execute a query.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/793">
     *      Explain reports incorrect value for opCount</a>
     */
    final public CAT opCount = new CAT();
    
    /**
     * #of chunks in.
     */
    final public CAT chunksIn = new CAT();
//    final public AtomicLong chunksIn = new AtomicLong();

    /**
     * #of units sets in (tuples, elements, binding sets, etc).
     */
    final public CAT unitsIn = new CAT();
//    final public AtomicLong unitsIn = new AtomicLong();

    /**
     * #of chunks out.
     */
    final public CAT chunksOut = new CAT();
//    final public AtomicLong chunksOut = new AtomicLong();


    /**
     * #of units sets in (tuples, elements, binding sets, etc).
     */
    final public CAT unitsOut = new CAT();
//    final public AtomicLong unitsOut = new AtomicLong();

    /**
     * The #of error which were masked by the semantics of the query.
     * <p>
     * Note: SPARQL type errors in aggregations often cause bindings or
     * solutions to fail without causing any runtime exception which can be
     * observed at the query level. This counter provides a means to observe
     * those errors.
     */
    final public CAT typeErrors = new CAT();

    /**
     * The #of tuples written onto a relation (not an index).
     * <p>
     * Note: This mutation counts should reflect the #of tuples written on a
     * relation rather than on an index. The per-index tuple mutation count MAY
     * also be reported, but it should be reported by extending this class and
     * declaring additional mutation counters.
     * <p>
     * Note: Operations which fix point some closure depend on the correctness
     * of the {@link #mutationCount} for their termination condition.
     * <P>
     * Note: Mutation operations MUST NOT overwrite existing tuples with
     * identical state. This is important for two reasons. First, fix point
     * operations depend on the {@link #mutationCount} being ZERO (0) in a given
     * round if nothing was changed in order to terminate the closure. Second,
     * due to the general MVCC and copy-on-write architecture, index writes
     * drive IO. If the tuple state would not be changed, then the index write
     * SHOULD NOT be performed.
     */
    final public CAT mutationCount = new CAT();

    /**
     * Constructor.
     * <p>
     * Note: Do not pre-increment {@link #opCount}. See {@link #add(BOpStats)}
     * and {@link AbstractRunningQuery#haltOp(IHaltOpMessage)}.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/793">
     *      Explain reports incorrect value for opCount</a>
     */
    public BOpStats() {

//        opCount.increment();
    	
    }

    /**
     * Combine the statistics (addition), but do NOT add to self.
     * 
     * @param o
     *            Another statistics object.
     */
    public void add(final BOpStats o) {
        if (this == o) {
            // Do not add to self!
            return;
        }
        elapsed.add(o.elapsed.get());
        opCount.add(o.opCount.get());
        chunksIn.add(o.chunksIn.get());
        unitsIn.add(o.unitsIn.get());
        unitsOut.add(o.unitsOut.get());
        chunksOut.add(o.chunksOut.get());
        typeErrors.add(o.typeErrors.get());
        mutationCount.add(o.mutationCount.get());
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("{elapsed=" + elapsed.get());
        sb.append(",opCount=" + opCount.get());
        sb.append(",chunksIn=" + chunksIn.get());
        sb.append(",unitsIn=" + unitsIn.get());
        sb.append(",chunksOut=" + chunksOut.get());
        sb.append(",unitsOut=" + unitsOut.get());
        sb.append(",typeErrors=" + typeErrors.get());
        sb.append(",mutationCount=" + mutationCount.get());
        toString(sb); // extension hook
        sb.append("}");
        return sb.toString();
    }

    /**
     * Extension hook for {@link #toString()}.
     * 
     * @param sb
     *            Where to write the additional state.
     */
    protected void toString(final StringBuilder sb) {

    }

}
