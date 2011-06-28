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
 * Created on Aug 26, 2010
 */

package com.bigdata.bop.engine;

import java.io.Serializable;

import com.bigdata.bop.BOp;
import com.bigdata.bop.PipelineOp;
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
	 * The #of instances of a given operator which have been created for a given
	 * query. This provides interesting information about the #of task instances
	 * for each operator which were required to execute a query.
	 * 
	 * TODO Due to the way this is incremented, this is always ONE (1) if
	 * {@link PipelineOp.Annotations#SHARED_STATE} is <code>true</code> (it
	 * reflects the #of times {@link #add(BOpStats)} was invoked plus one for
	 * the ctor rather than the #of times the operator task was invoked). This
	 * should be changed to reflect the #of operator task instances created
	 * instead.
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
     * Constructor.
     */
    public BOpStats() {

    	opCount.increment();
    	
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
    }
    
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("{elapsed=" + elapsed.get());
        sb.append(",opCount=" + opCount.get());
        sb.append(",chunksIn=" + chunksIn.get());
        sb.append(",unitsIn=" + unitsIn.get());
        sb.append(",chunksOut=" + chunksOut.get());
        sb.append(",unitsOut=" + unitsOut.get());
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
