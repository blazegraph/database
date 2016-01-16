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
 * Created on Feb 12, 2012
 */

package com.bigdata.bop.join;

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.join.PipelineJoin.Annotations;
import com.bigdata.counters.CAT;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * Extended statistics for the join operator.
 */
public class PipelineJoinStats extends BaseJoinStats {

	private static final long serialVersionUID = 1L;

    /**
     * The #of input solutions consumed (not just accepted).
     * <p>
     * Note: This counter is highly correlated with {@link BOpStats#unitsIn} but
     * is incremented only when we begin evaluation of the {@link IAccessPath}
     * associated with a specific input solution.
     * <p>
     * When {@link Annotations#COALESCE_DUPLICATE_ACCESS_PATHS} is
     * <code>true</code>, multiple input binding sets can be mapped onto the
     * same {@link IAccessPath} and this counter will be incremented by the #of
     * such input binding sets.
     */
    public final CAT inputSolutions = new CAT();

    /**
     * The #of output solutions generated. This is incremented as soon as the
     * solution is produced and is used by {@link #getJoinHitRatio()}. Of
     * necessity, updates to {@link #inputSolutions} slightly lead updates to
     * {@link #inputSolutions}.
     * <p>
     * Note: This counter is highly correlated with {@link BOpStats#unitsOut}.
     */
    public final CAT outputSolutions = new CAT();

    /**
     * The estimated join hit ratio. This is computed as
     * 
     * <pre>
     * outputSolutions / inputSolutions
     * </pre>
     * 
     * It is ZERO (0) when {@link #inputSolutions} is ZERO (0).
     * <p>
     * The join hit ratio is always accurate when the join is fully executed.
     * However, when a cutoff join is used to estimate the join hit ratio a
     * measurement error can be introduced into the join hit ratio unless
     * {@link Annotations#COALESCE_DUPLICATE_ACCESS_PATHS} is <code>false</code>,
     * {@link Annotations#MAX_PARALLEL} is GT ONE (1), or
     * {@link Annotations#MAX_PARALLEL_CHUNKS} is GT ZERO (0).
     * <p>
     * When access paths are coalesced because there is an inner loop over the
     * input solutions mapped onto the same access path. This inner loop the
     * causes {@link PipelineJoinStats#inputSolutions} to be incremented by the
     * #of coalesced access paths <em>before</em> any {@link #outputSolutions}
     * are counted. Coalescing access paths therefore can cause the join hit
     * ratio to be underestimated as there may appear to be more input solutions
     * consumed than were actually applied to produce output solutions if the
     * join was cutoff while processing a set of input solutions which were
     * identified as using the same as-bound access path.
     * <p>
     * The worst case can introduce substantial error into the estimated join
     * hit ratio. Consider a cutoff of <code>100</code>. If one input solution
     * generates 100 output solutions and two input solutions are mapped onto
     * the same access path, then the input count will be 2 and the output count
     * will be 100, which gives a reported join hit ration of <code>100/2</code>
     * when the actual join hit ratio is <code>100/1</code>.
     * <p>
     * A similar problem can occur if {@link Annotations#MAX_PARALLEL} or
     * {@link Annotations#MAX_PARALLEL_CHUNKS} is GT ONE (1) since input count
     * can be incremented by the #of threads before any output solutions are
     * generated. Estimation error can also occur if multiple join tasks are run
     * in parallel for different chunks of input solutions.
     */
    public double getJoinHitRatio() {

        final long in = inputSolutions.get();
        
        final long out = outputSolutions.get();
        
        if (in == 0)
            return 0;
        
        return ((double) out) / in;
        
    }

    @Override
	public void add(final BOpStats o) {

		super.add(o);

		if (o instanceof PipelineJoinStats) {

			final PipelineJoinStats t = (PipelineJoinStats) o;

			inputSolutions.add(t.inputSolutions.get());

			outputSolutions.add(t.outputSolutions.get());

			// if (t.fanIn > this.fanIn) {
			// // maximum reported fanIn for this join dimension.
			// this.fanIn = t.fanIn;
			// }
			// if (t.fanOut > this.fanOut) {
			// // maximum reported fanOut for this join dimension.
			// this.fanOut += t.fanOut;
			// }

		}

	}

	@Override
	protected void toString(final StringBuilder sb) {
		sb.append(",inputSolutions=" + inputSolutions.get());
		sb.append(",outputSolutions=" + outputSolutions.get());
		sb.append(",joinHitRatio=" + getJoinHitRatio());
	}

}
