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
 * Created on Jun 22, 2009
 */

package com.bigdata.bop.engine;

import java.text.DateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.join.PipelineJoin.PipelineJoinStats;
import com.bigdata.rdf.sail.Rule2BOpUtility;
import com.bigdata.striterator.IKeyOrder;

/**
 * Class defines the log on which summary operator execution statistics are
 * written..
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: RuleLog.java 3448 2010-08-18 20:55:58Z thompsonbry $
 */
public class QueryLog {

    protected static final transient Logger log = Logger
            .getLogger(QueryLog.class);

    static {
		logTableHeader();
    }
    
    static public void logTableHeader() {
    	if(log.isInfoEnabled())
    		log.info(QueryLog.getTableHeader());
    }
    
    /**
     * Log rule execution statistics.
     * 
     * @param stats
     *            The rule execution statistics.
     *            
     *            @todo need start and end time for the query.
     */
    static public void log(final IRunningQuery q) {

		if (log.isInfoEnabled()) {

			try {

				logDetailRows(q);

				logSummaryRow(q);
				
			} catch (RuntimeException t) {

				log.error(t,t);
				
			}

		}

    }

	/**
	 * Log a detail row for each operator in the query.
	 */
    static private void logDetailRows(final IRunningQuery q) {

		final Integer[] order = BOpUtility.getEvaluationOrder(q.getQuery());

		int orderIndex = 0;
		
		for (Integer bopId : order) {

			log.info(getTableRow(q, orderIndex, bopId, false/* summary */));
			
			orderIndex++;
			
		}

	}

    /**
     * Log a summary row for the query.
     */
    static private void logSummaryRow(final IRunningQuery q) {

		log.info(getTableRow(q, -1/* orderIndex */, q.getQuery().getId(), true/* summary */));

    }
    
    static private String getTableHeader() {

        final StringBuilder sb = new StringBuilder();

        /*
         * Common columns for the overall query and for each pipeline operator.
         */
        sb.append("queryId");
        sb.append("\tbeginTime");
        sb.append("\tdoneTime");
        sb.append("\tdeadline");
        sb.append("\telapsed");
        sb.append("\tserviceId");
        sb.append("\tcause");
        sb.append("\tbop");
        /*
         * Columns for each pipeline operator.
         */
        sb.append("\tevalOrder"); // [0..n-1]
        sb.append("\tbopId");
        sb.append("\tpredId");
        sb.append("\tevalContext");
        sb.append("\tcontroller");
        // metadata considered by the static optimizer.
        sb.append("\tstaticBestKeyOrder"); // original key order assigned by static optimizer.
        sb.append("\tnvars"); // #of variables in the predicate for a join.
        sb.append("\tfastRangeCount"); // fast range count used by the static optimizer.
        // dynamics (aggregated for totals as well).
        sb.append("\tfanIO");
        sb.append("\tsumMillis"); // cumulative milliseconds for eval of this operator.
        sb.append("\topCount"); // cumulative #of invocations of tasks for this operator.
        sb.append("\tchunksIn");
        sb.append("\tunitsIn");
        sb.append("\tchunksOut");
        sb.append("\tunitsOut");
        sb.append("\tjoinRatio"); // expansion rate multipler in the solution count.
        sb.append("\taccessPathDups");
        sb.append("\taccessPathCount");
        sb.append("\taccessPathRangeCount");
        sb.append("\taccessPathChunksIn");
        sb.append("\taccessPathUnitsIn");
        // dynamics based on elapsed wall clock time.
        sb.append("\tsolutions/ms");
        sb.append("\tmutations/ms");
        //
        // cost model(s)
        //
        sb.append('\n');

        return sb.toString();

    }

    /**
     * Return a tabular representation of the query {@link RunState}.
     *
     * @param q The {@link IRunningQuery}.
     * @param evalOrder The evaluation order for the operator.
     * @param bopId The identifier for the operator.
     * @param summary <code>true</code> iff the summary for the query should be written.
     * @return The row of the table.
     */
	static private String getTableRow(final IRunningQuery q,
			final int evalOrder, final Integer bopId, final boolean summary) {

        final StringBuilder sb = new StringBuilder();

        final DateFormat dateFormat = DateFormat.getDateTimeInstance(
                DateFormat.FULL, DateFormat.FULL);
        
        // The elapsed time for the query (wall time in milliseconds).
        final long elapsed = q.getElapsed();
        
        // The serviceId on which the query is running : null unless scale-out.
        final UUID serviceId = q.getQueryEngine().getServiceUUID();
        
        // The thrown cause : null unless the query was terminated abnormally.
        final Throwable cause = q.getCause();
        
        sb.append(q.getQueryId());
        sb.append('\t');
        sb.append(dateFormat.format(new Date(q.getStartTime())));
        sb.append('\t');
        sb.append(dateFormat.format(new Date(q.getDoneTime())));
        sb.append('\t');
        if(q.getDeadline()!=Long.MAX_VALUE)
        	sb.append(dateFormat.format(new Date(q.getDeadline())));
        sb.append('\t');
        sb.append(elapsed);
        sb.append('\t');
        sb.append(serviceId == null ? "N/A" : serviceId.toString());
        sb.append('\t');
        if (cause != null) 
            sb.append(cause.getLocalizedMessage());

    	final Map<Integer, BOp> bopIndex = q.getBOpIndex();
    	final Map<Integer, BOpStats> statsMap = q.getStats();
		final BOp bop = bopIndex.get(bopId);

		// the operator.
		sb.append('\t');
		if (summary) {
			/*
			 * The entire query (recursively). New lines are translated out to
			 * keep this from breaking the table format.
			 */
			sb.append(BOpUtility.toString(q.getQuery()).replace('\n', ' '));
            sb.append('\t');
            sb.append("total"); // summary line.
        } else {
        	// Otherwise show just this bop.
        	sb.append(bopIndex.get(bopId).toString());
	        sb.append('\t');
	        sb.append(evalOrder); // eval order for this bop.
        }
        
        sb.append('\t');
        sb.append(Integer.toString(bopId));
		sb.append('\t');
		{
			/*
			 * Show the predicate identifier if this is a Join operator.
			 * 
			 * @todo handle other kinds of join operators when added using a
			 * shared interface.
			 */
			final IPredicate<?> pred = (IPredicate<?>) bop
					.getProperty(PipelineJoin.Annotations.PREDICATE);
			if (pred != null) {
				sb.append(Integer.toString(pred.getId()));
			}
		}
		sb.append('\t');
		sb.append(bop.getEvaluationContext());
		sb.append('\t');
		sb.append(bop.getProperty(BOp.Annotations.CONTROLLER,
				BOp.Annotations.DEFAULT_CONTROLLER));

		/*
		 * Static optimizer metadata.
		 * 
		 * FIXME Should report [nvars] be the expected asBound #of variables
		 * given the assigned evaluation order and the expectation of propagated
		 * bindings (optionals may leave some unbound).
		 */
		{

			final IPredicate pred = (IPredicate<?>) bop
					.getProperty(PipelineJoin.Annotations.PREDICATE);
			
			if (pred != null) {
			
				final IKeyOrder keyOrder = (IKeyOrder<?>) pred
						.getProperty(Rule2BOpUtility.Annotations.ORIGINAL_INDEX);
				
				final Long rangeCount = (Long) pred
						.getProperty(Rule2BOpUtility.Annotations.ESTIMATED_CARDINALITY);
				
				sb.append('\t'); // keyorder
				if (keyOrder != null)
					sb.append(keyOrder);
				
				sb.append('\t'); // nvars
				if (keyOrder != null)
					sb.append(pred.getVariableCount(keyOrder));
				
				sb.append('\t'); // rangeCount
				if (rangeCount!= null)
					sb.append(rangeCount);
				
			} else {
				sb.append('\t'); // keyorder
				sb.append('\t'); // nvars
				sb.append('\t'); // rangeCount
			}
		}

		/*
		 * Dynamics.
		 */
		
		int fanIO = 0; // @todo aggregate from RunState.

		final PipelineJoinStats stats = new PipelineJoinStats();
		if(summary) {
	    	// Aggregate the statistics for all pipeline operators.
			for (BOpStats t : statsMap.values()) {
				stats.add(t);
			}
		} else {
            // Just this operator.
            final BOpStats tmp = statsMap.get(bopId);
            if (tmp != null)
                stats.add(tmp);
		}
		final long unitsIn = stats.unitsIn.get();
		final long unitsOut = stats.unitsOut.get();
		sb.append('\t');
		sb.append(Integer.toString(fanIO));
		sb.append('\t');
		sb.append(stats.elapsed.get());
		sb.append('\t');
		sb.append(stats.opCount.get());
		sb.append('\t');
		sb.append(stats.chunksIn.get());
		sb.append('\t');
		sb.append(stats.unitsIn.get());
		sb.append('\t');
		sb.append(stats.chunksOut.get());
		sb.append('\t');
		sb.append(stats.unitsOut.get());
		sb.append('\t');
		sb.append(unitsIn == 0 ? "N/A" : unitsOut / (double) unitsIn);
		sb.append('\t');
		sb.append(stats.accessPathDups.get());
		sb.append('\t');
		sb.append(stats.accessPathCount.get());
		sb.append('\t');
		sb.append(stats.accessPathRangeCount.get());
		sb.append('\t');
		sb.append(stats.accessPathChunksIn.get());
		sb.append('\t');
		sb.append(stats.accessPathUnitsIn.get());

		/*
		 * Use the total elapsed time for the query (wall time).
		 */
		// solutions/ms
		sb.append('\t');
		sb.append(elapsed == 0 ? 0 : stats.unitsOut.get() / elapsed);
		// mutations/ms : @todo mutations/ms.
		sb.append('\t');
//		sb.append(elapsed==0?0:stats.unitsOut.get()/elapsed);

        sb.append('\n');

        return sb.toString();

    }

}
