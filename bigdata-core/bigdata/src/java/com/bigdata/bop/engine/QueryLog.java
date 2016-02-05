/*

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
 * Created on Jun 22, 2009
 */

package com.bigdata.bop.engine;

import java.io.IOException;
import java.io.Writer;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.controller.NamedSetAnnotations;
import com.bigdata.bop.engine.RunState.RunStateEnum;
import com.bigdata.bop.join.IHashJoinUtility;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.join.PipelineJoinStats;
import com.bigdata.bop.joinGraph.rto.EdgeSample;
import com.bigdata.bop.joinGraph.rto.JGraph;
import com.bigdata.bop.joinGraph.rto.JoinGraph;
import com.bigdata.bop.joinGraph.rto.Path;
import com.bigdata.bop.joinGraph.rto.PathIds;
import com.bigdata.bop.rdf.join.ChunkedMaterializationOp;
import com.bigdata.bop.solutions.DropOp;
import com.bigdata.bop.solutions.GroupByOp;
import com.bigdata.bop.solutions.ProjectionOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.btree.Tuple;
import com.bigdata.counters.render.XHTMLRenderer;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpJoins;
import com.bigdata.striterator.IKeyOrder;

/**
 * Class defines the log on which summary operator execution statistics are
 * written.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class QueryLog {

    private static final String NA = "N/A";
    private static final String TD = "<td>";
    private static final String TDx = "</td\n>";
//    // the symbol used when a count is zero.
//    private static final String ZE = "0";
//    
////    // the symbol used when a count was zero, so count/sec is also zero.
////    final String NA = "0";
//    
//    // the symbol used when the elapsed time was zero, so count/sec is divide by zero.
//    private static final String DZ = "0";
    
    protected static final transient Logger log = Logger
            .getLogger(QueryLog.class);

    static {
        logTableHeader();
    }
    
    static public void logTableHeader() {
        if(log.isInfoEnabled())
            log.info(QueryLog.getTableHeader());
    }

//    /**
//     * A single buffer is reused to keep down the heap churn.
//     */
//    final private static StringBuilder sb = new StringBuilder(
//            Bytes.kilobyte32 * 4);

    /**
     * Log rule execution statistics @ INFO.
     * 
     * @param q
     *            The running query.
     */
    static public void log(final IRunningQuery q) {

        if (!log.isInfoEnabled())
            return;
            
        try {

            final IRunningQuery[] children = (q instanceof AbstractRunningQuery) ? ((AbstractRunningQuery) q)
                    .getChildren() : null;

            /**
             * Note: The static StringBuilder can not be used if the parent
             * query has child subqueries without running into a deadlock on
             * the [sb] object. If there are no children, we could reuse the
             * global static [sb] and the AbstractRunningQuery.lock().
             * However, log(IRunningQuery) is ONLY invoke by
             * AbstractRunningQuery.cancel() and then only runs IFF QueryLog
             * is @ INFO. Since this is a rare combination, allocating a new
             * StringBuilder object here will not have an adverse impact on
             * the heap and avoids the possibility of a deadlock.
             * 
             * @see <a href="http://trac.blazegraph.com/ticket/992" > Deadlock
             *      between AbstractRunningQuery.cancel(), QueryLog.log(),
             *      and ArbitraryLengthPathTask</a>
             */
            final StringBuilder sb = new StringBuilder();
//                synchronized (sb) 
            {

                // clear the buffer.
                sb.setLength(0);

                {
                    final Map<Integer/* bopId */, QueueStats> queueStats = ((ChunkedRunningQuery) q)
                            .getQueueStats();

                    logSummaryRow(q, queueStats, sb);

                    logDetailRows(q, queueStats, sb);
                }

                if (children != null) {

                    for (int i = 0; i < children.length; i++) {

                        final IRunningQuery c = children[i];
                        
                        final Map<Integer/* bopId */, QueueStats> queueStats = ((ChunkedRunningQuery) c)
                                .getQueueStats();
                        
                        logSummaryRow(c, queueStats, sb);
                        
                        logDetailRows(c, queueStats, sb);

                    }
                    
                }

                log.info(sb);

            }
            
        } catch (RuntimeException t) {

            log.error(t,t);
            
        }

    }

//  /**
//   * Log the query.
//   * 
//   * @param q
//   *            The query.
//   * @param sb
//   *            Where to write the log message.
//   */
//  static public void log(final boolean includeTableHeader,
//          final IRunningQuery q, final StringBuilder sb) {
//
//      if(includeTableHeader) {
//          
//          sb.append(getTableHeader());
//          
//      }
//      
//      logDetailRows(q, sb);
//
//      logSummaryRow(q, sb);
//      
//    }
    
    /**
     * Log a detail row for each operator in the query.
     */
    static private void logDetailRows(final IRunningQuery q,
            final Map<Integer/* bopId */, QueueStats> queueStats,
            final StringBuilder sb) {

        final Integer[] order = BOpUtility.getEvaluationOrder(q.getQuery());

        int orderIndex = 0;
        
        for (Integer bopId : order) {

            sb.append(getTableRow(q, orderIndex, bopId, false/* summary */,
                    queueStats));
            
//          sb.append('\n');
            
            orderIndex++;
            
        }

    }

    /**
     * Log a summary row for the query.
     */
    static private void logSummaryRow(final IRunningQuery q,
            final Map<Integer/* bopId */, QueueStats> queueStats,
            final StringBuilder sb) {

        sb.append(getTableRow(q, -1/* orderIndex */, q.getQuery().getId(),
                true/* summary */, queueStats));
        
//      sb.append('\n');

    }
    
    static private String getTableHeader() {

        final StringBuilder sb = new StringBuilder();

        /*
         * Common columns for the overall query and for each pipeline operator.
         */
        sb.append("queryId");
//        sb.append("\ttag");
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
        sb.append("\tevalContext");
        sb.append("\tcontroller");
        sb.append("\tbopId");
        sb.append("\tpredId");
        sb.append("\tbopSummary"); // short form of the bop.
        sb.append("\tpredSummary"); // short form of the pred.
        // metadata considered by the static optimizer.
        sb.append("\tstaticBestKeyOrder"); // original key order assigned by static optimizer.
        sb.append("\toverrideKeyOrder"); // key order iff explicitly overridden.
        sb.append("\tnvars"); // #of variables in the predicate for a join.
        sb.append("\tfastRangeCount"); // fast range count used by the static optimizer.
        // dynamics (aggregated for totals as well).
        sb.append("\trunState"); // true iff the operator will not be evaluated again.
        sb.append("\tsumMillis"); // cumulative milliseconds for eval of this operator.
        sb.append("\topCount"); // cumulative #of invocations of tasks for this operator.
        sb.append("\tnumRunning");// #of concurrent invocations of the operator (current value)
        sb.append("\tfanOut"); // #of shards/nodes on which the operator has started.
        sb.append("\tqueueShards"); // #of shards with work queued for this operator.
        sb.append("\tqueueChunks"); // #of chunks queued for this operator.
        sb.append("\tqueueSolutions"); // #of solutions queued for this operator.
        sb.append("\tchunksIn");
        sb.append("\tunitsIn");
        sb.append("\tunitsInPerChunk"); // average #of solutions in per chunk.
        sb.append("\tchunksOut");
        sb.append("\tunitsOut");
        sb.append("\tunitsOutPerChunk"); // average #of solutions out per chunk.
        sb.append("\tmutationCount");
        sb.append("\ttypeErrors");
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
     * @param q
     *            The {@link IRunningQuery}.
     * @param evalOrder
     *            The evaluation order for the operator.
     * @param bopId
     *            The identifier for the operator.
     * @param summary
     *            <code>true</code> iff the summary for the query should be
     *            written.
     *            
     * @return The row of the table.
     */
    static private String getTableRow(final IRunningQuery q,
            final int evalOrder, final Integer bopId, final boolean summary,
            final Map<Integer/*bopId*/,QueueStats> queueStats) {

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
//      sb.append(q.getQuery().getProperty(QueryHints.TAG,
//              QueryHints.DEFAULT_TAG));
//      sb.append('\t');
        sb.append(dateFormat.format(new Date(q.getStartTime())));
        sb.append('\t');
        sb.append(dateFormat.format(new Date(q.getDoneTime())));
        sb.append('\t');
        if(q.getDeadline()!=Long.MAX_VALUE)
            sb.append(dateFormat.format(new Date(q.getDeadline())));
        sb.append('\t');
        sb.append(elapsed);
        sb.append('\t');
        sb.append(serviceId == null ? NA : serviceId.toString());
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
            sb.append('\t'); // evalOrder
            sb.append("total");
            sb.append('\t'); // evaluation context
            sb.append('\t'); // controller annotation.
            sb.append('\t'); // bopId
            sb.append("total");
        } else {
            // Otherwise show just this bop.
            sb.append(bopIndex.get(bopId).toString());
            sb.append('\t');
            sb.append(evalOrder); // eval order for this bop.
            sb.append('\t');
            sb.append(bop.getEvaluationContext());
            sb.append('\t');
            sb.append(bop.getProperty(BOp.Annotations.CONTROLLER,
                    BOp.Annotations.DEFAULT_CONTROLLER));
            sb.append('\t');
            sb.append(Integer.toString(bopId));
        }
        
        sb.append('\t');
        @SuppressWarnings("rawtypes")
        final IPredicate pred = (IPredicate<?>) bop
                .getProperty(PipelineJoin.Annotations.PREDICATE);
        final Integer predId = pred == null ? null : (Integer) pred
                .getProperty(BOp.Annotations.BOP_ID);
        if (predId != null) {
            sb.append(predId);
        } else {
            if (pred != null) {
                // Expected but missing.
                sb.append(NA);
            }
        }
        sb.append('\t');
        // bopSummary
        if (summary) {
            sb.append("total");
        } else {
            sb.append(bop.getClass().getSimpleName());
            sb.append("[" + bopId + "]");
            final Integer defaultSink = (Integer) bop
                    .getProperty(PipelineOp.Annotations.SINK_REF);
            final Integer altSink = (Integer) bop
                    .getProperty(PipelineOp.Annotations.ALT_SINK_REF);
            if (defaultSink != null) {
                sb.append(", sink=" + defaultSink);
            }
            if (altSink != null) {
                sb.append(", altSink=" + altSink);
            }
        }
        sb.append('\t');
        if (pred != null) {
            sb.append(pred.getClass().getSimpleName());
            sb.append("[" + predId + "](");
            final Iterator<BOp> itr = pred.argIterator();
            boolean first = true;
            while (itr.hasNext()) {
                if (first) {
                    first = false;
                } else
                    sb.append(", ");
                final IVariableOrConstant<?> x = (IVariableOrConstant<?>) itr
                        .next();
                if (x.isVar()) {
                    sb.append("?");
                    sb.append(x.getName());
                } else {
                    sb.append(x.get());
                    //sb.append(((IV)x.get()).getValue());
                }
            }
            sb.append(")");
        }
        if (bop.getProperty(NamedSetAnnotations.NAMED_SET_REF) != null) {
            /*
             * Named Solution Set(s) summary.
             */
            final Object namedSetRef = bop
                    .getProperty(NamedSetAnnotations.NAMED_SET_REF);
            if (namedSetRef instanceof INamedSolutionSetRef) {
                final INamedSolutionSetRef ref = (INamedSolutionSetRef) namedSetRef;
                final IRunningQuery t = getRunningQuery(q, ref.getQueryId());
                if (t != null) {
                    final IQueryAttributes attrs = t == null ? null : t
                            .getAttributes();
                    final IHashJoinUtility state = (IHashJoinUtility) (attrs == null ? null
                            : attrs.get(ref));
                    if (state != null) {
                        // Prefer the IHashUtilityState
                        sb.append(state.toString());
                    } else {
                        // Otherwise the NamedSolutionSetRef
                        sb.append(ref.toString());
                    }
                    // sb.append(", joinvars=" + Arrays.toString(ref.joinVars));
                }
            } else {
                final INamedSolutionSetRef[] refs = (INamedSolutionSetRef[]) namedSetRef;
                for (int i = 0; i < refs.length; i++) {
                    final INamedSolutionSetRef ref = refs[i];
                    if (i > 0)
                        sb.append(",");
                    final IRunningQuery t = getRunningQuery(q, ref.getQueryId());
                    if (t != null) {
                        final IQueryAttributes attrs = t == null ? null : t
                                .getAttributes();
                        final IHashJoinUtility state = (IHashJoinUtility) (attrs == null ? null
                                : attrs.get(ref));
                        if (state != null) {
                            // Prefer the IHashUtilityState
                            sb.append(state.toString());
                            sb.append(cdata(",namedSet="));
                            sb.append(cdata(ref.getLocalName()));
                        } else {
                            // Otherwise the NamedSolutionSetRef
                            sb.append(ref.toString());
                        }
                    }
                    // sb.append(", joinvars=" +
                    // Arrays.toString(refs[0].joinVars));
                }
            }
        }
        if (bop instanceof ChunkedMaterializationOp) {
            sb.append(cdata("vars="
                    + Arrays.toString(((ChunkedMaterializationOp) bop)
                            .getVars()) + ",materializeInlineIVs="
                    + ((ChunkedMaterializationOp) bop).materializeInlineIVs()));
        }
        if (bop instanceof GroupByOp) {
            sb.append(cdata(((GroupByOp) bop).getGroupByState().toString()));
            sb.append(cdata(" "));// whitespace to break the line.
            sb.append(cdata(((GroupByOp) bop).getGroupByRewrite().toString()));
        }
        if (bop instanceof DropOp) {
            sb.append(cdata(Arrays.toString(((DropOp)bop).getDropVars())));
        }
        if (bop instanceof ConditionalRoutingOp) {
            sb.append(cdata(((ConditionalRoutingOp) bop).getCondition()
                    .toString()));
        }
        if (bop instanceof JoinGraph) {
            final JoinGraph t = ((JoinGraph) bop);
//            final Path p = t.getPath(q);
//            final Map<PathIds, EdgeSample> samples = t
//                    .getSamples(q);
            sb.append(cdata("sampleType=" + t.getSampleType()));
            sb.append(cdata(", limit=" + t.getLimit()));
            sb.append(cdata(", nedges=" + t.getNEdges()));
//            if (p != null && samples != null) { // Note: breaks table formatting.
//                // Show the RTO discovered join path.
//                w.write("<pre>");
//                w.write(cdata(JGraph.showPath(p, samples)));
//                w.write("</pre>");
//            }
        }
        if (bop instanceof ProjectionOp) {
            sb.append(cdata(Arrays.toString(((ProjectionOp) bop).getVariables())));
        }

        /*
         * Static optimizer metadata.
         * 
         * FIXME Should report [nvars] be the expected asBound #of variables
         * given the assigned evaluation order and the expectation of propagated
         * bindings (optionals may leave some unbound).
         */
        {
            
            if (pred != null) {
            
                // Static optimizer key order (if run).
                final IKeyOrder<?> keyOrder = (IKeyOrder<?>) pred
                        .getProperty(AST2BOpJoins.Annotations.ORIGINAL_INDEX);
                
                // Explicit override of the key order (if given).
                final Object overrideKeyOrder = pred
                        .getProperty(IPredicate.Annotations.KEY_ORDER);

                final Long rangeCount = (Long) pred
                        .getProperty(AST2BOpJoins.Annotations.ESTIMATED_CARDINALITY);
                
                sb.append('\t'); // keyorder
                if (keyOrder != null)
                    sb.append(keyOrder);
                
                sb.append('\t'); // keyorder override.
                if (overrideKeyOrder != null)
                    sb.append(overrideKeyOrder.toString());
                
                sb.append('\t'); // nvars
                if (keyOrder != null)
                    sb.append(pred.getVariableCount(keyOrder));
                
                sb.append('\t'); // rangeCount
                if (rangeCount!= null)
                    sb.append(rangeCount);
                
            } else {
                sb.append('\t'); // keyorder (static optimizer)
                sb.append('\t'); // keyorder (override)
                sb.append('\t'); // nvars
                sb.append('\t'); // rangeCount
            }
        }

        /*
         * Dynamics.
         */

        final int fanOut = ((AbstractRunningQuery) q).getStartedOnCount(bopId);

        final long numRunning = ((AbstractRunningQuery) q)
                .getRunningCount(bopId);

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
        if (bop != null) {
//            if (stats.opCount.get() == 0)
//                sb.append("NotStarted");
//            else
            // Note: This requires a lock!
//            final RunStateEnum runState = ((AbstractRunningQuery) q)
//                    .getRunState(bopId);
            // Note: Barges in if possible, but does not wait for a lock.
            final RunStateEnum runState = ((AbstractRunningQuery) q)
                    .tryGetRunState(bopId);
            sb.append(runState == null ? NA : runState.name());
        } else {
            sb.append(NA);
        }

        sb.append('\t');
        sb.append(stats.elapsed.get());
        sb.append('\t');
        sb.append(stats.opCount.get());
        sb.append('\t');
        sb.append(Long.toString(numRunning));
        sb.append('\t');
        sb.append(Integer.toString(fanOut));
        {
            final QueueStats tmp = queueStats == null ? null : queueStats
                    .get(bopId);
            if (tmp != null) {
                sb.append('\t');
                sb.append(tmp.shardSet.size()); // aka #of work queues.
                sb.append('\t');
                sb.append(tmp.chunkCount);
                sb.append('\t');
                sb.append(tmp.solutionCount);
            } else {
                sb.append('\t');
                sb.append('\t');
                sb.append('\t');
            }
        }
        sb.append('\t');
        sb.append(stats.chunksIn.get());
        sb.append('\t');
        sb.append(stats.unitsIn.get());
        sb.append('\t');
        sb.append(Double.toString(avg(stats.unitsIn.get(), stats.chunksIn.get())));
        sb.append('\t');
        sb.append(stats.chunksOut.get());
        sb.append('\t');
        sb.append(stats.unitsOut.get());
        sb.append('\t');
        sb.append(Double.toString(avg(stats.unitsOut.get(), stats.chunksOut.get())));
        sb.append('\t');
        sb.append(stats.mutationCount.get());
        sb.append('\t');
        sb.append(stats.typeErrors.get());
        sb.append('\t');
        sb.append(unitsIn == 0 ? NA : unitsOut / (double) unitsIn);
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
        // mutations/ms
        sb.append('\t');
        sb.append(elapsed == 0 ? 0 : stats.mutationCount.get() / elapsed);

        sb.append('\n');

        return sb.toString();

    }

    /**
     * Format the data as an (X)HTML table. The table will include a header
     * which declares the columns, a detail row for each operator (optional),
     * and a summary row for the query as a whole.
     * 
     * @param queryStr
     *            The original text of the query (e.g., a SPARQL query)
     *            (optional).
     * @param q
     *            The {@link IRunningQuery}.
     * @param children
     *            The child query(s) -or- <code>null</code> if they are not to
     *            be displayed.
     * @param w
     *            Where to write the table.
     * @param summaryOnly
     *            When <code>true</code> only the summary row will be written.
     * @param maxBopLength
     *            The maximum length to display from {@link BOp#toString()} and
     *            ZERO (0) to display everything. Data longer than this value
     *            will be accessible from a flyover, but not directly visible in
     *            the page.
     * @param clusterStats
     *            When <code>true</code>, provides additional statistics that
     *            are specific to the scale-out cluster architecture.
     * @param detailedStats
     *            When <code>true</code>, provides additional query statistics
     *            that are below the level of detail for most people.
     * @param mutationStats
     *            When <code>true</code>, provides additional statistics about
     *            mutations (only report for inference rules at this time).
     * @throws IOException
     */
    public static void getTableXHTML(//
            final String queryStr,//
            final IRunningQuery q,//
            final IRunningQuery[] children,//
            final Writer w, final boolean summaryOnly, final int maxBopLength,
            final boolean clusterStats, final boolean detailedStats,
            final boolean mutationStats) throws IOException {

        // the table start tag.
        w.write("<table border=\"1\" summary=\"" + attrib("Query Statistics")
                + "\"\n>");

        getTableHeaderXHTML(w, clusterStats, detailedStats, mutationStats);

        // Main query.
        {

            final Map<Integer/* bopId */, QueueStats> queueStats = ((ChunkedRunningQuery) q)
                    .getQueueStats();

            // Summary first.
            getSummaryRowXHTML(queryStr, q, w, queueStats, maxBopLength,
                    clusterStats, detailedStats, mutationStats);

            if (!summaryOnly) {

                // Then the detail rows.
                getTableRowsXHTML(queryStr, q, w, queueStats, maxBopLength,
                        clusterStats, detailedStats, mutationStats);

            }

        }
        
        if (!summaryOnly) {

            // Then the children too.
            
            if (children != null) {

                for (int i = 0; i < children.length; i++) {

                    final IRunningQuery c = children[i];

                    // Repeat the header so we can recognize what follows as a
                    // child query.
                    getTableHeaderXHTML(w, clusterStats, detailedStats,
                            mutationStats);

                    {
                        // Work queue summary for the child query.
                        final Map<Integer/* bopId */, QueueStats> queueStats = ((ChunkedRunningQuery) c)
                                .getQueueStats();

                        // Summary first.
                        getSummaryRowXHTML(null/* queryStr */, c, w,
                                queueStats, maxBopLength, clusterStats,
                                detailedStats, mutationStats);

                        // Then the detail rows.
                        getTableRowsXHTML(null/* queryStr */, c, w, queueStats,
                                maxBopLength, clusterStats, detailedStats,
                                mutationStats);
                        
                    }

                }

            }

        }

        w.write("</table\n>");
        
    }
    
    public static void getTableHeaderXHTML(final Writer w,
            final boolean clusterStats, final boolean detailedStats,
            final boolean mutationStats) throws IOException {

        // header row.
        w.write("<tr\n>");
        /*
         * Common columns for the overall query and for each pipeline operator.
         */
        w.write("<th>queryId</th>");
//        w.write("<th>tag</th>");
        if (detailedStats) {
            w.write("<th>beginTime</th>");
            w.write("<th>doneTime</th>");
        }
        w.write("<th>deadline</th>");// deadline iff specified.
        w.write("<th>elapsed</th>");// elapsed time for query (wall time ms)
        if(clusterStats) {
            w.write("<th>serviceId</th>");
        }
        w.write("<th>cause</th>");
//        w.write("<th>query</th>");
//        w.write("<th>bop</th>");
        /*
         * Columns for each pipeline operator.
         */
        w.write("<th>evalOrder</th>"); // [0..n-1]
        if (clusterStats) {
            w.write("<th>evalContext</th>");
        }
        if (detailedStats) {
            w.write("<th>controller</th>");
            w.write("<th>bopId</th>");
            w.write("<th>predId</th>");
        }
        w.write("<th>bopSummary</th>");
        w.write("<th>predSummary</th>");
        if (detailedStats) {
            w.write("<th>bopAnnotations</th>");
            w.write("<th>predAnnotations</th>");
        }
        // metadata considered by the static optimizer.
        if(detailedStats) {
            w.write("<th>staticBestKeyOrder</th>"); // original key order assigned
                                                    // by static optimizer.
            w.write("<th>overriddenKeyOrder</th>"); // explicit key order override.
        }
        w.write("<th>nvars</th>"); // #of variables in the predicate for a join.
        w.write("<th>fastRangeCount</th>"); // fast range count used by the
                                            // static optimizer.
                                            // dynamics (aggregated for totals as well).
        if (detailedStats) {
            w.write("<th>runState</th>"); // execution state for this operator.
        }
        w.write("<th>sumMillis</th>"); // cumulative milliseconds for eval of this operator.
        if(detailedStats) {
            w.write("<th>opCount</th>"); // cumulative #of invocations of tasks for this operator.
            w.write("<th>numRunning</th>"); // #of concurrent invocations of the operator (current value)
        }
        if(clusterStats) {
            w.write("<th>fanOut</th>"); // #of shards/nodes on which the operator has started.
            w.write("<th>queueShards</th>"); // #of shards with work queued for this operator.
        }
        if(detailedStats) {
            w.write("<th>queueChunks</th>"); // #of chunks queued for this operator.
            w.write("<th>queueSolutions</th>"); // #of solutions queued for this operator.
            w.write("<th>chunksIn</th>"); // #of chunks of solutions in.
        }
        w.write("<th>unitsIn</th>"); // #of solutions in.
        if(detailedStats) {
            w.write("<th>unitsInPerChunk</th>"); // average #of solutions in per chunk.
            w.write("<th>chunksOut</th>"); // #of chunks of solutions out.
        }
        w.write("<th>unitsOut</th>"); // #of solutions out.
        if(detailedStats) {
            w.write("<th>unitsOutPerChunk</th>"); // average #of solutions out per chunk.
        }
        if(mutationStats) {
            w.write("<th>mutationCount</th>");
        }
        w.write("<th>typeErrors</th>"); // #of SPARQL type errors.
        w.write("<th>joinRatio</th>"); // expansion rate multiplier in the solution count.
        if(detailedStats) {
            w.write("<th>accessPathDups</th>"); // #of duplicate APs that were coalesced.
            w.write("<th>accessPathCount</th>"); // #of access paths
            w.write("<th>accessPathRangeCount</th>");
            w.write("<th>accessPathChunksIn</th>"); // #of chunks read from APs.
            w.write("<th>accessPathUnitsIn</th>"); // #of tuples read from APs.
        }
        // dynamics based on elapsed wall clock time.
        if(detailedStats) {
            w.write("<th>");w.write(cdata("solutions/ms"));w.write("</th>");
        }
        if(mutationStats) {
            w.write("<th>");w.write(cdata("mutations/ms"));w.write("</th>");
        }
        //
        // cost model(s)
        //
        w.write("</tr\n>");

    }

    /**
     * Write the table rows.
     * 
     * @param queryStr
     *            The query text (optional).
     * @param q
     *            The {@link IRunningQuery}.
     * @param w
     *            Where to write the rows.
     * @param maxBopLength
     *            The maximum length to display from {@link BOp#toString()} and
     *            ZERO (0) to display everything. Data longer than this value
     *            will be accessible from a flyover, but not directly visible in
     *            the page.
     * @param clusterStats
     *            When <code>true</code>, provides additional statistics that
     *            are specific to the scale-out cluster architecture.
     * @param detailedStats
     *            When <code>true</code>, provides additional query statistics
     *            that are below the level of detail for most people.
     * @param mutationStats
     *            When <code>true</code>, provides additional statistics about
     *            mutations (only report for inference rules at this time).
     *            
     * @throws IOException
     */
    public static void getTableRowsXHTML(final String queryStr,
            final IRunningQuery q, final Writer w,
            final Map<Integer/* bopId */, QueueStats> queueStats,
            final int maxBopLength, final boolean clusterStats,
            final boolean detailedStats, final boolean mutationStats)
            throws IOException {

        final Integer[] order = BOpUtility.getEvaluationOrder(q.getQuery());

        int orderIndex = 0;
        
        for (Integer bopId : order) {

            getTableRowXHTML(queryStr, q, w, orderIndex, bopId,
                    false/* summary */, queueStats, maxBopLength, clusterStats,
                    detailedStats, mutationStats);

            orderIndex++;
            
        }

    }

    /**
     * Return a tabular representation of the query {@link RunState}.
     * 
     * @param queryStr
     *            The query text (optional).
     * @param q
     *            The {@link IRunningQuery}.
     * @param evalOrder
     *            The evaluation order for the operator.
     * @param bopId
     *            The identifier for the operator.
     * @param summary
     *            <code>true</code> iff the summary for the query should be
     *            written.
     * @param maxBopLength
     *            The maximum length to display from {@link BOp#toString()} and
     *            ZERO (0) to display everything.  Data longer than this value
     *            will be accessible from a flyover, but not directly visible
     *            in the page.
     * @param clusterStats
     *            When <code>true</code>, provides additional statistics that
     *            are specific to the scale-out cluster architecture.
     * @param detailedStats
     *            When <code>true</code>, provides additional query statistics
     *            that are below the level of detail for most people.
     * @param mutationStats
     *            When <code>true</code>, provides additional statistics about
     *            mutations (only report for inference rules at this time).
     *            
     * @return The row of the table.
     */
    static private void getTableRowXHTML(final String queryStr,
            final IRunningQuery q, final Writer w, final int evalOrder,
            final Integer bopId, final boolean summary,
            final Map<Integer/* bopId */, QueueStats> queueStats,
            final int maxBopLength, final boolean clusterStats,
            final boolean detailedStats, final boolean mutationStats)
            throws IOException {

        final DateFormat dateFormat = DateFormat.getDateTimeInstance(
                DateFormat.FULL, DateFormat.FULL);
        
        // The elapsed time for the query (wall time in milliseconds).
        final long elapsed = q.getElapsed();
        
        // The serviceId on which the query is running : null unless scale-out.
        final UUID serviceId = q.getQueryEngine().getServiceUUID();
        
        // The thrown cause : null unless the query was terminated abnormally.
        final Throwable cause = q.getCause();
        
        w.write("<tr\n>");
        w.write(TD + cdata(q.getQueryId().toString()) + TDx);
//        w.write(TD
//                + cdata(q.getQuery().getProperty(QueryHints.TAG,
//                        QueryHints.DEFAULT_TAG)) + TDx);
        if(detailedStats) {
            w.write(TD + dateFormat.format(new Date(q.getStartTime())) + TDx);
            w.write(TD + cdata(dateFormat.format(new Date(q.getDoneTime()))) + TDx);
        }
        w.write(TD);
        if (q.getDeadline() != Long.MAX_VALUE)
            w.write(cdata(dateFormat.format(new Date(q.getDeadline()))));
        w.write(TDx);
        w.write(TD + cdata(Long.toString(elapsed)) + TDx);
        if (clusterStats) {
            w.write(TD);
            w.write(cdata(serviceId == null ? NA : serviceId.toString()));
            w.write(TDx);
        }
        w.write(TD);
        if (cause != null) {
            String msg = cause.getLocalizedMessage();
            if (msg == null)
                msg = cause.toString();
            w.write(cdata(msg));
        }
        w.write(TDx);
        
        final Map<Integer, BOp> bopIndex = q.getBOpIndex();
        final Map<Integer, BOpStats> statsMap = q.getStats();
        final BOp bop = bopIndex.get(bopId);

        // the operator.
        if (summary) {
//          // The query string (SPARQL).
//            w.write(TD);
//          w.write(queryStr == null ? cdata(NA) : prettyPrintSparql(queryStr));
//            w.write(TDx);
//            // The query plan (BOPs)
//          {
//              w.write(TD);
//              final String bopStr = BOpUtility.toString(q.getQuery());
//              if (maxBopLength == 0 || bopStr.length() <= maxBopLength) {
//                  // The entire query plan.
//                  w.write(cdata(bopStr));
//              } else {
//                  // A slice of the query plan.
//                  w.write("<a href=\"#\" title=\"");
//                  w.write(attrib(bopStr));// the entire query as a tooltip.
//                  w.write("\"\n>");
//                  w.write(cdata(bopStr.substring(0/* begin */, Math.min(
//                          maxBopLength, bopStr.length()))));
//                  w.write("...");
//                  w.write("</a>");
//              }
//              w.write(TDx);
//          }
            w.write(TD);
            w.write("total"); // evalOrder
            w.write(TDx);
            if (clusterStats) {
                w.write(TD); w.write(TDx); // evalContext
            }
            if (detailedStats) {
                w.write(TD); w.write(TDx); // controller
                w.write(TD);
                w.write("total"); // bopId
                w.write(TDx);
            }
        } else {
//          // The query string (SPARQL).
//            w.write(TD);
//            w.write("...");// elide the original query string on a detail row.
//            w.write(TDx);
//          // The query plan (BOPs)
//          {
//              w.write(TD);
//              final String bopStr = bopIndex.get(bopId).toString();
//              if (maxBopLength == 0 || bopStr.length() <= maxBopLength) {
//                  // The entire query plan.
//                  w.write(cdata(bopStr));
//              } else {
//                  // A slice of the query plan.
//                  w.write("<a href=\"#\" title=\"");
//                  w.write(attrib(bopStr));// the entire query as a tooltip.
//                  w.write("\"\n>");
//                  // A slice of the query inline on the page.
//                  w.write(cdata(bopStr.substring(0/* begin */, Math.min(
//                          maxBopLength, bopStr.length()))));
//                  w.write("...");
//                  w.write("</a>");
//              }
//              w.write(TDx);
//          }
            w.write(TD);
            w.write(Integer.toString(evalOrder)); // eval order for this bop.
            w.write(TDx);
            if (clusterStats) {
                w.write(TD);
                w.write(cdata(bop.getEvaluationContext().toString()));
                w.write(TDx);
            }
            if (detailedStats) {
                w.write(TD);
                w.write(cdata(bop.getProperty(BOp.Annotations.CONTROLLER,
                      BOp.Annotations.DEFAULT_CONTROLLER).toString()));
                w.write(TDx);
                w.write(TD);
                w.write(Integer.toString(bopId));
                w.write(TDx);
            }
        }        

        @SuppressWarnings("rawtypes")
        final IPredicate pred = summary ? null : (IPredicate<?>) bop
                .getProperty(PipelineJoin.Annotations.PREDICATE);
        final Integer predId = pred == null ? null : (Integer) pred
                .getProperty(BOp.Annotations.BOP_ID);
        if (detailedStats) {
            w.write(TD);
            if (predId != null) {
                w.write(cdata(predId.toString()));
            } else {
                if (pred != null) {
                    // Expected but missing.
                    w.write(cdata(NA));
                }
            }
            w.write(TDx);
        }

        // bopSummary
        w.write(TD);
        if (summary) {
            w.write("total");
        } else {
            w.write(cdata(bop.getClass().getSimpleName()));
            w.write(cdata("[" + bopId + "]"));
        }
        w.write(TDx);
        
        /*
         * Pperator summary (not shown for the "total" line).
         * 
         * TODO We should have self-reporting of the summary for each operator,
         * potentially as XHTML. Also, the parser should pass along the SPARQL
         * snip that corresponds to the operator so we can display it here. We
         * already handle this for the SERVICE call's inner graph pattern. It
         * could be handled in general.
         */
        w.write(TD);
        if(!summary) {
            if (pred != null) {
                w.write(cdata(pred.getClass().getSimpleName()));
                w.write(cdata("[" + predId + "]("));
                final Iterator<BOp> itr = pred.argIterator();
                boolean first = true;
                while (itr.hasNext()) {
                    if (first) {
                        first = false;
                    } else
                        w.write(cdata(", "));
                    final IVariableOrConstant<?> x = (IVariableOrConstant<?>) itr
                            .next();
                    if (x.isVar()) {
                        w.write(cdata("?"));
                        w.write(cdata(x.getName()));
                    } else {
                        w.write(cdata(x.get().toString()));
                        //sb.append(((IV)x.get()).getValue());
                    }
                }
                w.write(cdata(")"));
            }
            if (bop.getProperty(NamedSetAnnotations.NAMED_SET_REF) != null) {
                /*
                 * Named Solution Set(s) summary.
                 */
                final Object namedSetRef = bop
                        .getProperty(NamedSetAnnotations.NAMED_SET_REF);
                if (namedSetRef instanceof INamedSolutionSetRef) {
                    final INamedSolutionSetRef ref = (INamedSolutionSetRef) namedSetRef;
                    final IRunningQuery t = getRunningQuery(q, ref.getQueryId());
                    if (t != null) {
                        final IQueryAttributes attrs = t == null ? null : t
                                .getAttributes();
                        final IHashJoinUtility state = (IHashJoinUtility) (attrs == null ? null
                                : attrs.get(ref));
                        if (state != null) {
                            // Prefer the IHashUtilityState
                            w.write(cdata(state.toString()));
                            w.write(cdata(",namedSet="));
                            w.write(cdata(ref.getLocalName()));
                        } else {
                            // Otherwise the NamedSolutionSetRef
                            w.write(cdata(ref.toString()));
                        }
                        // w.write(cdata(", joinvars=" +
                        // Arrays.toString(ref.joinVars)));
                    }
                } else {
                    final INamedSolutionSetRef[] refs = (INamedSolutionSetRef[]) namedSetRef;
                    for (int i = 0; i < refs.length; i++) {
                        final INamedSolutionSetRef ref = refs[i];
                        if (i > 0)
                            w.write(cdata(","));
                        final IRunningQuery t = getRunningQuery(q, ref.getQueryId());
                        if (t != null) {
                            final IQueryAttributes attrs = t == null ? null : t
                                    .getAttributes();
                            final IHashJoinUtility state = (IHashJoinUtility) (attrs == null ? null
                                    : attrs.get(ref));
                            if (state != null) {
                                // Prefer the IHashUtilityState
                                w.write(cdata(state.toString()));
                            } else {
                                // Otherwise the NamedSolutionSetRef
                                w.write(cdata(ref.toString()));
                            }
                        }
                        // w.write(cdata(", joinvars=" +
                        // Arrays.toString(refs[0].joinVars)));
                    }
                }
            }
            if (bop instanceof ChunkedMaterializationOp) {
                w.write(cdata("vars="
                        + Arrays.toString(((ChunkedMaterializationOp) bop)
                                .getVars())
                        + ",materializeInlineIVs="
                        + ((ChunkedMaterializationOp) bop)
                                .materializeInlineIVs()));
            }
            if (bop instanceof GroupByOp) {
                w.write(cdata(((GroupByOp) bop).getGroupByState().toString()));
                if (detailedStats) {
                    w.write(cdata(" "));// whitespace to break the line.
                    w.write(cdata(((GroupByOp) bop).getGroupByRewrite()
                            .toString()));
                }
            }
            if (bop instanceof DropOp) {
                w.write(cdata(Arrays.toString(((DropOp) bop).getDropVars())));
            }
            if (bop instanceof ConditionalRoutingOp) {
                w.write(cdata(((ConditionalRoutingOp) bop).getCondition()
                        .toString()));
            }
            if (bop instanceof JoinGraph) {
                final JoinGraph t = ((JoinGraph) bop);
                final Path p = t.getPath(q);
                final Map<PathIds, EdgeSample> samples = t
                        .getSamples(q);
                w.write(cdata("sampleType=" + t.getSampleType()));
                w.write(cdata(", limit=" + t.getLimit()));
                w.write(cdata(", nedges=" + t.getNEdges()));
                if (p != null && samples != null) {
                    // Show the RTO discovered join path.
                    w.write("<pre>");
                    w.write(cdata(JGraph.showPath(p, samples)));
                    w.write("</pre>");
                }
            }
            if (bop instanceof ProjectionOp) {
                w.write(cdata(Arrays.toString(((ProjectionOp) bop)
                        .getVariables())));
            }
            if (bop instanceof SliceOp) {
                w.write(cdata("offset=" + ((SliceOp) bop).getOffset()));
                w.write(cdata(", limit=" + ((SliceOp) bop).getLimit()));
            }
        }
        w.write(TDx); // end predSummary

        if (detailedStats) {
            // bopAnnotations
            w.write(TD);
            if (!summary) {
                showAnnotations(w, bop.annotations());
            }
            w.write(TDx);
        }

        if (detailedStats) {
            // predAnnotations
            w.write(TD);
            if (pred != null) {
                showAnnotations(w, pred.annotations());
            }
            w.write(TDx);
        }

        /*
         * Static optimizer metadata.
         * 
         * FIXME Should report [nvars] be the expected asBound #of variables
         * given the assigned evaluation order and the expectation of propagated
         * bindings (optionals may leave some unbound).
         */
        {

            if (pred != null) {
            
                // Static optimizer key order (if run).
                final IKeyOrder<?> keyOrder = (IKeyOrder<?>) pred
                        .getProperty(AST2BOpJoins.Annotations.ORIGINAL_INDEX);
                
                // Explicit override of the key order (if given).
                final Object overrideKeyOrder = pred
                        .getProperty(IPredicate.Annotations.KEY_ORDER);

                final Long rangeCount = (Long) pred
                        .getProperty(AST2BOpJoins.Annotations.ESTIMATED_CARDINALITY);

                if (detailedStats) {
                    // static best keyorder
                    w.write(TD);
                    if (keyOrder != null)
                        w.write(keyOrder.toString());
                    w.write(TDx);

                    // override keyorder
                    w.write(TD);
                    if (overrideKeyOrder != null)
                        w.write(overrideKeyOrder.toString());
                    w.write(TDx);
                }

                // nvars
                w.write(TD);
                if (keyOrder != null)
                    w.write(Integer.toString(pred.getVariableCount(keyOrder)));
                w.write(TDx);

                // rangeCount
                w.write(TD);
                if (rangeCount != null)
                    w.write(Long.toString(rangeCount));
                w.write(TDx);

            } else {
                if (detailedStats) {
                    // keyorder (static)
                    w.write(TD);
                    w.write(TDx);
                    // keyorder (override)
                    w.write(TD);
                    w.write(TDx);
                }
                // nvars
                w.write(TD);
                w.write(TDx);
                // rangeCount
                w.write(TD);
                w.write(TDx);
            }
        }

        /*
         * Dynamics.
         */

        final long numRunning = ((AbstractRunningQuery) q)
                .getRunningCount(bopId);

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

        // run state
        if (detailedStats) {
            w.write(TD);
            if (bop != null) {
//            if (stats.opCount.get() == 0)
//                w.write(cdata("NotStarted"));
//            else
            // Note: This requires a lock!
//          final RunStateEnum runState = ((AbstractRunningQuery) q)
//                  .getRunState(bopId);
                // Note: Barges in if possible, but does not wait for a lock.
                final RunStateEnum runState = ((AbstractRunningQuery) q)
                        .tryGetRunState(bopId);
                w.write(cdata(runState == null ? NA : runState.name()));
            } else {
                w.write(cdata(NA));
            }
            w.write(TDx);
        }
        
        w.write(TD);
        w.write(Long.toString(stats.elapsed.get()));
        w.write(TDx);
        if (detailedStats) {
            w.write(TD);
            w.write(Long.toString(stats.opCount.get()));
            w.write(TDx);
            w.write(TD);
            w.write(Long.toString(numRunning));
            w.write(TDx);
        }
        if (clusterStats) {
            final int fanOut = ((AbstractRunningQuery) q)
                    .getStartedOnCount(bopId);
            w.write(TD);
            w.write(Integer.toString(fanOut));
            w.write(TDx);
        }
        /*
         * Statistics about the intermediate solutions awaiting processing.
         */
        {
            final QueueStats tmp = queueStats == null ? null : queueStats
                    .get(bopId);
            if (tmp != null) {
                if (clusterStats) {
                    w.write(TD);
                    w.write(Long.toString(tmp.shardSet.size()));
                    w.write(TDx);
                }
                if (detailedStats) {
                    w.write(TD);
                    w.write(Long.toString(tmp.chunkCount));
                    w.write(TDx);
                    w.write(TD);
                    w.write(Long.toString(tmp.solutionCount));
                    w.write(TDx);
                }
            } else {
                if (clusterStats) { // queueShards
                    w.write(TD);
                    w.write(TDx);
                }
                if (detailedStats) {
                    w.write(TD); // queueChunks
                    w.write(TDx);
                    w.write(TD); // queueSolutions
                    w.write(TDx);
                }
            }
        }
        if (detailedStats) {
            w.write(TD);
            w.write(Long.toString(stats.chunksIn.get()));
            w.write(TDx);
        }
        w.write(TD);
        w.write(Long.toString(stats.unitsIn.get()));
        w.write(TDx);
        if(detailedStats) {
            w.write(TD);
            w.write(Double.toString(avg(stats.unitsIn.get(), stats.chunksIn.get())));
            w.write(TDx);
            w.write(TD);
            w.write(Long.toString(stats.chunksOut.get()));
            w.write(TDx);
        }
        w.write(TD);
        w.write(Long.toString(stats.unitsOut.get()));
        w.write(TDx);
        if(detailedStats) {
            w.write(TD);
            w.write(Double.toString(avg(stats.unitsOut.get(), stats.chunksOut.get())));
            w.write(TDx);
        }
        if (mutationStats) {
            w.write(TD);
            w.write(cdata(Long.toString(stats.mutationCount.get())));
            w.write(TDx);
        }
        w.write(TD);
        w.write(Long.toString(stats.typeErrors.get()));
        w.write(TDx);
        w.write(TD);
        w.write(cdata(unitsIn == 0 ? NA : Double.toString(unitsOut / (double) unitsIn)));
        w.write(TDx);
        if (detailedStats) {
            w.write(TD);
            w.write(Long.toString(stats.accessPathDups.get()));
            w.write(TDx);
            w.write(TD);
            w.write(Long.toString(stats.accessPathCount.get()));
            w.write(TDx);
            w.write(TD);
            w.write(Long.toString(stats.accessPathRangeCount.get()));
            w.write(TDx);
            w.write(TD);
            w.write(Long.toString(stats.accessPathChunksIn.get()));
            w.write(TDx);
            w.write(TD);
            w.write(Long.toString(stats.accessPathUnitsIn.get()));
            w.write(TDx);
        }

        /*
         * Use the total elapsed time for the query (wall time).
         */
        // solutions/ms
        if (detailedStats) {
            w.write(TD);
//            final long solutionCount = stats.unitsOut.get();
//            final String solutionsPerSec = (solutionCount == 0 ? NA //
//                    : (elapsed == 0L ? DZ //
//                            : "" + (long) (solutionCount * 1000d / elapsed)));
            w.write(cdata(elapsed == 0 ? "0" : Long.toString(stats.unitsOut
                    .get() / elapsed)));
            w.write(TDx);
        }
        // mutations/ms
        if (mutationStats) {
            w.write(TD);
            w.write(cdata(elapsed == 0 ? "0" : Long
                    .toString(stats.mutationCount.get() / elapsed)));
            w.write(TDx);
        }
        w.write("</tr\n>");

    }

    /**
     * Shows annotations on a {@link BOp}.
     * 
     * @param w
     *            Where to write the XHTML data.
     * @param anns
     *            The annotations (optional).
     * @throws IOException
     */
    static private void showAnnotations(final Writer w,
            final Map<String, Object> anns) throws IOException {
        if (anns != null && !anns.isEmpty()) {
            w.write("<dl>");
            for (Map.Entry<String, Object> e : anns.entrySet()) {
                w.write("<dt>");
                final String key = e.getKey();
                w.write(cdata(key));
                w.write("</dt><dd>");
                final Object val = e.getValue();
                // See CoreBaseBop for this pattern.
                if (val != null && val.getClass().isArray()) {
                    w.write(cdata(Arrays.toString((Object[]) val)));
                } else if (key.equals(IPredicate.Annotations.FLAGS)) {
                    w.write(cdata(Tuple.flagString((Integer) val)));
                } else if (val instanceof BOp) {
                    w.write(cdata(((BOp) val).toShortString()));
                } else {
                    w.write(cdata("" + val));
                }
                w.write("</dd>");
            }
            w.write("</dl>");
        }
    }

    /**
     * Write a summary row for the query. The table element, header, and footer
     * must be written separately.
     * 
     * @param queryStr
     *            The original query text (optional).
     * @param q
     *            The {@link IRunningQuery}.
     * @param w
     *            Where to write the data.
     * @param maxBopLength
     *            The maximum length to display from {@link BOp#toString()} and
     *            ZERO (0) to display everything. Data longer than this value
     *            will be accessible from a flyover, but not directly visible in
     *            the page.
     * @param clusterStats
     *            When <code>true</code>, provides additional statistics that
     *            are specific to the scale-out cluster architecture.
     * @param detailedStats
     *            When <code>true</code>, provides additional query statistics
     *            that are below the level of detail for most people.
     * @param mutationStats
     *            When <code>true</code>, provides additional statistics about
     *            mutations (only report for inference rules at this time).
     *            
     * @throws IOException
     */
    static private void getSummaryRowXHTML(final String queryStr,
            final IRunningQuery q, final Writer w,
            final Map<Integer/* bopId */, QueueStats> queueStats,
            final int maxBopLength, final boolean clusterStats,
            final boolean detailedStats, final boolean mutationStats)
            throws IOException {

        getTableRowXHTML(queryStr, q, w, -1/* orderIndex */, q.getQuery()
                .getId(), true/* summary */, queueStats, maxBopLength,
                clusterStats, detailedStats, mutationStats);

    }

    private static String cdata(final String s) {
        
        return XHTMLRenderer.cdata(s);
        
    }

    private static String attrib(final String s) {
        
        return XHTMLRenderer.attrib(s);
        
    }

//    private static String prettyPrintSparql(String s) {
//
////        return cdata(s);
////        
////    }
//    
//      s = s.replace("\n", " ");
//      
//      s = s.replace("PREFIX", "\nPREFIX");
//      s = s.replace("select", "\nselect");
//      s = s.replace("where", "\nwhere");
//      s = s.replace("{","{\n");
//      s = s.replace("}","\n}");
//      s = s.replace(" ."," .\n"); // TODO Must not match within quotes (literals) or <> (URIs).
////        s = s.replace("||","||\n");
////        s = s.replace("&&","&&\n");
//      
//      s = cdata(s);
//      
//      s = s.replace("\n", "<br>");
//      
////        return "<pre>"+s+"</pre>";
//      
//      return s;
//      
//    }

    /**
     * Return <code>x/y</code> unless <code>y:=0</code>, in which case return
     * ZERO (0).
     * 
     * @param x
     *            The numerator.
     * @param y
     *            The denomerator.
     *            
     * @return The average.
     */
    static private double avg(final long x, final long y) {
        
        if (y == 0)
            return 0d;
        
        return x / (double) y;
        
    }
    
    /**
     * Return the {@link IRunningQuery} for that queryId iff it is available.
     * 
     * @param q
     *            The query that you already have.
     * @param queryId
     *            The {@link UUID} of the desired query.
     * 
     * @return The {@link IRunningQuery} iff it can be found and otherwise
     *         <code>null</code>.
     */
    static private IRunningQuery getRunningQuery(final IRunningQuery q,
            final UUID queryId) {

		if (queryId == null) {
			/**
			 * @see <a href="https://jira.blazegraph.com/browse/BLZG-1493" > NPE
			 *      in nested star property paths </a>. When queryId is null, it
			 *      refers to the current query.
			 */
			return q;
		}
    	
        if (q.getQueryId().equals(queryId)) {

            /*
             * Avoid lookup perils if we already have the right query.
             */

            return q;

        }

        try {

            return q.getQueryEngine().getRunningQuery(queryId);
            
        } catch (RuntimeException t) {
            
            // Done and gone.
            return null;
            
        }

    }
    
}
