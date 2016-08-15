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
 * Created on Nov 22, 2011
 */

package com.bigdata.rdf.sparql.ast.hints;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.bigdata.bop.join.IHashJoinUtility;
import com.bigdata.rdf.sparql.ast.FunctionRegistry.Factory;

/**
 * A factory which is used to register and resolve query hints.
 * 
 * TODO Query hints for includeInferred, timeout/deadline, the "noJoinVarsLimit"
 * at which we break an unconstrained hash join (see the
 * {@link IHashJoinUtility} implementation classes).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class QueryHintRegistry {

    private static ConcurrentMap<String/* name */, IQueryHint<?>> registry = new ConcurrentHashMap<String/* name */, IQueryHint<?>>();

    /**
     * Register an {@link IQueryHint}.
     * 
     * @param The
     *            query hint.
     * 
     * @throws UnsupportedOperationException
     *             if there is already a {@link Factory} registered for that
     *             URI.
     */
    public static final void add(final IQueryHint<?> queryHint) {

        if (registry.putIfAbsent(queryHint.getName(), queryHint) != null) {

            throw new UnsupportedOperationException("Already declared.");

        }

    }

    /**
     * Return the {@link IQueryHint} under that name.
     * 
     * @param name
     *            The name of the {@link IQueryHint}.
     *            
     * @return The {@link IQueryHint} -or- <code>null</code> if there is none
     *         registered for that name.
     */
    public static final IQueryHint<?> get(final String name) {
        
        return registry.get(name);
        
    }
    
    /*
     * Register implementations.
     * 
     * Note: Most query hints are declared by the QueryHints class. However,
     * there are some which are "hidden", or at least not disclosed in the same
     * fashion. These tend to be knobs that users should not be messing with
     * directly.
     */
    static {

        add(new QueryIdHint());

        // Optimizer hints.
        add(new RunFirstHint());
        add(new RunLastHint());
        add(new RunOnceHint());
        add(new OptimizerQueryHint());
        add(new RTOSampleTypeQueryHint());
        add(new RTOLimitQueryHint());
        add(new RTONEdgesQueryHint());
        add(new OptimisticQueryHint());
        add(new NormalizeFilterExpressionHint());

        // Analytic query mode.
        add(new AnalyticQueryHint());
        add(new QueryEngineChunkHandlerQueryHint());
        add(new NativeDistinctQueryHint());
        add(new NativeDistinctSPOHint());
        add(new NativeDistinctSPOThresholdHint());
        add(new NativeHashJoinsHint());
        
        // JOIN hints.
        add(new MergeJoinHint());
        add(new HashJoinHint());
        add(new KeyOrderHint());
        add(new RemoteAPHint());
        add(new AccessPathSampleLimitHint());
        add(new AccessPathScanAndFilterHint());
        add(new NumTasksPerThreadHint());
        add(new MinDatapointsPerTaskHint());
        
        // DESCRIBE
        add(new DescribeModeHint());
        add(new DescribeIterationLimitHint());
        add(new DescribeStatementLimitHint());

        // CONSTRUCT
        add(new ConstructDistinctSPOHint());
        
        /*
         * BufferAnnotations
         * 
         * Note: The buffer annotations should be applied to any PipelineOp.
         * They control the vectoring out of the pipeline operator, which sets
         * up the vectoring for the downstream operator(s).
         */
        add(new BufferChunkOfChunksCapacityHint());
        add(new BufferChunkCapacityHint());
        add(new ChunkSizeHint());

        /*
         * PipelineOp annotations.
         * 
         * Note: The pipeline annotations should be applied to any PipelineOp.
         * They control the vectoring and parallelism of pipeline operators.
         * 
         * TODO Support MAX_MEMORY, but it should only be applied if the
         * operator in question is running against the native heap.
         */
        add(new AtOnceHint());
        add(new PipelineMaxParallelHint());
        add(new PipelineMaxMessagesPerTaskHint());
        add(new PipelineQueueCapacityHint());

        /*
         * Mark a statement pattern as "range safe", which in effect means it 
         * uses only one datatype in it value space (for bindings for O) and
         * that the filters in the query are respecting that datatype.
         */
        add(new RangeHint());
        
        /*
         * Limit the input into joins by limiting the number of elements read
         * from an access path.  Not exactly a cutoff join, which limits output
         * from the join rather than input into it.
         */
        add(new CutoffLimitHint());

        /**
         * FILTER (NOT) EXISTS evaluation strategy hint.
         */
        add(new FilterExistsHint());
        
        /*
         * Mark a statement pattern to include history (SPOs where 
         * type == StatementEnum.History, which are normally hidden from view). 
         */
        add(new HistoryHint());
        
        /*
         * Selectively enable/disbale usage of pipelined hash joins.
         */
        add(new PipelinedHashJoinHint());
        
        /*
         * Disable default graph distinct filter
         */
        add(new DefaultGraphDistinctFilterHint());
        
        /*
         * Automatically convert non-String Literals to strings for SPARQL REGEX
         * 
         * {@see BLZG-1780}
         */
        add(new RegexMatchNonStringHint());
        
    }

}
