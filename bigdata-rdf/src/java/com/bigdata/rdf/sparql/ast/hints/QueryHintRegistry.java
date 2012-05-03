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
 * Created on Nov 22, 2011
 */

package com.bigdata.rdf.sparql.ast.hints;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.bigdata.rdf.sparql.ast.FunctionRegistry.Factory;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * A factory which is used to register and resolve query hints.
 * 
 * TODO Query hint actions should not be extendable once the system is up. E.g.,
 * something at least a little bit protected. This is because the query hints
 * have access to the {@link AST2BOpContext}.
 * 
 * TODO Query hints for includeInferred, timeout/deadline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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

        add(new RunFirstHint());
        add(new RunLastHint());
        add(new RunOnceHint());
        add(new OptimizerQueryHint());
        add(new OptimisticQueryHint());

        add(new AnalyticQueryHint());
        add(new NativeDistinctQueryHint());
        add(new NativeDistinctSPOHint());
        add(new NativeDistinctSPOThresholdHint());
        add(new NativeHashJoinsHint());
        add(new MergeJoinHint());
        add(new HashJoinHint());
        add(new KeyOrderHint());
        add(new RemoteAPHint());
        add(new AccessPathSampleLimitHint());
        add(new AccessPathScanAndFilterHint());

        /*
         * BufferAnnotations
         * 
         * TODO The buffer annotations should probably be applied to any
         * IJoinNode, but I have not reviewed the code paths for join group
         * nodes, etc. to make sure that the annotations would be respected if
         * we hang them off of anything other than a statement pattern.
         */
        add(new BufferChunkOfChunksCapacityHint());
        add(new BufferChunkCapacityHint());
        add(new ChunkSizeHint());

        /*
         * PipelineOp annotations.
         * 
         * TODO The pipeline annotations should probably be applied to any
         * IJoinNode, but I have not reviewed the code paths for join group
         * nodes, etc. to make sure that the annotations would be respected if
         * we hang them off of anything other than a statement pattern.
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

    }

}
