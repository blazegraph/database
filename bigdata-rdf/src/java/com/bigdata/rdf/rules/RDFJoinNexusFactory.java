/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jul 9, 2008
 */

package com.bigdata.rdf.rules;

import java.lang.ref.WeakReference;
import java.util.WeakHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IEvaluationPlan;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;

/**
 * Factory for {@link RDFJoinNexus} objects.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFJoinNexusFactory implements IJoinNexusFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 8270873764858640472L;
   
    final RuleContextEnum ruleContext;
    final ActionEnum action;
    final long writeTimestamp;
    final long readTimestamp;
    final boolean justify;
    final boolean backchain;
    final boolean forceSerialExecution;
    final int maxParallelSubqueries;
    final int chunkOfChunksCapacity;
    final int chunkCapacity;
    final long chunkTimeout;
    final int fullyBufferedReadThreshold;
    final int solutionFlags;
    @SuppressWarnings("unchecked")
	final IElementFilter filter;
    final IEvaluationPlanFactory planFactory;

    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getSimpleName());
        
        sb.append("{ ruleContext="+ruleContext);

        sb.append(", action="+action);

        sb.append(", writeTime="+writeTimestamp);
        
        sb.append(", readTime="+readTimestamp);
        
        sb.append(", justify="+justify);
        
        sb.append(", backchain="+backchain);
        
        sb.append(", forceSerialExecution="+forceSerialExecution);
        
        sb.append(", maxParallelSubqueries="+maxParallelSubqueries);
        
        sb.append(", chunkOfChunksCapacity="+chunkOfChunksCapacity);
        
        sb.append(", chunkCapacity="+chunkCapacity);

        sb.append(", chunkTimeout="+chunkTimeout);

        sb.append(", fullyBufferedReadThreshold="+fullyBufferedReadThreshold);
        
        sb.append(", solutionFlags="+solutionFlags);
        
        sb.append(", filter="+(filter==null?"N/A":filter.getClass().getName()));

        sb.append(", planFactory="+planFactory.getClass().getName());

        sb.append("}");
        
        return sb.toString();
        
    }

	/**
     * 
     * @param action
     *            Indicates whether this is a Query, Insert, or Delete
     *            operation.
     * @param writeTimestamp
     *            The timestamp of the relation view(s) using to write on the
     *            {@link IMutableRelation}s (ignored if you are not execution
     *            mutation programs).
     * @param readTimestamp
     *            The timestamp of the relation view(s) used to read from the
     *            access paths.
     * @param forceSerialExecution
     *            When <code>true</code>, rule sets will be forced to execute
     *            sequentially even when they are not flagged as a sequential
     *            program.
     * @param maxParallelSubqueries
     *            The maximum #of subqueries for the first join dimension that
     *            will be issued in parallel. Use ZERO(0) to avoid submitting
     *            tasks to the {@link ExecutorService} entirely and ONE (1) to
     *            submit a single task at a time to the {@link ExecutorService}.
     * @param justify
     *            if justifications are required.
     * @param backchain
     *            Normally <code>true</code> for high level query and
     *            <code>false</code> for database-at-once-closure and Truth
     *            Maintenance. When <code>true</code>, query time inferences
     *            are included when reading on an {@link IAccessPath} for the
     *            {@link SPORelation} using the {@link InferenceEngine} to
     *            "backchain" any necessary entailments.
     * @param chunkOfChunksCapacity
     *            The capacity of the thread-safe {@link IBuffer}s used to
     *            buffer chunks of {@link ISolution}s, often from concurrent
     *            producers. This may be zero to use a {@link SynchronousQueue}
     *            instead of a {@link BlockingQueue} of the given capacity.
     * @param chunkCapacity
     *            The capacity of the {@link UnsynchronizedArrayBuffer}s used
     *            to buffer a single chunk of {@link ISolution}s.
     * @param chunkTimeout
     *            The timeout in milliseconds that the {@link BlockingBuffer}
     *            will wait for another chunk to combine with the current chunk
     *            before returning the current chunk. This may be ZERO (0) to
     *            disable the chunk combiner.
     * @param fullyBufferedReadThreshold
     *            If the estimated range count for an
     *            {@link IAccessPath#iterator(int, int)} is LTE to this
     *            threshold, then use a fully buffered (synchronous) iterator.
     *            Otherwise use an asynchronous iterator whose internal queue
     *            capacity is specified by <i>queryBufferCapacity</i>.
     * @param solutionFlags
     *            Flags controlling the behavior of
     *            {@link #newSolution(IRule, IBindingSet)}.
     * @param filter
     *            An optional filter that will be applied to keep matching
     *            elements out of the {@link IBuffer} for Query or Mutation
     *            operations.
     * @param planFactory
     *            The factory used to generate {@link IEvaluationPlan}s for
     *            {@link IRule}s.
     */
	public RDFJoinNexusFactory(RuleContextEnum ruleContext, ActionEnum action,
            long writeTimestamp, long readTimestamp,//
            boolean forceSerialExecution, int maxParallelSubqueries,//
            boolean justify, boolean backchain, int chunkOfChunksCapacity,
            int chunkCapacity, long chunkTimeout, int fullyBufferedReadThreshold,
            int solutionFlags, IElementFilter filter,
            IEvaluationPlanFactory planFactory) {

        if (ruleContext == null)
            throw new IllegalArgumentException();

        if (action == null)
            throw new IllegalArgumentException();

        if (maxParallelSubqueries < 0)
            throw new IllegalArgumentException();

        if (planFactory == null)
            throw new IllegalArgumentException();

        this.ruleContext = ruleContext;

        this.action = action;
        
        this.writeTimestamp = writeTimestamp;

        this.readTimestamp = readTimestamp;

        this.justify = justify;

//        this.backchain = ruleContext == RuleContextEnum.HighLevelQuery ? true
//                : false;

        this.backchain = backchain;
        
        this.forceSerialExecution = forceSerialExecution;
        
        this.maxParallelSubqueries = maxParallelSubqueries;
        
        this.chunkOfChunksCapacity = chunkOfChunksCapacity;
        
        this.chunkCapacity = chunkCapacity;
        
        this.chunkTimeout = chunkTimeout;
        
        this.fullyBufferedReadThreshold = fullyBufferedReadThreshold;
        
        this.solutionFlags = solutionFlags;

        this.filter = filter;
        
        this.planFactory = planFactory;

    }

    //@todo refactor singleton factory into base class or utility class.
    public IJoinNexus newInstance(IIndexManager indexManager) {

        synchronized (joinNexusCache) {

            final WeakReference<IJoinNexus> ref = joinNexusCache
                    .get(indexManager);

            IJoinNexus joinNexus = ref == null ? null : ref.get();

            if (joinNexus == null) {

                joinNexus = new RDFJoinNexus(this, indexManager);

                joinNexusCache.put(indexManager, new WeakReference<IJoinNexus>(
                        joinNexus));

            }

            return joinNexus;

        }
        
    }

    private final transient WeakHashMap<IIndexManager, WeakReference<IJoinNexus>> joinNexusCache = new WeakHashMap<IIndexManager, WeakReference<IJoinNexus>>();
    
    public long getReadTimestamp() {
        
        return readTimestamp;
        
    }

    public long getWriteTimestamp() {
        
        return writeTimestamp;
        
    }

}
