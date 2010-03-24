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
 * Created on Jul 9, 2008
 */

package com.bigdata.rdf.rules;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;

import org.apache.log4j.Logger;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.axioms.Axioms;
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
import com.bigdata.relation.rule.eval.IRuleTaskFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.IStepTask;

/**
 * Factory for {@link RDFJoinNexus} objects.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFJoinNexusFactory implements IJoinNexusFactory {

    protected static final transient Logger log = Logger.getLogger(RDFJoinNexusFactory.class);

    protected static final transient boolean INFO = log.isInfoEnabled();
    
    /**
     * 
     */
    private static final long serialVersionUID = 8270873764858640472L;

    final RuleContextEnum ruleContext;
    final ActionEnum action;
    final long writeTimestamp;
    /*final*/ long readTimestamp;
    final boolean justify;
    final boolean backchain;
    final boolean isOwlSameAsUsed;
//    final boolean forceSerialExecution;
//    final int maxParallelSubqueries;
//    final int chunkOfChunksCapacity;
//    final int chunkCapacity;
//    final long chunkTimeout;
//    final int fullyBufferedReadThreshold;
    final Properties properties;
    final int solutionFlags;
    @SuppressWarnings("unchecked")
	final IElementFilter filter;
    final IEvaluationPlanFactory planFactory;
    final IRuleTaskFactory defaultRuleTaskFactory;

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getSimpleName());

        sb.append("{ ruleContext=" + ruleContext);

        sb.append(", action=" + action);

        sb.append(", writeTime=" + writeTimestamp);

        sb.append(", readTime=" + readTimestamp);

        sb.append(", justify=" + justify);

        sb.append(", backchain=" + backchain);

        sb.append(", isOwlSameAsUsed=" + isOwlSameAsUsed);

        sb.append(", properties=" + properties);
        
//        sb.append(", forceSerialExecution="+forceSerialExecution);
//        
//        sb.append(", maxParallelSubqueries="+maxParallelSubqueries);
//        
//        sb.append(", chunkOfChunksCapacity="+chunkOfChunksCapacity);
//        
//        sb.append(", chunkCapacity="+chunkCapacity);
//
//        sb.append(", chunkTimeout="+chunkTimeout);
//
//        sb.append(", fullyBufferedReadThreshold="+fullyBufferedReadThreshold);
        
        sb.append(", solutionFlags=" + solutionFlags);

        sb.append(", filter="
                + (filter == null ? "N/A" : filter.getClass().getName()));

        sb.append(", planFactory=" + planFactory.getClass().getName());

        sb.append(", defaultRuleTaskFactory="
                + defaultRuleTaskFactory.getClass().getName());

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
     * @param isOwlSameAsUsed
     *            <code>true</code> iff {@link Axioms#isOwlSameAs()} AND
     *            <code>(x owl:sameAs y)</code> is not empty in the data.
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
     * @param defaultRuleTaskFactory
     *            The factory that will be used to generate the
     *            {@link IStepTask} to execute an {@link IRule} unless the
     *            {@link IRule} explicitly specifies a factory object using
     *            {@link IRule#getTaskFactory()}.
     */
	public RDFJoinNexusFactory(//
	        final RuleContextEnum ruleContext,//
	        final ActionEnum action,//
            final long writeTimestamp,//
            final long readTimestamp,//
//            boolean forceSerialExecution, int maxParallelSubqueries,//
            final boolean justify, //
            final boolean backchain, //
            final boolean isOwlSameAsUsed, 
//            int chunkOfChunksCapacity, int chunkCapacity, long chunkTimeout,
//            int fullyBufferedReadThreshold,
            final Properties properties,//
            final int solutionFlags, //
            final IElementFilter filter,//
            final IEvaluationPlanFactory planFactory,//
            final IRuleTaskFactory defaultRuleTaskFactory
            ) {

        if (ruleContext == null)
            throw new IllegalArgumentException();

        if (action == null)
            throw new IllegalArgumentException();

//        if (maxParallelSubqueries < 0)
//            throw new IllegalArgumentException();

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

        this.isOwlSameAsUsed = isOwlSameAsUsed;
        
//        this.forceSerialExecution = forceSerialExecution;
//        
//        this.maxParallelSubqueries = maxParallelSubqueries;
//        
//        this.chunkOfChunksCapacity = chunkOfChunksCapacity;
//        
//        this.chunkCapacity = chunkCapacity;
//        
//        this.chunkTimeout = chunkTimeout;
//        
//        this.fullyBufferedReadThreshold = fullyBufferedReadThreshold;

        this.properties = properties;
        
        this.solutionFlags = solutionFlags;

        this.filter = filter;
        
        this.planFactory = planFactory;
        
        this.defaultRuleTaskFactory = defaultRuleTaskFactory;

        joinNexusCache = new WeakHashMap<IIndexManager, WeakReference<IJoinNexus>>();

    }
    
    // @todo refactor singleton factory into base class or utility class.
	// @todo assumes one "central" relation (SPORelation).
    public IJoinNexus newInstance(final IIndexManager indexManager) {

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

    private transient WeakHashMap<IIndexManager, WeakReference<IJoinNexus>> joinNexusCache;
    
    public long getReadTimestamp() {
        
        return readTimestamp;
        
    }

    public void setReadTimestamp(final long readTimestamp) {

        if (this.readTimestamp == readTimestamp) {

            // NOP.
            return;

        }
        
        synchronized(joinNexusCache) {
            
            // discard cache since advancing the readTimestamp.
            joinNexusCache.clear();
            
            this.readTimestamp = readTimestamp; 
            
            if(INFO)
                log.info("readTimestamp: "+readTimestamp);
            
        }
        
    }

    public long getWriteTimestamp() {
        
        return writeTimestamp;
        
    }

    private void readObject(java.io.ObjectInputStream in)
         throws IOException, ClassNotFoundException {
        
        /*
         * Note: Must be explicitly allocated when de-serialized.
         */

        joinNexusCache = new WeakHashMap<IIndexManager, WeakReference<IJoinNexus>>();

        in.defaultReadObject();

    }

}
