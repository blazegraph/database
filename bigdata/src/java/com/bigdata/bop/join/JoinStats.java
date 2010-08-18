package com.bigdata.bop.join;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.join.PipelineJoin.JoinTask;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.IRuleState;
import com.bigdata.relation.rule.eval.RuleStats;
//import com.bigdata.relation.rule.eval.pipeline.JoinMasterTask;
//import com.bigdata.relation.rule.eval.pipeline.JoinTask.AccessPathTask;

/**
 * Statistics about processing for a {@link PipelineJoin} as reported by a
 * single {@link JoinTask}. Each {@link JoinTask} handles a single index
 * partition, so the {@link JoinStats} for those index partitions are reported
 * to the client coordinating the query execution for aggregation and reporting.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JoinStats implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 9028650921831777131L;

    /**
     * The timestamp associated with the start of execution for the join
     * dimension. This is not aggregated. The timestamp is assigned when the
     * {@link JoinStats} object is created. That corresponds either to the start
     * of the distributed {@link JoinMasterTask} execution (aggregated level) or
     * to the start of some specific {@link JoinTask} (detail level).
     */
    public final long startTime;
    
    /**
     * The index partition for which these statistics were collected or -1
     * if the statistics are aggregated across index partitions.
     */
    public final int partitionId;

    /**
     * The index in the evaluation order whose statistics are reported here.
     */
    public final int orderIndex;

    /**
     * The maximum observed fan in for this join dimension (maximum #of sources
     * observed writing on any join task for this join dimension). Since join
     * tasks may be closed and new join tasks re-opened for the same query, join
     * dimension and index partition, and since each join task for the same join
     * dimension could, in principle, have a different fan in based on the
     * actual binding sets propagated this is not necessarily the "actual" fan
     * in for the join dimension.
     */
    public int fanIn;

    /**
     * The maximum observed fan out for this join dimension (maximum #of sinks
     * on which any join task is writing for this join dimension). Since join
     * tasks may be closed and new join tasks re-opened for the same query, join
     * dimension and index partition, and since each join task for the same join
     * dimension could, in principle, have a different fan out based on the
     * actual binding sets propagated this is not necessarily the "actual" fan
     * out for the join dimension.
     */
    public int fanOut;

    /**
     * The #of index partitions for which join tasks were created for this join
     * dimension. This is computed by explicitly tracking the distinct index
     * partition identifiers reported for the join dimension. This is the "real"
     * fan out for the prior join dimension.
     */
    public int partitionCount;

    /**
     * Map used to track the #of distinct partition identifiers for this join
     * dimension.
     */
    transient private IntSet partitionIds;
    
    /**
     * The #of binding set chunks read from all source {@link JoinTask}s.
     */
    public long bindingSetChunksIn;

    /** The #of binding sets read from all source {@link JoinTask}s. */
    public long bindingSetsIn;

    /**
     * The #of {@link IAccessPath}s read. This will differ from
     * {@link #bindingSetIn} iff the same {@link IBindingSet} is read from
     * more than one source and the {@link JoinTask} is able to recognize
     * the duplication and collapse it by removing the duplicate(s).
     */
    public long accessPathCount;
    
    /**
     * The #of duplicate {@link IAccessPath}s that were eliminated by a
     * {@link JoinTask}. Duplicate {@link IAccessPath}s arise when the
     * source {@link JoinTask}(s) generate the bindings on the
     * {@link IPredicate} for a join dimension. Duplicates are detected by a
     * {@link JoinTask} when it generates chunk of distinct
     * {@link AccessPathTask}s from a chunk of {@link IBindingSet}s read
     * from its source(s) {@link JoinTask}s.
     * <p>
     * Note: While the {@link IPredicate}s for those tasks may have the
     * same bindings, the source {@link IBindingSet}s typically (always?)
     * have variety not represented in the bound {@link IPredicate} and
     * therefore are combined under a single {@link AccessPathTask}. This
     * reduces redundant reads on an {@link IAccessPath} while producing
     * exactly the same output {@link IBindingSet}s that would have been
     * produced if we did not identify the duplicate {@link IAccessPath}s.
     */
    public long accessPathDups;

    /** #of chunks visited over all access paths. */
    public long chunkCount;

    /** #of elements visited over all chunks. */
    public long elementCount;

    /**
     * The #of {@link IBindingSet}s written onto the next join dimension
     * (aka the #of solutions written iff this is the last join dimension).
     * <p>
     * Note: An {@link IBindingSet} can be written onto more than one index
     * partition for the next join dimension, so one generated
     * {@link IBindingSet} MAY result in N GTE ONE "binding sets out". This
     * occurs when the {@link IAccessPath} required to read on the next
     * {@link IPredicate} in the evaluation order spans more than one index
     * partition.
     */
    public long bindingSetsOut;

    /**
     * The #of {@link IBindingSet} chunks written onto the next join
     * dimension (aka the #of solutions written iff this is the last join
     * dimension in the evaluation order).
     */
    public long bindingSetChunksOut;

    /**
     * The mutationCount is the #of solutions output by a {@link JoinTask}(s)
     * for the last join dimension of a mutation operation that were not
     * already present in the target relation. This value is always zero
     * (0L) for query.
     * <p>
     * Note: The mutationCount MUST be obtained from {@link IBuffer#flush()}
     * for the buffer on which the {@link JoinTask}(s) for the last join
     * dimension write their solutions. For mutation, this buffer is
     * obligated to report the #of elements whose state was changed in the
     * target relation. Failure to correctly obey this contract can result
     * in non-termination of fix point closure operations.
     * 
     * @see RuleStats#mutationCount
     */
    public AtomicLong mutationCount = new AtomicLong();
    
    /**
     * Ctor variant used by the {@link JoinMasterTask} to aggregate
     * statistics across the index partitions for a given join dimension.
     * 
     * @param orderIndex
     *            The index in the evaluation order.
     */
    public JoinStats(final int orderIndex) {

        this(-1, orderIndex);
        
    }

    /**
     * Ctor variant used by a {@link JoinTask} to self-report.
     * 
     * @param partitionId
     *            The index partition identifier.
     * @param orderIndex
     *            The index in the evaluation order.
     */
    public JoinStats(final int partitionId, final int orderIndex) {

        this.startTime = System.currentTimeMillis();

        this.partitionId = partitionId;

        this.orderIndex = orderIndex;

        fanIn = fanOut = 0;

        // either zero or one depending on the ctor.
        partitionCount = partitionId == -1 ? 0 : 1;
        
        bindingSetChunksIn = bindingSetsIn = 0L;
        
        accessPathCount = accessPathDups = 0L;

        chunkCount = elementCount = bindingSetsOut = 0L;

        bindingSetChunksOut = 0L;

    }

    synchronized void add(final JoinStats o) {

        if (this.orderIndex != o.orderIndex)
            throw new IllegalArgumentException();

        if (partitionIds == null) {
            /*
             * Track the distinct partition identifiers for which join tasks
             * were created for this join dimension. This gives us the real
             * fanOut of the distributed join. However, this is the fanOut
             * across the entire execution of the join. The maximum concurrent
             * fanOut is just [fanOut].
             */
            partitionIds = new IntOpenHashSet();
        }
        if (partitionIds.add(o.partitionId)) {
            // one more distinct partition identifier.
            partitionCount++;
        }
        
        if (o.fanIn > this.fanIn) {
            // maximum reported fanIn for this join dimension.
            this.fanIn = o.fanIn;
        }
        if (o.fanOut > this.fanOut) {
            // maximum reported fanOut for this join dimension.
            this.fanOut += o.fanOut;
        }
        this.bindingSetChunksIn += o.bindingSetChunksIn;
        this.bindingSetsIn += o.bindingSetsIn;
        this.accessPathCount += o.accessPathCount;
        this.accessPathDups += o.accessPathDups;
        this.chunkCount += o.chunkCount;
        this.elementCount += o.elementCount;
        this.bindingSetsOut += o.bindingSetsOut;
        this.bindingSetChunksOut += o.bindingSetChunksOut;
        this.mutationCount.addAndGet(o.mutationCount.get());

    }

    public String toString() {
        
        final StringBuilder sb = new StringBuilder("JoinStats");
        
        sb.append("{ orderIndex="+orderIndex);
        
        sb.append(", partitionId="+partitionId);
        
        sb.append(", fanIn="+fanIn);
        
        sb.append(", fanOut="+fanOut);

        sb.append(", partitionIdCount="+partitionCount);
        
        sb.append(", bindingSetChunksIn="+bindingSetChunksIn);
        
        sb.append(", bindingSetsIn="+bindingSetsIn);
        
        sb.append(", accessPathCount="+accessPathCount);
        
        sb.append(", accessPathDups="+accessPathDups);
        
        sb.append(", chunkCount="+chunkCount);
        
        sb.append(", elementCount="+elementCount);
        
        sb.append(", bindingSetsOut="+bindingSetsOut);

        sb.append(", bindingSetChunksOut="+bindingSetChunksOut);

        sb.append(", mutationCount="+mutationCount);
        
        sb.append("}");
        
        return sb.toString();
        
    }

    static private final transient String sep = ", ";
    
    /**
     * Formats the array of {@link JoinStats} into a CSV table view.
     * 
     * @param rule
     *            The {@link IRule} whose {@link JoinStats} are being reported.
     * @param ruleState
     *            Contains details about evaluation order for the
     *            {@link IPredicate}s in the tail of the <i>rule</i>, the access
     *            paths that were used, etc.
     * @param a
     *            The {@link JoinStats}.
     * 
     * @return The table view.
     */
    public static StringBuilder toString(final IRule rule,
            final IRuleState ruleState, final JoinStats[] a) {

        /*
         * Note: This is the same format that is used for the performance
         * counters. This makes it easier to correlate what is going on in the
         * query execution log with the performance counter data.
         */
        final DateFormat dateFormat = DateFormat.getDateTimeInstance(
                DateFormat.MEDIUM/* date */, DateFormat.MEDIUM/* time */);

        final int[] order = ruleState.getPlan().getOrder();
        
        final StringBuilder sb = new StringBuilder();
        
        // Note: orderIndex is also known as the evalOrder.
        sb.append("startTime, rule, orderIndex, keyOrder, nvars, rangeCount, fanIn, fanOut, partitionCount, bindingSetChunksIn, bindingSetsIn, accessPathCount, accessPathDups, chunkCount, elementCount, bindingSetsOut, bindingSetChunksOut, mutationCount, tailIndex, tailPredicate");
        
        sb.append("\n");
        
        int i = 0;
        for(JoinStats s : a) {

            final int tailIndex = order[i++];

            final String ruleNameStr = "\"" + rule.getName().replace(',', ' ')
                    + "\"";

            sb.append(dateFormat.format(s.startTime).replace(sep, " ")+sep);
            sb.append(ruleNameStr + sep);
            sb.append(Integer.toString(s.orderIndex)+sep);
//            sb.append(Integer.toString(s.partitionId)+sep); // always -1 when aggregated.
            sb.append(ruleState.getKeyOrder()[tailIndex].toString().replace(sep, " ")+sep);
            sb.append(ruleState.getNVars()[tailIndex]+sep);
            sb.append(ruleState.getPlan().rangeCount(tailIndex)+sep);
            sb.append(Integer.toString(s.fanIn)+sep);
            sb.append(Integer.toString(s.fanOut)+sep);
            sb.append(Integer.toString(s.partitionCount)+sep);
            sb.append(Long.toString(s.bindingSetChunksIn)+sep);
            sb.append(Long.toString(s.bindingSetsIn)+sep);
            sb.append(Long.toString(s.accessPathCount)+sep);
            sb.append(Long.toString(s.accessPathDups)+sep);
            sb.append(Long.toString(s.chunkCount)+sep);
            sb.append(Long.toString(s.elementCount)+sep);
            sb.append(Long.toString(s.bindingSetsOut)+sep);
            sb.append(Long.toString(s.bindingSetChunksOut)+sep);
            sb.append(Long.toString(s.mutationCount.get())+sep);
            sb.append(Integer.toString(tailIndex)+sep);
            sb.append(rule.getTail(tailIndex).toString().replace(sep, " ")+"\n");
            
        }
        
        return sb;
        
    }
    
}
