package com.bigdata.relation.rule.eval.pipeline;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.RuleStats;
import com.bigdata.relation.rule.eval.pipeline.JoinTask.AccessPathTask;

/**
 * Statistics about processing for a single join dimension as reported by a
 * single {@link JoinTask}. Each {@link JoinTask} handles a single index
 * partition, so the {@link JoinStats} for those index partitions need to be
 * aggregated by the {@link JoinMasterTask}.
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
     * The index partition for which these statistics were collected or -1
     * if the statistics are aggregated across index partitions.
     */
    public final int partitionId;

    /**
     * The index in the evaluation order whose statistics are reported here.
     */
    public final int orderIndex;

    /** #of join tasks writing on this join task. */
    public int fanIn;

    /**
     * #of join tasks written on by this join task (zero if last in eval
     * order).
     */
    public int fanOut;

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
     *            The index partition.
     * @param orderIndex
     *            The index in the evaluation order.
     */
    public JoinStats(final int partitionId, final int orderIndex) {

        this.partitionId = partitionId;

        this.orderIndex = orderIndex;

        fanIn = fanOut = 0;

        bindingSetChunksIn = bindingSetsIn = 0L;
        
        accessPathCount = accessPathDups = 0L;

        chunkCount = elementCount = bindingSetsOut = 0L;

        bindingSetChunksOut = 0L;

    }

    synchronized void add(final JoinStats o) {

        if (this.orderIndex != o.orderIndex)
            throw new IllegalArgumentException();

        this.fanIn += o.fanIn;
        this.fanOut += o.fanOut;
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

    /**
     * Formats the array of {@link JoinStats} into a CSV table view.
     * 
     * @param rule
     *            The {@link IRule} whose {@link JoinStats} are being
     *            reported.
     * @param order
     *            The execution order for the {@link IPredicate}s in the
     *            tail of the <i>rule</i>.
     * @param a
     *            The {@link JoinStats}.
     * 
     * @return The table view.
     */
    public static StringBuilder toString(final IRule rule,
            final int[] order, final JoinStats[] a) {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append("rule, orderIndex, partitionId, fanIn, fanOut, bindingSetChunksIn, bindingSetsIn, accessPathCount, accessPathDups, chunkCount, elementCount, bindingSetsOut, bindingSetChunksOut, mutationCount, tailIndex, tailPredicate");
        
        sb.append("\n");
        
        int i = 0;
        for(JoinStats s : a) {

            final int tailIndex = order[i++];

            sb.append(rule.getName().replace(',', ' ')+", ");
            sb.append(Integer.toString(s.orderIndex)+", ");
            sb.append(Integer.toString(s.partitionId)+", ");
            sb.append(Integer.toString(s.fanIn)+", ");
            sb.append(Integer.toString(s.fanOut)+", ");
            sb.append(Long.toString(s.bindingSetChunksIn)+", ");
            sb.append(Long.toString(s.bindingSetsIn)+", ");
            sb.append(Long.toString(s.accessPathCount)+", ");
            sb.append(Long.toString(s.accessPathDups)+", ");
            sb.append(Long.toString(s.chunkCount)+", ");
            sb.append(Long.toString(s.elementCount)+", ");
            sb.append(Long.toString(s.bindingSetsOut)+", ");
            sb.append(Long.toString(s.bindingSetChunksOut)+", ");
            sb.append(Long.toString(s.mutationCount.get())+", ");
            sb.append(Integer.toString(tailIndex)+", ");
            sb.append(rule.getTail(tailIndex).toString().replace(",", "")+"\n");
            
        }
        
        return sb;
        
    }
    
}
