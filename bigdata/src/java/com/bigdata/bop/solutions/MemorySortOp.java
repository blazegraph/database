package com.bigdata.bop.solutions;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryContext;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * An in-memory merge sort for binding sets. The operator is pipelined. Each
 * time it runs, it evaluates the value expressions on which the ordering will
 * be imposed, binding the results on the incoming solution and buffers the
 * as-bound solution for eventual sorting. The sort is applied only once the
 * last chunk of source solutions has been observed. Computing the value
 * expressions first is not only an efficiency, but is also required in order to
 * detect type errors. When a type error is detected for a value expression the
 * corresponding input solution is dropped. Since the computed value expressions
 * must become bound on the solutions to be sorted, the caller is responsible
 * for wrapping any value expression more complex than a variable or a constant
 * with an {@link IBind} onto an anonymous variable. All such variables will be
 * dropped when the solutions are written out. Since this operator must be able
 * to compare all {@link IV}s in all {@link IBindingSet}s, it depends on the
 * materialization of non-inline {@link IV}s and the ability of the value
 * comparator to handle comparisons between materialized non-inline {@link IV}s
 * and inline {@link IV}s.
 * 
 * TODO External memory ORDER BY operator.
 * <p>
 * SPARQL ORDER BY semantics are complex and evaluating a SPARQL ORDER BY is
 * further complicated by the schema flexibility of the value to be sorted. The
 * simplest path to a true external memory sort operator would be to buffer
 * manage paging for blocks of inline IVs without materialized RDF values s and
 * non-inline IVs with materialized RDF values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
public class MemorySortOp extends SortOp {

    private static final transient Logger log = Logger.getLogger(MemorySortOp.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Required deep copy constructor.
     */
    public MemorySortOp(final MemorySortOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public MemorySortOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        switch (getEvaluationContext()) {
		case CONTROLLER:
			break;
		default:
			throw new UnsupportedOperationException(
					Annotations.EVALUATION_CONTEXT + "="
							+ getEvaluationContext());
		}

        if (getMaxParallel() != 1)
            throw new UnsupportedOperationException(Annotations.MAX_PARALLEL
                    + "=" + getMaxParallel());

        // shared state is used to share the hash table.
        if (!isSharedState()) {
            throw new UnsupportedOperationException(Annotations.SHARED_STATE
                    + "=" + isSharedState());
        }
        
        // required parameter.
        getValueComparator();
        
        // validate required parameter.
        for (ISortOrder<?> s : getSortOrder()) {

            final IValueExpression<?> expr = s.getExpr();
            
            if(expr instanceof IVariableOrConstant<?>)
                continue;

            if(expr instanceof IBind<?>)
                continue;

            throw new IllegalArgumentException(
                    "Value expression not wrapped by bind: " + expr);

        }
        
	}

    public BOpStats newStats(final IQueryContext queryContext) {

        return new MyStats();

    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new SortTask(this, context));
        
    }

    private static class MyStats extends BOpStats {

        private static final long serialVersionUID = 1L;
        
        private volatile transient LinkedList<IBindingSet> solutions;
        
        MyStats() {

            this.solutions = new LinkedList<IBindingSet>();
            
        }
        
        void release() {

            log.error("Releasing state");
            
            solutions = null;
            
        }

    }

    /**
     * Task executing on the node.
     */
    static private class SortTask implements Callable<Void> {

        private final MemorySortOp op;
        
        private final BOpContext<IBindingSet> context;

        private final MyStats stats;

        private final ISortOrder<?>[] sortOrder;

        SortTask(final MemorySortOp op,
                final BOpContext<IBindingSet> context) {
        	
            this.op = op;
            
            this.context = context;
            
            this.stats = (MyStats) context.getStats();

            this.sortOrder = op.getSortOrder();
            
        }

        public Void call() throws Exception {

            final IAsynchronousIterator<IBindingSet[]> itr = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                acceptSolutions(itr);

                if (context.isLastInvocation()) {

                    doOrderBy(sink);

                    sink.flush();
                    
                }

            } finally {

                if (context.isLastInvocation()) {

                    // Discard the operator's internal state.
                    stats.release();

                }

                sink.close();

            }

            // Done.
            return null;

        }

        /**
         * Evaluate the value expressions for each input solution, drop any
         * solution for which there is a type error, and buffer the as-bound
         * solutions.
         * 
         * @param itr
         *            The source solutions.
         */
        private void acceptSolutions(
                final IAsynchronousIterator<IBindingSet[]> itr) {

            try {

                while (itr.hasNext()) {

                    final IBindingSet[] a = itr.next();

                    stats.chunksIn.increment();
                    stats.unitsIn.add(a.length);

                    for (IBindingSet bset : a) {

                        try {

                            for (ISortOrder<?> s : sortOrder) {

                                /*
                                 * Evaluate. A BIND() will have side-effect on
                                 * [bset].
                                 */
                                s.getExpr().get(bset);

                            }

                            // add to the set of solutions to be sorted.
                            stats.solutions.add(bset);
                            
                        } catch (SparqlTypeErrorException ex) {

                            if (log.isInfoEnabled())
                                log.info("Dropping solution due to type error: "
                                        + bset);
                            
                            continue;
                            
                        }

                    }
                    
                }

            } finally {
        
                itr.close();
                
            }
        
        } // acceptSolutions

        /**
         * Sort the solutions based on the as-bound value expressions.
         * 
         * @param sink
         *            Where to write the results.
         */
        private void doOrderBy(final IBlockingBuffer<IBindingSet[]> sink) {

            final IBindingSet[] all = stats.solutions
                    .toArray(new IBindingSet[0]);

            @SuppressWarnings({ "rawtypes", "unchecked" })
            final Comparator<IBindingSet> c = new BindingSetComparator(
                    sortOrder, op.getValueComparator());
            
            // sort.
            Arrays.sort(all, c);
            
            // Drop variables for computed value expressions.
            for(IBindingSet bset : all) {
                for(ISortOrder<?> s : sortOrder) {
                    final IValueExpression<?> expr = s.getExpr();
                    if(expr instanceof IBind) {
                        bset.clear(((IBind<?>) expr).getVar());
                    }
                }
            }

            // write output and flush.
            sink.add(all);
            sink.flush();

        }

    } // ChunkTask
    
} // MemorySortOp
