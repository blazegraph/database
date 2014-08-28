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
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.solutions.SliceOp.Annotations;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.relation.accesspath.IBlockingBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * An in-memory merge sort for binding sets. The operator is pipelined. Each
 * time it runs, it evaluates the value expressions on which the ordering will
 * be imposed, binding the results on the incoming solutions and buffers the
 * as-bound solution for eventual sorting. The sort is applied only once the
 * last chunk of source solutions has been observed.
 * <p>
 * Computing the value expressions first is not only an efficiency, but is also
 * required in order to detect type errors. When a type error is detected for a
 * value expression the corresponding input solution is kept but with no new 
 * bindings, see trac-765. Since the
 * computed value expressions must become bound on the solutions to be sorted,
 * the caller is responsible for wrapping any value expression more complex than
 * a variable or a constant with an {@link IBind} onto an anonymous variable.
 * All such variables will be dropped when the solutions are written out. Since
 * this operator must be able to compare all {@link IV}s in all
 * {@link IBindingSet}s, it depends on the materialization of non-inline
 * {@link IV}s and the ability of the value comparator to handle comparisons
 * between materialized non-inline {@link IV}s and inline {@link IV}s.
 * 
 * TODO External memory ORDER BY operator.
 * <p>
 * SPARQL ORDER BY semantics are complex and evaluating a SPARQL ORDER BY is
 * further complicated by the schema flexibility of the value to be sorted. The
 * simplest path to a true external memory sort operator would be to buffer and
 * manage paging for blocks of inline IVs without materialized RDF values s and
 * non-inline IVs with materialized RDF values.
 * <p>
 * An operator could also be written which buffers the solutions on the native
 * heap but the as-bound values which will be used to order those solutions are
 * either buffered on the JVM heap or materialized onto the JVM heap when the
 * sort is executed. This could scale better than a pure JVM heap version, but
 * only to the extent that the keys are smaller than the total solutions. The
 * solutions would probably be written as serialized binding sets on the memory
 * manager such that each solution has its own int32 address. That address can
 * then be paired with the as-bound key to be sorted on the JVM heap.
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
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
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

        if (!isLastPassRequested()) {
            throw new UnsupportedOperationException(Annotations.LAST_PASS
                    + "=" + isLastPassRequested());
        }
        
        // ORDER_BY must preserve order.
        if (isReorderSolutions())
            throw new UnsupportedOperationException(
                    Annotations.REORDER_SOLUTIONS + "=" + isReorderSolutions());

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
    
    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new SortTask(this, context));
        
    }

    /**
     * Task executing on the node.
     */
    static private class SortTask implements Callable<Void> {

        private final MemorySortOp op;
        
        private final BOpContext<IBindingSet> context;

        private final BOpStats stats;

        private final ISortOrder<?>[] sortOrder;

        /**
         * The {@link IQueryAttributes} for the {@link IRunningQuery} off which
         * we will hang the named solution set.
         */
        private final IQueryAttributes attrs;
        
        /**
         * The solutions. A reference to this object is stored on the
         * {@link IQueryAttributes}.
         */
        private transient LinkedList<IBindingSet> solutions;
        
        /**
         * The name of the key under which the {@link #solutions} are stored in
         * the {@link IQueryAttributes}.
         */
        private final String key;
        
        @SuppressWarnings("unchecked")
        SortTask(final MemorySortOp op,
                final BOpContext<IBindingSet> context) {
        	
            this.op = op;
            
            this.context = context;
            
            this.stats = context.getStats();

            this.sortOrder = op.getSortOrder();
        
            this.attrs = context.getQueryAttributes();
            
            this.key = Integer.toString(op.getId());
                    
            solutions = (LinkedList<IBindingSet>) attrs.get(key);

            if(solutions == null) {

                solutions = new LinkedList<IBindingSet>();
                
                if (attrs.putIfAbsent(key, solutions) != null)
                    throw new AssertionError();
                
            }
            
        }

        void release() {

            if (log.isInfoEnabled())
                log.info("Releasing state");

            attrs.remove(key);
            
            solutions = null;
            
        }

        @Override
        public Void call() throws Exception {

            final ICloseableIterator<IBindingSet[]> itr = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            final boolean lastInvocation = context.isLastInvocation();
            
            try {

                acceptSolutions(itr);

                if (lastInvocation) {

                    doOrderBy(sink);
                    
                }
                
            } catch(Throwable t) {
                
                log.error(t,t);
                
                throw new RuntimeException(t);

            } finally {

                if (lastInvocation) {

                    // Discard the operator's internal state.
                    release();

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
                final ICloseableIterator<IBindingSet[]> itr) {

            try {

                while (itr.hasNext()) {

                    final IBindingSet[] a = itr.next();

                    stats.chunksIn.increment();
                    stats.unitsIn.add(a.length);

                    for (IBindingSet bset : a) {

                        // Note: Necessary scope for type error reporting.
                        IValueExpression<?> expr = null;
                        
                        try {

                            for (ISortOrder<?> s : sortOrder) {

                                /*
                                 * Evaluate. A BIND() will have side-effect on
                                 * [bset].
                                 */
                                (expr = s.getExpr()).get(bset);

                            }

                        } catch (SparqlTypeErrorException ex) {

                            // log type error, do not drop solution (see trac 765).
                            TypeErrorLog.handleTypeError(ex, expr, stats);
                            
                        }

                        // add to the set of solutions to be sorted.
                        solutions.add(bset);

                    } // next source solution

//                    /*
//                     * Note: By synchronizing on [stats] here we are able to run
//                     * concurrent evaluation tasks for this operator which
//                     * compute the as-bound values.
//                     */
//                    synchronized (stats) {
//                        for (IBindingSet bset : a) {
//                        }
//                    }
                        
                }

                if (log.isInfoEnabled())
                    log.info("Buffered " + solutions.size()
                            + " solutions so far");

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

            if (log.isInfoEnabled())
                log.info("Sorting.");

            final IBindingSet[] all = solutions.toArray(new IBindingSet[0]);

            @SuppressWarnings({ "rawtypes", "unchecked" })
            final Comparator<IBindingSet> c = new BindingSetComparator(
                    sortOrder, op.getValueComparator());

            // sort.
            {
                
                final long begin = System.currentTimeMillis();

                Arrays.sort(all, c);

                final long elapsed = System.currentTimeMillis() - begin;

                if (log.isInfoEnabled())
                    log.info("Sorted " + all.length + " solutions in "
                            + elapsed + "ms.");
                
            }

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
