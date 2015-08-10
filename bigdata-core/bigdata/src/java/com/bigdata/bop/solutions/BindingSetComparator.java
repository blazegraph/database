package com.bigdata.bop.solutions;

import java.util.Comparator;

import com.bigdata.bop.IBind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;

/**
 * A comparator for binding sets.
 */
public class BindingSetComparator<E> implements Comparator<IBindingSet> {

    /**
     * The sort order to be imposed.
     */
    private final ISortOrder<E>[] sortOrder;

    /**
     * The comparison to use on the values in the binding sets.
     */
    private final Comparator<E> valueComparator;

    /** The #of solutions compared. */
    private long n = 0;
    
    /**
     * 
     * @param sortOrder
     *            The sort order to be imposed. While the {@link ISortOrder} is
     *            defined in terms of {@link IValueExpression}s, any
     *            {@link IValueExpression}s MUST be either bare variables or
     *            constants or be {@link IBind}s where the nested
     *            {@link IValueExpression} has been computed and bound on the
     *            solutions to be compared by the caller.
     * @param valueComparator
     *            The comparator used on the values extracted from the
     *            {@link IBindingSet}s.
     */
    public BindingSetComparator(final ISortOrder<E>[] sortOrder,
            final Comparator<E> valueComparator) {

        if (sortOrder == null)
            throw new IllegalArgumentException();

        if (sortOrder.length == 0)
            throw new IllegalArgumentException();

        if(valueComparator == null)
            throw new IllegalArgumentException();
        
        this.sortOrder = sortOrder;
        
        this.valueComparator = valueComparator;
        
    }
    
    @Override
    public int compare(final IBindingSet bs1, final IBindingSet bs2) {

        if ((n++ % 1000) == 1) {
            /*
             * Check for interrupts, but not too often.
             */
            if (Thread.interrupted())
                throw new RuntimeException(new InterruptedException());
        }
        
        for (int i = 0; i < sortOrder.length; i++) {

            final ISortOrder<E> o = sortOrder[i];

            IValueExpression<E> v = o.getExpr();

            if (v instanceof IBind<?>) {

                /*
                 * Use the variable on which the computed expression was bound.
                 * 
                 * Note: This assumes that the value expression for the bind has
                 * already been evaluated such that we may simply resolve the
                 * bound value for the variable against the binding set. This is
                 * necessary to avoid recomputing the value expression each time
                 * we encounter a given binding set. Since we are sorting, we
                 * could see the same binding set many, many times while the
                 * sort figures out its position in the total ordering. The
                 * top-level ORDER BY operator is responsible for evaluating
                 * those value expressions when it buffers its solution set for
                 * the sort.
                 */

                v = ((IBind) v).getVar();

            }

            /*
             * Note: This wind up invoking the comparison on IVs. When comparing
             * IVs which are not inline values, the IVs MUST have been
             * materialized before the comparator may be applied.
             */
            int ret = valueComparator.compare(v.get(bs1), v.get(bs2));

            if (!o.isAscending())
                ret = -ret;

            if (ret != 0) {

                // Not equal for this variable.
                return ret;
                
            }

        }

        // equal for all variables.
        return 0;

    }

}
