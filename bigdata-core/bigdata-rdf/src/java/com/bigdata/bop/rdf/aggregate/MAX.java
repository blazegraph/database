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
package com.bigdata.bop.rdf.aggregate;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.solutions.IVComparator;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;

/**
 * Operator reports the minimum observed value over the presented binding sets
 * for the given variable using SPARQL ORDER_BY semantics. Missing values are
 * ignored.
 * <p>
 * Note: MIN (and MAX) are defined in terms of the ORDER_BY semantics for
 * SPARQL. Therefore, this must handle comparisons when the value is not an IV,
 * e.g., using {@link IVComparator}.
 *
 * @author thompsonbry
 *
 *         TODO What is reported if there are no non-null observations?
 */
public class MAX extends AggregateBase<IV> implements INeedsMaterialization{

//    private static final transient Logger log = Logger.getLogger(MAX.class);

    /**
	 *
	 */
    private static final long serialVersionUID = 1L;

    /**
     * Provides SPARQL <em>ORDER BY</em> semantics.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/736">
     *      MIN() malfunction </a>
     */
    private static final transient IVComparator comparator = new IVComparator();

    public MAX(final MAX op) {
        super(op);
    }

    public MAX(final BOp[] args, final Map<String, Object> annotations) {
        super(args, annotations);
    }

    public MAX(final boolean distinct, final IValueExpression...expr) {
        super(distinct, expr);
    }

    /**
     * The maximum observed value and initially <code>null</code>.
     * <p>
     * Note: This field is guarded by the monitor on the {@link MAX} instance.
     */
    private transient IV max = null;

    /**
     * The first error encountered since the last {@link #reset()}.
     */
    private transient Throwable firstCause = null;

    synchronized public IV get(final IBindingSet bindingSet) {

        try {

            return doGet(bindingSet);

        } catch (Throwable t) {

            if (firstCause == null) {

                firstCause = t;

            }

            throw new RuntimeException(t);

        }

    }

    private IV doGet(final IBindingSet bindingSet) {
        
        for (int i = 0; i < arity(); i++) {

            final IValueExpression<IV<?, ?>> expr = (IValueExpression<IV<?, ?>>) get(i);

            final IV iv = expr.get(bindingSet);

            if (iv != null) {

                /*
                 * Aggregate non-null values.
                 */

                if (max == null) {

                    max = iv;

                } else {

                    // Note: This is SPARQL GT semantics, not ORDER BY.
//                   if (CompareBOp.compare(iv, max, CompareOp.GT)) {

                    // SPARQL ORDER_BY semantics
                    if (comparator.compare(iv, max) > 0) {

                        max = iv;

                    }

                }

            }
        }
        return max;

    }

    @Override
    synchronized public void reset() {

        max = null;

        firstCause = null;

    }

    synchronized public IV done() {

        if (firstCause != null) {

            throw new RuntimeException(firstCause);

        }

        return max;

    }

    /**
     * Note: {@link MAX} only works on pretty much anything and uses the same
     * semantics as {@link CompareBOp} (it is essentially the transitive closure
     * of LT over the column projection of the inner expression). This probably
     * means that we always need to materialize something unless it is an inline
     * numeric IV.
     *
     * FIXME MikeP: What is the right return value here?
     */
    @Override
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.ALWAYS;

    }

}
