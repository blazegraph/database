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
package com.bigdata.bop.rdf.aggregate;

import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.evaluation.util.ValueComparator;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;

/**
 * Operator reports the minimum observed value over the presented binding sets
 * for the given variable using SPARQL ORDER_BY semantics. Missing values are
 * ignored.
 * <p>
 * Note: MIN (and MAX) are defined in terms of the ORDER_BY semantics for
 * SPARQL. Therefore, this must handle comparisons when the value is not an IV,
 * e.g., using {@link ValueComparator}.
 *
 * @author thompsonbry
 *
 * TODO What is reported if there are no non-null observations?
 */
public class MIN extends AggregateBase<IV> implements IAggregate<IV> {

//    private static final transient Logger log = Logger.getLogger(MIN.class);

    /**
	 *
	 */
    private static final long serialVersionUID = 1L;

    public MIN(MIN op) {
        super(op);
    }

    public MIN(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    public MIN(boolean distinct, IValueExpression<IV> expr) {
        super(/*FunctionCode.MIN,*/ distinct, expr);
    }

    /**
     * The minimum observed value and initially <code>null</code>.
     * <p>
     * Note: This field is guarded by the monitor on the {@link MIN} instance.
     */
    private transient IV min = null;

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

        final IValueExpression<IV> expr = (IValueExpression<IV>) get(0);

        final IV iv = expr.get(bindingSet);

        if (iv != null) {

            /*
             * Aggregate non-null values.
             */

            if (min == null) {

                min = iv;

            } else {

                /**
                 * FIXME This needs to use the ordering define by ORDER_BY. The
                 * CompareBOp imposes the ordering defined for the "<" operator
                 * which is less robust and will throw a type exception if you
                 * attempt to compare unlike Values.
                 *
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/300#comment:5
                 */
                if (CompareBOp.compare(iv, min, CompareOp.LT)) {

                    min = iv;

                }

            }

        }

        return min;

    }

    synchronized public void reset() {

        min = null;

        firstCause = null;

    }

    synchronized public IV done() {

        if (firstCause != null) {

            throw new RuntimeException(firstCause);

        }

        return min;

    }

    /**
     * Note: {@link MIN} only works on pretty much anything and uses the same
     * semantics as {@link CompareBOp} (it is essentially the transitive closure
     * of LT over the column projection of the inner expression). This probably
     * means that we always need to materialize something unless it is an inline
     * numeric IV.
     *
     * FIXME MikeP: What is the right return value here?
     */
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.ALWAYS;

    }

}
