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

import org.openrdf.model.Literal;
import org.openrdf.model.impl.LiteralImpl;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;

/**
 * Operator combines the string values over the presented binding sets for the
 * given variable. Missing values are ignored. The initial value is an empty
 * plain literal.
 * 
 * @author thompsonbry
 */
public class GROUP_CONCAT extends AggregateBase<Literal> implements
        IAggregate<Literal> {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends AggregateBase.Annotations {

        /**
         * Required string property provides the separator used when combining
         * the {@link IValueExpression} computed for each solution within the
         * group.
         */
        String SEPARATOR = GROUP_CONCAT.class.getName() + ".separator";

        /**
         * The maximum #of values to concatenate.
         */
        String LIMIT = GROUP_CONCAT.class.getName() + ".limit";

    }

    public GROUP_CONCAT(BOpBase op) {
        super(op);
    }

    public GROUP_CONCAT(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * 
     * @param var
     *            The variable whose values will be combined.
     * @param sep
     *            The separator string.
     */
    public GROUP_CONCAT(final boolean distinct,
            final IValueExpression<Literal> expr, final IConstant<String> sep) {
        this(new BOp[] { expr }, NV.asMap(//
                new NV(Annotations.FUNCTION_CODE, FunctionCode.GROUP_CONCAT),//
                new NV(Annotations.DISTINCT, distinct),//
                new NV(Annotations.SEPARATOR, sep)//
                ));
    }

    private String sep() {
        if (sep == null) {
            sep = (String) getRequiredProperty(Annotations.SEPARATOR);
        }
        return sep;
    }

    private transient String sep;

    private long limit() {
        if (limit == 0) {
            limit = getProperty(Annotations.LIMIT, Long.MAX_VALUE);
        }
        return limit;
    }

    private transient long limit;

    /**
     * The running concatenation of observed bound values.
     * <p>
     * Note: This field is guarded by the monitor on the {@link GROUP_CONCAT}
     * instance.
     */
    private transient StringBuilder aggregated = null;

    /**
     * The #of values in {@link #aggregated}.
     * <p>
     * Note: This field is guarded by the monitor on the {@link GROUP_CONCAT}
     * instance.
     */
    private transient long nvalues = 0;

    synchronized public void reset() {

        aggregated = null;

        nvalues = 0;

    }

    synchronized public Literal done() {

        if (aggregated == null)
            return EMPTY_LITERAL;

        return new LiteralImpl(aggregated.toString());

    }

    synchronized public Literal get(final IBindingSet bindingSet) {

        final IVariable<Literal> var = (IVariable<Literal>) get(0);

        final Literal val = (Literal) bindingSet.get(var);

        if (val != null && nvalues < limit()) {

            if (aggregated == null)
                aggregated = new StringBuilder(val.stringValue());
            else {
                aggregated.append(sep());
                aggregated.append(val.stringValue());
            }

            nvalues++;

        }

        // Note: Nothing returned until done().
        return null;

    }

    /**
     * We always need to have the materialized values.
     */
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.ALWAYS;

    }

}
