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
package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;

/**
 * A math expression involving a left and right IValueExpression operand. The operation to be applied to the operands is
 * specified by the {@link Annotations#OP} annotation.
 */
public class NumericBOp extends IVValueExpression<IV>  {

    private static final long serialVersionUID = 9136864442064392445L;

    public interface Annotations extends ImmutableBOp.Annotations {
        String OP = NumericBOp.class.getName() + ".op";
    }

    public enum NumericOp {
        ABS, ROUND, CEIL, FLOOR;
    }

    /**
     *
     * @param left
     *            The left operand.
     * @param right
     *            The right operand.
     * @param op
     *            The annotation specifying the operation to be performed on those operands.
     */
    @SuppressWarnings("rawtypes")
    public NumericBOp(final IValueExpression<? extends IV> left, final NumericOp op) {

        this(new BOp[] { left }, NV.asMap(new NV(Annotations.OP, op)));

    }

    /**
     * Required shallow copy constructor.
     *
     * @param args
     *            The operands.
     * @param op
     *            The operation.
     */
    public NumericBOp(final BOp[] args, Map<String, Object> anns) {

        super(args, anns);

        if (args.length != 1 || args[0] == null || getProperty(Annotations.OP) == null) {

            throw new IllegalArgumentException();

        }

    }

    /**
     * Required deep copy constructor.
     *
     * @param op
     */
    public NumericBOp(final NumericBOp op) {

        super(op);

    }

    @SuppressWarnings("rawtypes")
    public IValueExpression<? extends IV> left() {
        return get(0);
    }

    public NumericOp op() {
        return (NumericOp) getRequiredProperty(Annotations.OP);
    }

    @SuppressWarnings("rawtypes")
    public IV get(final IBindingSet bindingSet) {
        
        final IV iv = get(0).get(bindingSet);

        if (iv == null)
            throw new SparqlTypeErrorException();

        if (!iv.isLiteral())
            throw new SparqlTypeErrorException();

        if (!iv.isInline() )
            throw new SparqlTypeErrorException();

        return IVUtility.numericalFunc(iv, op());
    }


    public String toString() {

        final StringBuilder sb = new StringBuilder();
        sb.append(op());
        sb.append("(").append(left()).append(")");
        return sb.toString();

    }

}
