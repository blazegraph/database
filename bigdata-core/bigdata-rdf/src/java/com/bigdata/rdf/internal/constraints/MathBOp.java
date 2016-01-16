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

/**
Note: Portions of this file are copyright by Aduna.

Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.

Licensed under the Aduna BSD-style license.
*/

package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * A math expression involving a left and right IValueExpression operand. The
 * operation to be applied to the operands is specified by the
 * {@link Annotations#OP} annotation.
 */
final public class MathBOp extends IVValueExpression
		implements INeedsMaterialization {

    /**
	 *
	 */
	private static final long serialVersionUID = 9136864442064392445L;

	private static final transient Logger log = Logger.getLogger(MathBOp.class);

    public interface Annotations extends IVValueExpression.Annotations {

        /**
         * The operation to be applied to the left and right operands
         * (required). The value of this annotation is a {@link MathOp}, such as
         * {@link MathOp#PLUS}.
         *
         * @see MathOp
         */
        String OP = (MathBOp.class.getName() + ".op").intern();

    }

	public enum MathOp {
		PLUS,
		MINUS,
		MULTIPLY,
		DIVIDE,
		MIN,
		MAX,
		ROUND,
		CEIL,
		FLOOR;

		public static MathOp valueOf(org.openrdf.query.algebra.MathExpr.MathOp op) {
			switch(op) {
			case PLUS: return MathOp.PLUS;
			case MINUS: return MathOp.MINUS;
			case MULTIPLY: return MathOp.MULTIPLY;
			case DIVIDE: return MathOp.DIVIDE;
			}
			throw new IllegalArgumentException();
		}
	}

    /**
     *
     * @param left
     *            The left operand.
     * @param right
     *            The right operand.
     * @param op
     *            The annotation specifying the operation to be performed on
     *            those operands.
     */
    public MathBOp(final IValueExpression<? extends IV> left,
    		final IValueExpression<? extends IV> right, final MathOp op,
    		final GlobalAnnotations globals) {

        this(new BOp[] { left, right }, anns(globals, new NV(Annotations.OP, op)));

    }

	/**
	 * Required shallow copy constructor.
	 *
	 * @param args
	 *            The operands.
	 * @param op
	 *            The operation.
	 */
    public MathBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

		if (args.length != 2 || args[0] == null || args[1] == null
				|| getProperty(Annotations.OP) == null
				|| getProperty(Annotations.NAMESPACE)==null) {

			throw new IllegalArgumentException();

		}

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     *
     * @param op
     */
    public MathBOp(final MathBOp op) {

        super(op);

    }

    final public IV get(final IBindingSet bs) {

        final IV iv1 = getAndCheckLiteral(0, bs);
        final IV iv2 = getAndCheckLiteral(1, bs);

    	if (log.isDebugEnabled()) {
    		log.debug(toString(iv1.toString(), iv2.toString()));
    	}

		final Literal lit1 = asLiteral(iv1);
		final Literal lit2 = asLiteral(iv2);

		ILexiconConfiguration<?> lexicon = getLexiconConfiguration(bs);

		for (IMathOpHandler handler: lexicon.getTypeHandlers()) {
		    if (handler.canInvokeMathOp(lit1, lit2)) {
		        final IV iv =
		                handler.doMathOp(lit1, iv1, lit2, iv2, op(), vf());

		        // try to create a real IV if possible
		        if (iv.isNullIV()) {
		            final BigdataValue val = iv.getValue();
		            return asIV(val, bs);
		        } else {
		            return iv;
		        }
		    }
		}

    	if (log.isDebugEnabled()) {
    		log.debug("illegal argument(s), filtering solution: " + iv1 + ", " + iv2);
    	}

    	throw new SparqlTypeErrorException();

    }

    public IValueExpression<? extends IV> left() {
    	return get(0);
    }

    public IValueExpression<? extends IV> right() {
    	return get(1);
    }

    public MathOp op() {
    	return (MathOp) getRequiredProperty(Annotations.OP);
    }

    public BigdataValueFactory vf(){
        return super.getValueFactory();
    }

    public String toString() {

    	final StringBuilder sb = new StringBuilder();
    	sb.append(op());
    	sb.append("(").append(left()).append(", ").append(right()).append(")");
    	return sb.toString();

    }

    private String toString(final String left, final String right) {

    	final StringBuilder sb = new StringBuilder();
    	sb.append(op());
    	sb.append("(").append(left).append(", ").append(right).append(")");
    	return sb.toString();

    }

    final public boolean equals(final MathBOp m) {

    	if (m == null)
    		return false;

    	if (this == m)
    		return true;

    	return op().equals(m.op()) &&
    		left().equals(m.left()) &&
    		right().equals(m.right());

    }

    final public boolean equals(final IVValueExpression o) {

    	if(!(o instanceof MathBOp)) {
            // incomparable types.
            return false;
        }
        return equals((MathBOp) o);

    }


	/**
	 * Caches the hash code.
	 */
	private int hash = 0;

	public int hashCode() {

		int h = hash;
		if (h == 0) {
			final int n = arity();
			for (int i = 0; i < n; i++) {
				h = 31 * h + get(i).hashCode();
			}
			h = 31 * h + op().hashCode();
			hash = h;
		}
		return h;

	}

    /**
     * The MathBOp can work on inline numerics. It is only when the operands
     * evaluate to non-inline numerics that this bop needs materialization.
     */
    public Requirement getRequirement() {
    	return INeedsMaterialization.Requirement.SOMETIMES;
    }

}
