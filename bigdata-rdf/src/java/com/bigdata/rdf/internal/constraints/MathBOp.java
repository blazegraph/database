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

import org.openrdf.query.algebra.MathExpr.MathOp;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.bop.NV;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;

/**
 * A math expression involving a left and right IValueExpression operand.
 */
final public class MathBOp extends ImmutableBOp 
		implements IValueExpression<IV> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 9136864442064392445L;
    
    public interface Annotations extends ImmutableBOp.Annotations {

        /**
         * The {@link IVariable} which is bound to that constant value
         * (optional).
         */
        String OP = MathBOp.class.getName() + ".op";

    }
    
    /**
     * Required deep copy constructor.
     * 
     * @param op
     */
    public MathBOp(final MathBOp op) {

        super(op);
        
    }

    public MathBOp(final IValueExpression<IV> left, 
    		final IValueExpression<IV> right, final MathOp op) {

        super(new BOp[] { left, right }, NV.asMap(new NV(Annotations.OP, op)));

        if (left == null || right == null || op == null)
            throw new IllegalArgumentException();

    }

//    /**
//     * Clone is overridden to reduce heap churn.
//     */
//    final public Math clone() {
//
//        return this;
//        
//    }

    public IValueExpression<IV> left() {
    	return (IValueExpression<IV>) get(0);
    }
    
    public IValueExpression<IV> right() {
    	return (IValueExpression<IV>) get(1);
    }
    
    public MathOp op() {
    	return (MathOp) getRequiredProperty(Annotations.OP);
    }
    
    public String toString() {

    	final StringBuilder sb = new StringBuilder();
    	sb.append(op());
    	sb.append("(").append(left()).append(", ").append(right()).append(")");
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
    
    final public boolean equals(final IValueExpression<IV> o) {

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

    final public IV get(final IBindingSet bindingSet) {
        
        final IV left = left().get(bindingSet);
        final IV right = right().get(bindingSet);
        
        if (left == null || right == null)
        	return null;
        
        return IVUtility.numericalMath(left, right, op());

    }

}
