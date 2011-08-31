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

/**
Note: Portions of this file are copyright by Aduna.

Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.

Licensed under the Aduna BSD-style license.
*/

package com.bigdata.rdf.internal.constraints;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.constraints.AbstractLiteralBOp.Annotations;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sparql.ast.ComputedMaterializationRequirement;

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
	
	private transient BigdataValueFactory vf;
    
    public interface Annotations extends ImmutableBOp.Annotations {

        /**
         * The operation to be applied to the left and right operands
         * (required). The value of this annotation is a {@link MathOp}, such as
         * {@link MathOp#PLUS}.
         * 
         * @see MathOp
         */
        String OP = (MathBOp.class.getName() + ".op").intern();

        public String NAMESPACE = (MathBOp.class.getName() + ".namespace").intern();
    }
    
	public enum MathOp {
		PLUS,
		MINUS,
		MULTIPLY,
		DIVIDE,
		MIN,
		MAX;

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
    		final IValueExpression<? extends IV> right, final MathOp op,final String lex) {

        this(new BOp[] { left, right }, NV.asMap(new NV(Annotations.OP, op),new NV(Annotations.NAMESPACE, lex)));

    }

	/**
	 * Required shallow copy constructor.
	 * 
	 * @param args
	 *            The operands.
	 * @param op
	 *            The operation.
	 */
    public MathBOp(final BOp[] args, Map<String,Object> anns) {
    
        super(args,anns);

		if (args.length != 2 || args[0] == null || args[1] == null
				|| getProperty(Annotations.OP) == null
				|| getProperty(Annotations.NAMESPACE)==null) {

			throw new IllegalArgumentException();
		
		}

    }

    /**
     * Required deep copy constructor.
     * 
     * @param op
     */
    public MathBOp(final MathBOp op) {

        super(op);
        
    }

    final public IV get(final IBindingSet bs) {
        
        final IV left = left().get(bs);

        // not yet bound?
        if (left == null)
        	throw new SparqlTypeErrorException.UnboundVarException();

        final IV right = right().get(bs);
        
        // not yet bound?
        if (right == null)
        	throw new SparqlTypeErrorException.UnboundVarException();
        
        try {
        
        	if (log.isDebugEnabled()) {
        		log.debug(toString(left.toString(), right.toString()));
        	}
        	
        	if (left.isInline() && right.isInline()) {
        	
        		return IVUtility.numericalMath(left, right, op());
        		
        	} else {
        		
        		final BigdataValue val1 = left.getValue();
        		
        		final BigdataValue val2 = right.getValue();
        		
        		if (val1 == null || val2 == null)
        			throw new NotMaterializedException();
        		
        		if (!(val1 instanceof Literal) || !(val2 instanceof Literal)) {
        			throw new SparqlTypeErrorException();
        		}
        		try{
        		return IVUtility.literalMath((Literal) val1, (Literal) val2,
        				op());
        		}catch(IllegalArgumentException iae){
        		    return DateTimeUtility.dateTimeMath((Literal)val1, left,
        		            (Literal)val2, right, op(), vf());
        		}
        	}
        	
        } catch (IllegalArgumentException ex) {
        	
        	if (log.isDebugEnabled()) {
        		log.debug("illegal argument, filtering solution");
        	}
        	
        	throw new SparqlTypeErrorException();
        	
        }

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
        if (vf == null) {
            synchronized (this) {
                if (vf == null) {
                    final String namespace = (String) getRequiredProperty(Annotations.NAMESPACE);
                    vf = BigdataValueFactoryImpl.getInstance(namespace);
                }
            }
        }
        return vf;
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
