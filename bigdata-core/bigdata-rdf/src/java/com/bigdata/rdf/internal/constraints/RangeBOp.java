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
package com.bigdata.rdf.internal.constraints;

import java.util.LinkedHashMap;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.bop.ModifiableBOpBase;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;

/**
 * Operator used to impose a key-range constraint on a variable on access path.
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/238 (Lift range
 *      constraints onto AP).
 *      
 * @author mrpersonick
 */
@SuppressWarnings("rawtypes")
final public class RangeBOp extends ModifiableBOpBase { // implements IVariable<Range> {

    /**
	 * 
	 */
    private static final long serialVersionUID = 3368581489737593349L;

//	private static final Logger log = Logger.getLogger(RangeBOp.class);
	
    public interface Annotations extends ImmutableBOp.Annotations {

//        /**
//         * The variable whose range is restricted by the associated
//         * {@link #FROM} and/or {@link #TO} filters.
//         */
//		String VAR = RangeBOp.class.getName() + ".var";

		/** The inclusive lower bound. */
		String FROM = RangeBOp.class.getName() + ".from";
		
		/** The exclusive upper bound. */
		String TO = RangeBOp.class.getName() + ".to";
		
    }
    
//    /** Cached to/from lookups. */
//    private transient volatile IValueExpression<IV> to, from;

    public RangeBOp() {
    	
        this(BOp.NOARGS, new LinkedHashMap<String,Object>());
        
    }
    
//    public RangeBOp(final IVariable<? extends IV> var) {
//    	
//        this(BOp.NOARGS, NV.asMap(new NV(Annotations.VAR, var)));
//        
//    }
    
    public RangeBOp(//final IVariable<? extends IV> var,
    		final IValueExpression<? extends IV> from, 
    		final IValueExpression<? extends IV> to) {

        this(BOp.NOARGS, NV.asMap(
//    			new NV(Annotations.VAR, var),
			    new NV(Annotations.FROM, from),
				new NV(Annotations.TO, to)));

    }

	/**
	 * Required shallow copy constructor.
	 */
    public RangeBOp(final BOp[] args, final Map<String,Object> anns) {
        super(args,anns);
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public RangeBOp(final RangeBOp op) {
        super(op);
    }

//    @SuppressWarnings("unchecked")
//    public IVariable<? extends IV> var() {
//    	return (IVariable<? extends IV>) getProperty(Annotations.VAR);
//    }
    
    @SuppressWarnings("unchecked")
	public IValueExpression<? extends IV> from() {
		return (IValueExpression<? extends IV>) getProperty(Annotations.FROM);
	}
    
    @SuppressWarnings("unchecked")
	public IValueExpression<? extends IV> to() {
		return (IValueExpression<? extends IV>) getProperty(Annotations.TO);
    }
    
    public void setFrom(final IValueExpression<? extends IV> from) {
    	setProperty(Annotations.FROM, from);
    }

    public void setTo(final IValueExpression<? extends IV> to) {
    	setProperty(Annotations.TO, to);
    }

//    final public Range get(final IBindingSet bs) {
//        
////    	log.debug("getting the asBound value");
//    	
//    	final IV from = from().get(bs);
//    	
////    	log.debug("from: " + from);
//    	
//    	// sort of like Var.get(), which returns null when the variable
//    	// is not yet bound
//		if (from == null)
//			return null;
//    	
//    	final IV to = to().get(bs);
//    	
////    	log.debug("to: " + to);
//    	
//    	// sort of like Var.get(), which returns null when the variable
//    	// is not yet bound
//		if (to == null)
//			return null;
//
//    	try {
//    	    /*
//    	     * FIXME Should handle from/to (non-)exclusive boundaries using a
//    	     * successor pattern.
//    	     */
//    		// let Range ctor() do the type checks and valid range checks
//    		return new Range(from, to);
//    	} catch (IllegalArgumentException ex) {
//    		// log the reason the range is invalid
////    		if (log.isInfoEnabled())
////    			log.info("dropping solution: " + ex.getMessage());
//    		// drop the solution
//    		throw new SparqlTypeErrorException();
//    	}
//    	
//    }
//    
    final public RangeBOp asBound(final IBindingSet bs) {

    	/*
    	 * Only care if we change from a non-null, non-Constant to a ground IV.
    	 */
    	final IValueExpression<? extends IV> origFrom = from();
    	final IValueExpression<? extends IV> origTo = to();
    	
		IValueExpression<? extends IV> asBoundFrom;
		{
			if (origFrom == null) {
				asBoundFrom = null;
			} else if (origFrom instanceof IConstant) {
				asBoundFrom = origFrom;
			} else {
				try {
					final IV iv = origFrom.get(bs);
					asBoundFrom = new Constant<IV>(iv);
				} catch (SparqlTypeErrorException ex) {
					asBoundFrom = origFrom;
				}
			}
		}
		
		IValueExpression<? extends IV> asBoundTo;
		{
			if (origTo == null) {
				asBoundTo = null;
			} else if (origTo instanceof IConstant) {
				asBoundTo = origTo;
			} else {
				try {
					final IV iv = origTo.get(bs);
					asBoundTo = new Constant<IV>(iv);
				} catch (SparqlTypeErrorException ex) {
					asBoundTo = origTo;
				}
			}
		}
		
		/*
		 * Null means no value expression, constant value expression, or not
		 * able to evaluate at this time.  Non-null means the asBound is 
		 * different from the original.
		 */
		if (asBoundFrom == origFrom && asBoundTo == origTo) {
			return this;
		}
		
		final RangeBOp asBound = new RangeBOp();
		if (asBoundFrom != null)
			asBound.setFrom(asBoundFrom);
		if (asBoundTo != null)
			asBound.setTo(asBoundTo);
		
		return asBound;
		
	}
    
//    final public boolean isFullyBound() {
//    	
//    	return (from() == null || from() instanceof IConstant) && 
//    		   (to() == null || to() instanceof IConstant);
//    	
//    }
    
    final public boolean isFromBound() {
    	return from() instanceof IConstant;
    }
    
    final public boolean isToBound() {
    	return to() instanceof IConstant;
    }
    
//
////	@Override
//	public boolean isVar() {
//		return true;
//	}
//
////	@Override
//	public boolean isConstant() {
//		return false;
//	}
//
////	@Override
//	public Range get() {
////		log.debug("somebody tried to get me");
//		
//		return null;
//	}
//
////	@Override
//	public String getName() {
//		return var().getName();
//	}
//
////	@Override
//	public boolean isWildcard() {
//		return false;
//	}
//
//
//	/*
//	 * TODO The default BOp equals() and hashCode() should be fine.
//	 */
//	
//	// TODO This looks dangerous. It is only considering the variable!
//    final public boolean equals(final IVariableOrConstant op) {
//
//    	if (op == null)
//    		return false;
//    	
//    	if (this == op) 
//    		return true;
//
//        if (op instanceof IVariable<?>) {
//
//            return var().getName().equals(((IVariable<?>) op).getName());
//
//        }
//        
//        return false;
//    	
//    }
//    
////    final private boolean _equals(final RangeBOp op) {
////    	
////    	return var().equals(op.var())
////    		&& from().equals(op.from())
////    		&& to().equals(op.to());
////
////    }
//    
//	/**
//	 * Caches the hash code.
//	 */
////	private int hash = 0;
//	public int hashCode() {
////		
////		int h = hash;
////		if (h == 0) {
////			h = 31 * h + var().hashCode();
////			h = 31 * h + from().hashCode();
////			h = 31 * h + to().hashCode();
////			hash = h;
////		}
////		return h;
////
//		return var().hashCode();
//	}

}
