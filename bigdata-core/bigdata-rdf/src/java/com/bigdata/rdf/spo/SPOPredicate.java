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
package com.bigdata.rdf.spo;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.relation.rule.IAccessPathExpander;

/**
 * A predicate that is a triple with one or more variables. While the general
 * case allows a predicate to have an arbitrary name, for RDFS reasoning we are
 * only concerned with predicates of the form <code>triple(s,p,o)</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Remove reliance on the {@link SPOPredicate}. It only adds the s(), p(),
 *       o(), and c() methods.
 */
public class SPOPredicate extends Predicate<ISPO> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 3517916629931687107L;

	public static interface Annotations extends Predicate.Annotations {

		/**
		 * The SID {@link IVariable} (optional).
		 */
		String SID = SPOPredicate.class.getName() + ".sid";
		
		/**
		 * Include historical SPOs (type == StatementEnum.History).  By default
		 * these will be filtered out.
		 * 
		 * @see {@link StatementEnum#History}
		 */
		String INCLUDE_HISTORY = SPOPredicate.class.getName() + ".includeHistory";

	}
	
    /**
     * Variable argument version of the shallow copy constructor.
     */
    public SPOPredicate(final BOp[] values, final NV... annotations) {
        super(values, annotations);
    }

    /**
     * Required shallow copy constructor.
     */
    public SPOPredicate(final BOp[] values, final Map<String, Object> annotations) {
        super(values, annotations);
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public SPOPredicate(final SPOPredicate op) {
        super(op);
    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. The
     * predicate is NOT optional. No constraint is specified. No expander is
     * specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     */
    @SuppressWarnings("rawtypes")
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o) {

//        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
//                null/* c */, false/* optional */, null/* constraint */, null/* expander */);
        super(
                new IVariableOrConstant[] { s, p, o }, //
                new NV(Annotations.RELATION_NAME, new String[] { relationName }) //
        );

    }

    /**
     * Partly specified ctor. The predicate is NOT optional. No constraint is
     * specified. No expander is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param c
     */
    @SuppressWarnings("rawtypes")
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o,
            final IVariableOrConstant<IV> c) {

//        this(new String[] { relationName }, -1/* partitionId */, s, p, o, c,
//                false/* optional */, null/* constraint */, null/* expander */);
        super(new IVariableOrConstant[] { s, p, o, c }, //
                new NV(Annotations.RELATION_NAME, relationName) //
        );

    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. The
     * predicate is NOT optional. No constraint is specified. No expander is
     * specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     */
    @SuppressWarnings("rawtypes")
    public SPOPredicate(final String[] relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o) {

//        this(relationName, -1/* partitionId */, s, p, o,
//                null/* c */, false/* optional */, null/* constraint */, null/* expander */);
        super(new IVariableOrConstant[] { s, p, o }, //
                new NV(Annotations.RELATION_NAME, relationName));

    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. No
     * constraint is specified. No expander is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param optional
     */
    @SuppressWarnings("rawtypes")
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o,
            final boolean optional) {

//        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
//                null/* c */, optional, null/* constraint */, null/* expander */);
        super(new IVariableOrConstant[] { s, p, o }, //
                new NV(Annotations.RELATION_NAME, new String[]{relationName}), //
                new NV(Annotations.OPTIONAL, optional));

    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. No
     * constraint is specified. No expander is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param expander
     *            MAY be <code>null</code>.
     */
    @SuppressWarnings("rawtypes")
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o,
            final IAccessPathExpander<ISPO> expander) {

//        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
//                null/* c */, false/* optional */, null/* constraint */,
//                expander);
        super(new IVariableOrConstant[] { s, p, o }, //
                new NV(Annotations.RELATION_NAME, new String[]{relationName}), //
                new NV(Annotations.ACCESS_PATH_EXPANDER, expander));

    }

    /**
     * Partly specified ctor. The context will be <code>null</code>. No
     * constraint is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param optional
     * @param expander
     *            MAY be <code>null</code>.
     */
    @SuppressWarnings("rawtypes")
    public SPOPredicate(final String relationName,
            final IVariableOrConstant<IV> s,
            final IVariableOrConstant<IV> p,
            final IVariableOrConstant<IV> o, final boolean optional,
            final IAccessPathExpander<ISPO> expander) {

//        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
//                null/* c */, optional, null/* constraint */, expander);
        super(new IVariableOrConstant[] { s, p, o }, //
                new NV(Annotations.RELATION_NAME, new String[]{relationName}), //
                new NV(Annotations.OPTIONAL, optional), //
                new NV(Annotations.ACCESS_PATH_EXPANDER, expander));
    
    }

//    /**
//     * Constrain the predicate by setting the context position. If the context
//     * position on the {@link SPOPredicate} is non-<code>null</code>, then you
//     * must use {@link #asBound(IBindingSet)} to replace all occurrences of the
//     * variable appearing in the context position of the predicate with the
//     * desired constant. If the context position is already bound the a
//     * constant, then you can not modify it (you can only increase the
//     * constraint, not change the constraint).
//     * 
//     * @throws IllegalStateException
//     *             unless the context position on the {@link SPOPredicate} is
//     *             <code>null</code>.
//     * 
//     * @deprecated With {@link DefaultGraphSolutionExpander} and
//     *             {@link NamedGraphSolutionExpander}.
//     */
//    @SuppressWarnings("rawtypes")
//    public SPOPredicate setC(final IConstant<IV> c) {
//
//        if (c == null)
//            throw new IllegalArgumentException();
//
//        if (this.get(3) != null)
//            throw new IllegalStateException("Context not null: " + c);
//
//        final SPOPredicate tmp = this.clone();
//
//        tmp._set(3/*c*/, c);
//        
//        return tmp;
//
//    }
    
    @Override
    public SPOPredicate clone() {

        // Fast path for clone().
        return new SPOPredicate(this);
//        return (SPOPredicate) super.clone();
        
    }
    
	/**
	 * The variable or constant for the subject position (required).
	 */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    final public IVariableOrConstant<IV> s() {
        
        return (IVariableOrConstant<IV>) get(0/* s */);
        
    }
    
	/**
	 * The variable or constant for the predicate position (required).
	 */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    final public IVariableOrConstant<IV> p() {
        
        return (IVariableOrConstant<IV>) get(1/* p */);
        
    }

	/**
	 * The variable or constant for the object position (required).
	 */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    final public IVariableOrConstant<IV> o() {
        
        return (IVariableOrConstant) get(2/* o */);
        
    }
    
	/**
	 * The variable or constant for the context position (required iff in quads
	 * mode).
	 */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    final public IVariableOrConstant<IV> c() {
        
        return (IVariableOrConstant<IV>) get(3/* c */);
        
    }

	/**
	 * The variable for the statement identifier (optional). The statement
	 * identifier is the composition of the (subject, predicate, and object)
	 * positions of the fully predicate. When this variable is declared, it will
	 * be bound to a {@link SidIV} for matched statements.
	 * 
	 * @see Annotations#SID
	 */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	final public IVariable<IV> sid() {

		return (IVariable<IV>) getProperty(Annotations.SID);

	}
    
    /*
     * Note: Moved to Predicate. See notes there before putting back into
     * use.
     */

//    final public RangeBOp range() {
//    	
//    	return (RangeBOp) getProperty(Annotations.RANGE);
//    	
//    }

    /**
     * If true, do not filter out {@link StatementEnum#History} SPOs.
     */
    public final boolean includeHistory() {
        
        return getProperty(Annotations.INCLUDE_HISTORY, false);
        
    }
    
    /**
     * Strengthened return type.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public SPOPredicate asBound(final IBindingSet bindingSet) {

    	final IVariable<?> sidVar = sid();
    	
    	if (sidVar != null && bindingSet.isBound(sidVar)) {
    		
    		final Object obj = bindingSet.get(sidVar).get();
    		
    		// prior predicate bound something other than a sid to a sid var
    		if (obj instanceof SidIV == false) {
    			
    			// inconsistent
    			return null;
    			
    		}
    		
    		final SidIV sidIV = (SidIV) obj;
    		
    		final ISPO spo = sidIV.getInlineValue();
    		
    		final IV s = spo.s();
    		
    		final IV p = spo.p();
    		
    		final IV o = spo.o();
    		
    		// TODO implement RDR in quads mode
//    		final IV c = spo.c();
    		
    		if (this.s().isConstant() && !this.s().get().equals(s)) {
    			
    			// inconsistent
    			return null;
    			
    		} else if (this.s().isVar()) {
    			
    			bindingSet.set((IVariable) this.s(), new Constant<IV>(s)); 
    			
    		}
    		
    		if (this.p().isConstant() && !this.p().get().equals(p)) {
    			
    			// inconsistent
    			return null;
    			
    		} else if (this.p().isVar()) {
    			
    			bindingSet.set((IVariable) this.p(), new Constant<IV>(p)); 
    			
    		}
    		
    		if (this.o().isConstant() && !this.o().get().equals(o)) {
    			
    			// inconsistent
    			return null;
    			
    		} else if (this.o().isVar()) {
    			
    			bindingSet.set((IVariable) this.o(), new Constant<IV>(o)); 
    			
    		}
    		
    		// TODO implement RDR in quads mode
//    		if (this.c().isConstant() && !this.c().get().equals(c)) {
//    			
//    			// inconsistent
//    			return null;
//    			
//    		} else if (this.c().isVar()) {
//    			
//    			bindingSet.set((IVariable) this.c(), new Constant<IV>(c)); 
//    			
//    		}

    	}
    	
        return (SPOPredicate) new SPOPredicate(argsCopy(), annotationsRef())
                ._asBound(bindingSet);

        /*
         * Note: Moved to Predicate. See notes there before putting back into
         * use.
         */
        
//        final RangeBOp rangeBOp = range();
//        
//        // we don't have a range bop for ?o
//        if (rangeBOp == null)
//        	return tmp;
//
//		/*
//		 * Attempt to evaluate the RangeBOp.
//		 */
//		final RangeBOp asBound = rangeBOp.asBound(bindingSet);
//
//		tmp._setProperty(Annotations.RANGE, asBound);
//
//		return tmp;

	}

//    /**
//     * Strengthened return type.
//     * <p>
//     * {@inheritDoc}
//     */
//    @Override
//    @SuppressWarnings("rawtypes")
//    public IV getValue(final int index) {
//
//        return (IV) super.get(index);
//
//    }
    
}
