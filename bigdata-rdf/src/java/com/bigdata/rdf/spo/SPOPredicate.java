/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.spo;

import java.util.Arrays;

import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;

/**
 * A predicate that is a triple with one or more variables. While the general
 * case allows a predicate to have an arbitrary name, for RDFS reasoning we are
 * only concerned with predicates of the form <code>triple(s,p,o)</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOPredicate implements IPredicate<ISPO> {

    /**
     * 
     */
    private static final long serialVersionUID = 1396017399712849975L;

    private final String[] relationName;
    
    private final IVariableOrConstant<Long> s;

    private final IVariableOrConstant<Long> p;

    private final IVariableOrConstant<Long> o;

    /** The context position MAY be <code>null</code>. */
    private final IVariableOrConstant<Long> c;

    private final boolean optional;
    
    private final IElementFilter<ISPO> constraint;

    private final ISolutionExpander<ISPO> expander;
    
    public String getOnlyRelationName() {
        
        if (relationName.length != 1)
            throw new IllegalStateException();
        
        return relationName[0];
        
    }
    
    public String getRelationName(int index) {
        
        return relationName[index];
        
    }
    
    public int getRelationCount() {
        
        return relationName.length;
        
    }

    /**
     * The arity is 3 unless the context position was given (as either a
     * variable or bound to a constant) in which case it is 4.
     */
    public final int arity() {
        
        return c == null ? 3 : 4;
        
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
    public SPOPredicate(String relationName, IVariableOrConstant<Long> s,
            IVariableOrConstant<Long> p, IVariableOrConstant<Long> o) {

        this(new String[] { relationName }, s, p, o, null/* c */,
                false/* optional */, null/* constraint */, null/* expander */);
        
    }
    
    /**
     * Partly specified ctor. The context will be <code>null</code>. 
     * No constraint is specified. No expander is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param optional
     */
    public SPOPredicate(String relationName, IVariableOrConstant<Long> s,
            IVariableOrConstant<Long> p, IVariableOrConstant<Long> o, 
            final boolean optional) {

        this(new String[] { relationName }, s, p, o, null/* c */,
                optional, null/* constraint */, null/* expander */);
        
    }
    
    /**
     * Partly specified ctor. The context will be <code>null</code>. 
     * No constraint is specified. No expander is specified.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param expander
     *            MAY be <code>null</code>.
     */
    public SPOPredicate(String relationName, IVariableOrConstant<Long> s,
            IVariableOrConstant<Long> p, IVariableOrConstant<Long> o, 
            ISolutionExpander<ISPO> expander) {

        this(new String[] { relationName }, s, p, o, null/* c */,
                false/* optional */, null/* constraint */, expander);
        
    }
    
    /**
     * Partly specified ctor. The context will be <code>null</code>. 
     * No constraint is specified. 
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @param optional
     * @param expander
     *            MAY be <code>null</code>.
     */
    public SPOPredicate(String relationName, IVariableOrConstant<Long> s,
            IVariableOrConstant<Long> p, IVariableOrConstant<Long> o, 
            final boolean optional, ISolutionExpander<ISPO> expander) {

        this(new String[] { relationName }, s, p, o, null/* c */,
                optional, null/* constraint */, expander);
        
    }
    
    /**
     * Fully specified ctor.
     * 
     * @param relationName
     * @param s
     * @param p
     * @param o
     * @parma c MAY be <code>null</code>.
     * @param optional
     * @param constraint
     *            MAY be <code>null</code>.
     * @param expander
     *            MAY be <code>null</code>.
     */
    public SPOPredicate(String[] relationName,
            IVariableOrConstant<Long> s,//
            IVariableOrConstant<Long> p,//
            IVariableOrConstant<Long> o,//
            IVariableOrConstant<Long> c,//
            final boolean optional,
            IElementFilter<ISPO> constraint,//
            ISolutionExpander<ISPO> expander
            ) {
        
        if (relationName == null)
            throw new IllegalArgumentException();
       
        for (int i = 0; i < relationName.length; i++) {
            
            if (relationName[i] == null)
                throw new IllegalArgumentException();
            
        }
        
        if (relationName.length == 0)
            throw new IllegalArgumentException();
        
        if (s == null)
            throw new IllegalArgumentException();
        
        if (p == null)
            throw new IllegalArgumentException();
        
        if (o == null)
            throw new IllegalArgumentException();
        
        this.relationName = relationName;
        
        this.s = s;
        this.p = p;
        this.o = o;
        this.c = c; // MAY be null.

        this.optional = optional;
        
        this.constraint = constraint; /// MAY be null.
        
        this.expander = expander;// MAY be null.
        
    }

    /**
     * Copy constructor overrides the relation name(s).
     * 
     * @param relationName
     *            The new relation name(s).
     */
    protected SPOPredicate(SPOPredicate src, String[] relationName) {
        
        if (relationName == null)
            throw new IllegalArgumentException();
       
        for(int i=0; i<relationName.length; i++) {
            
            if (relationName[i] == null)
                throw new IllegalArgumentException();
            
        }
        
        if (relationName.length == 0)
            throw new IllegalArgumentException();
 
        this.s = src.s;
        this.p = src.p;
        this.o = src.o;
        this.c = src.c;
        
        this.relationName = relationName; // override.
     
        this.optional = src.optional;
        
        this.constraint = src.constraint;
        
        this.expander = src.expander;
        
    }

    public final IVariableOrConstant<Long> get(int index) {
        switch (index) {
        case 0:
            return s;
        case 1:
            return p;
        case 2:
            return o;
        case 3:
            if(c!=null) return c;
            // fall through
        default:
            throw new IndexOutOfBoundsException(""+index);
        }
    }
    
//    /**
//     * Return the index of the variable or constant in the {@link Predicate}.
//     * 
//     * @param t
//     *            The variable or constant.
//     * 
//     * @return The index of that variable or constant. The index will be 0 for
//     *         the subject, 1 for the predicate, or 2 for the object. if the
//     *         variable or constant does not occur in this {@link Predicate} then
//     *         <code>-1</code> will be returned.
//     */
//    public final int indexOf(VarOrConstant t) {
//
//        // variables use a singleton factory.
//        if( s == t ) return 0;
//        if( p == t ) return 1;
//        if( o == t ) return 2;
//        
//        // constants do not give the same guarentee.
//        if(s.equals(t)) return 0;
//        if(p.equals(t)) return 1;
//        if(o.equals(t)) return 2;
//        
//        return -1;
//        
//    }

    final public IVariableOrConstant<Long> s() {
        
        return s;
        
    }
    
    final public IVariableOrConstant<Long> p() {
        
        return p;
        
    }

    final public IVariableOrConstant<Long> o() {
        
        return o;
        
    }
    
    final public IVariableOrConstant<Long> c() {
        
        return c;
        
    }
    
    /**
     * Return true iff the {s,p,o} arguments of the predicate are bound (vs
     * variables) - the context position is NOT tested.
     */
    final public boolean isFullyBound() {

        return !s.isVar() && !p.isVar() && !o.isVar();

    }

    /**
     * The #of arguments in the predicate that are variables (vs constants) (the
     * context position is NOT counted).
     */
    final public int getVariableCount() {
        
        return (s.isVar() ? 1 : 0) + (p.isVar() ? 1 : 0) + (o.isVar() ? 1 : 0);
        
    }
    
    public SPOPredicate asBound(IBindingSet bindingSet) {
        
        final IVariableOrConstant<Long> s;
        {
            if (this.s.isVar() && bindingSet.isBound((IVariable) this.s)) {

                s = bindingSet.get((IVariable) this.s);

            } else {

                s = this.s;

            }
        }
        
        final IVariableOrConstant<Long> p;
        {
            if (this.p.isVar() && bindingSet.isBound((IVariable)this.p)) {

                p = bindingSet.get((IVariable) this.p);

            } else {

                p = this.p;

            }
        }
        
        final IVariableOrConstant<Long> o;
        {
            if (this.o.isVar() && bindingSet.isBound((IVariable) this.o)) {

                o = bindingSet.get((IVariable) this.o);

            } else {

                o = this.o;

            }
        }
        
        final IVariableOrConstant<Long> c;
        {
            if (this.c != null && this.c.isVar()
                    && bindingSet.isBound((IVariable) this.c)) {

                c = bindingSet.get((IVariable) this.c);

            } else {

                c = this.c;

            }
        }
        
        return new SPOPredicate(relationName, s, p, o, c, optional, constraint,
                expander);
        
    }

    public SPOPredicate setRelationName(String[] relationName) {

        return new SPOPredicate(this, relationName);
        
    }
    
    public String toString() {

        return toString(null);
        
    }

    public String toString(IBindingSet bindingSet) {

        StringBuilder sb = new StringBuilder();

        sb.append("(");

        sb.append(Arrays.toString(relationName));

        sb.append(", ");
        
        sb.append(s.isConstant() || bindingSet == null
                || !bindingSet.isBound((IVariable) s) ? s.toString()
                : bindingSet.get((IVariable) s));

        sb.append(", ");

        sb.append(p.isConstant() || bindingSet == null
                || !bindingSet.isBound((IVariable) p) ? p.toString()
                : bindingSet.get((IVariable) p));

        sb.append(", ");

        sb.append(o.isConstant() || bindingSet == null
                || !bindingSet.isBound((IVariable) o) ? o.toString()
                : bindingSet.get((IVariable) o));

        if (c != null) {

            sb.append(", ");

            sb.append(c.isConstant() || bindingSet == null
                    || !bindingSet.isBound((IVariable) c) ? c.toString()
                    : bindingSet.get((IVariable) c));

        }
        
        sb.append(")");

        if(isOptional()) {
            
            sb.append("[optional]");
            
        }
        
        return sb.toString();

    }

    final public boolean isOptional() {
        
        return optional;
        
    }
    
    final public IElementFilter<ISPO> getConstraint() {

        return constraint;
        
    }

    final public ISolutionExpander<ISPO> getSolutionExpander() {
        
        return expander;
        
    }

    public boolean equals(IPredicate<ISPO> other) {
        
        if (this == other)
            return true;
        
        final int arity = arity();
        
        if(arity != other.arity()) return false;
        
        for(int i=0; i<arity; i++) {
            
            final IVariableOrConstant<Long> x = get(i);
            
            final IVariableOrConstant<Long> y = other.get(i);
            
            // handles context when null.
            if (x == null && y != null)
                return false;
            
            // handles non-null on this predicate.
            if (!x.equals(y))
                return false; 
            
        }
        
        return true;
        
    }

//    public boolean equals(SPOPredicate other) {
//        
//        if(this == other) return true;
//        
//        if(!s.equals(other.s)) return false;
//        
//        if(!p.equals(other.p)) return false;
//        
//        if(!o.equals(other.o)) return false;
//        
//        return true;
//        
//    }

}
