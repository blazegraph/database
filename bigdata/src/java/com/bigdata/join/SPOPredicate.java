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
package com.bigdata.join;

/**
 * A predicate that is a triple with one or more variables. While the general
 * case allows a predicate to have an arbitrary name, for RDFS reasoning we are
 * only concerned with predicates of the form <code>triple(s,p,o)</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOPredicate implements IPredicate<ISPO> {

    private final IPredicateConstraint<ISPO> constraint;

    private final IVariableOrConstant<Long> s;

    private final IVariableOrConstant<Long> p;

    private final IVariableOrConstant<Long> o;

    public final int arity() {
        
        return 3;
        
    }

    public SPOPredicate(IVariableOrConstant<Long> s,
            IVariableOrConstant<Long> p, IVariableOrConstant<Long> o) {

        this(null, s, p, o);
        
    }
    
    public SPOPredicate(IPredicateConstraint<ISPO> constraint,
            IVariableOrConstant<Long> s, IVariableOrConstant<Long> p,
            IVariableOrConstant<Long> o) {
        
        assert s != null;
        assert p != null;
        assert o != null;
        
        this.constraint = constraint;
        
        this.s = s;
        this.p = p;
        this.o = o;
        
    }

    public final IVariableOrConstant<Long> get(int index) {
        switch (index) {
        case 0:
            return s;
        case 1:
            return p;
        case 2:
            return o;
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
    
    /**
     * Return true iff all arguments of the predicate are bound (vs
     * variables).
     */
    public boolean isConstant() {

        return !s.isVar() && !p.isVar() && !o.isVar();

    }

    /**
     * The #of arguments in the predicate that are variables (vs constants).
     */
    public int getVariableCount() {
        
        return (s.isVar() ? 1 : 0) + (p.isVar() ? 1 : 0) + (o.isVar() ? 1 : 0);
        
    }
    
    public SPOPredicate asBound(IBindingSet bindingSet) {
        
        final IVariableOrConstant<Long> s;
        {
            if (this.s.isVar() && bindingSet.isBound(this.s.getName())) {

                s = new Constant<Long>((Long)bindingSet.get(((Var) this.s).name));

            } else {

                s = this.s;

            }
        }
        
        final IVariableOrConstant<Long> p;
        {
            if (this.p.isVar() && bindingSet.isBound(this.s.getName())) {

                p = new Constant<Long>((Long)bindingSet.get(((Var) this.p).name));

            } else {

                p = this.p;

            }
        }
        
        final IVariableOrConstant<Long> o;
        {
            if (this.o.isVar() && bindingSet.isBound(this.s.getName())) {

                o = new Constant<Long>((Long)bindingSet.get(((Var) this.o).name));

            } else {

                o = this.o;

            }
        }
        
        return new SPOPredicate(constraint,s,p,o);
        
    }

    public void copyValues(ISPO spo, IBindingSet bindingSet ) {

        if(s.isVar()) {
            
            bindingSet.setLong(s.getName(), spo.s());
            
        }
        
        if(p.isVar()) {
            
            bindingSet.setLong(p.getName(), spo.p());
            
        }

        if(o.isVar()) {
            
            bindingSet.setLong(o.getName(), spo.o());
            
        }
        
    }
    
    public String toString() {

        return toString(null);
        
    }

    public String toString(IBindingSet bindingSet) {

        StringBuilder sb = new StringBuilder();

        sb.append("(");

        sb.append(s.isConstant() || bindingSet == null ? s.toString()
                : bindingSet.get(s.getName()));

        sb.append(", ");

        sb.append(p.isConstant() || bindingSet == null ? p.toString()
                : bindingSet.get(p.getName()));

        sb.append(", ");

        sb.append(o.isConstant() || bindingSet == null ? o.toString()
                : bindingSet.get(o.getName()));

        sb.append(")");

        return sb.toString();

    }

    public IPredicateConstraint<ISPO> getConstraint() {

        return constraint;
        
    }

}
