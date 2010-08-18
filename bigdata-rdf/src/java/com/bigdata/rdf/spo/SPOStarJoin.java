/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.IStarJoin;

/**
 * Implementation of a star join for SPOs.  See {@link IStarJoin}.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class SPOStarJoin extends SPOPredicate 
        implements IStarJoin<ISPO>, Serializable {

    /**
     * generated serial version UID 
     */
    private static final long serialVersionUID = 981603459301801862L;
    
    /**
     * The star constraints for this star join.
     */
    protected final Collection<IStarConstraint<ISPO>> starConstraints;

    /**
     * Construct an SPO star join from a normal SPO predicate.  The star join
     * will have a triple pattern of (S,?,?) instead of the (S,P,O) from the
     * original SPO predicate.  This way all SPOs for the common subject are
     * considered.  SPO star constraints must be added later to make this
     * star join selective.
     *  
     * @param pred
     *          the normal SPO predicate from which to pull the S
     */
    public SPOStarJoin(final SPOPredicate pred) {
        
        this(pred.relationName, pred.partitionId, pred.s(), Var.var(), 
                Var.var(), pred.c(), pred.isOptional(), 
                pred.getConstraint(), pred.getSolutionExpander());
        
    }
    
    /**
     * Create an SPO star join over the given relation for the given subject.
     *  
     * @param relationName
     *          the name of the SPO relation to use
     * @param s
     *          the subject of this star join
     */
    public SPOStarJoin(final String relationName,
            final IVariableOrConstant<IV> s) {

        this(new String[] { relationName }, -1/* partitionId */, s, 
                Var.var(), Var.var(),
                null/* c */, false/* optional */, null/* constraint */, 
                null/* expander */);

    }
    
    /**
     * Fully specified ctor.
     * 
     * @param relationName
     * @param partitionId
     * @param s
     * @param p
     * @param o
     * @param c 
     *            MAY be <code>null</code>.
     * @param optional
     * @param constraint
     *            MAY be <code>null</code>.
     * @param expander
     *            MAY be <code>null</code>.
     */
    public SPOStarJoin(final String[] relationName, //
            final int partitionId, //
            final IVariableOrConstant<IV> s,//
            final IVariableOrConstant<IV> p,//
            final IVariableOrConstant<IV> o,//
            final IVariableOrConstant<IV> c,//
            final boolean optional, //
            final IElementFilter<ISPO> constraint,//
            final ISolutionExpander<ISPO> expander//
            ) {
        
        super(relationName, partitionId, s, p, o, c, optional, constraint, 
                expander);
    
        this.starConstraints = new LinkedList<IStarConstraint<ISPO>>();
        
    }
    
    /**
     * Add an SPO star constraint to this star join.
     */
    public void addStarConstraint(IStarConstraint<ISPO> constraint) {
        
        starConstraints.add(constraint);
        
    }
    
    /**
     * Return an iterator over the SPO star constraints for this star join.
     */
    public Iterator<IStarConstraint<ISPO>> getStarConstraints() {
        
        return starConstraints.iterator();
        
    }
    
    /**
     * Return the number of star constraints for this star join.
     */
    public int getNumStarConstraints() {
        
        return starConstraints.size();
        
    }
    
    /**
     * Return an iterator over the constraint variables for this star join.
     */
    public Iterator<IVariable> getConstraintVariables() {
        
        final Set<IVariable> vars = new HashSet<IVariable>();

        for (IStarConstraint constraint : starConstraints) {
            
            if (((SPOStarConstraint) constraint).p.isVar()) {
                vars.add((IVariable) ((SPOStarConstraint) constraint).p);
            }
            
            if (((SPOStarConstraint) constraint).o.isVar()) {
                vars.add((IVariable) ((SPOStarConstraint) constraint).o);
            }
            
        }
        
        return vars.iterator();
        
    }
    
    /**
     * Return an as-bound version of this star join and its star contraints
     * using the supplied binding set.
     */
    @Override
    public SPOPredicate asBound(IBindingSet bindingSet) {
        
        SPOPredicate pred = super.asBound(bindingSet);
        
        SPOStarJoin starJoin = new SPOStarJoin(pred.relationName, 
                pred.partitionId, pred.s, pred.p, pred.o, pred.c, pred.optional, 
                pred.constraint, pred.expander);
        
        for (IStarConstraint starConstraint : starConstraints) {
            
            starJoin.addStarConstraint(starConstraint.asBound(bindingSet));
            
        }
        
        return starJoin;
        
    }
    
    protected StringBuilder toStringBuilder(final IBindingSet bindingSet) {
        
        StringBuilder sb = super.toStringBuilder(bindingSet);
        
        if (starConstraints.size() > 0) {
    
            sb.append("star[");
            
            for (IStarConstraint sc : starConstraints) {
                
                sb.append(sc);
                
                sb.append(",");
                
            }
            
            sb.setCharAt(sb.length()-1, ']');
            
        }
        
        return sb;
        
    }
    
    /**
     * Implementation of a star constraint for SPOs.  Constraint will specify
     * a P and O (variable or constant) and whether the constraint is optional 
     * or non-optional.
     */
    public static class SPOStarConstraint implements IStarConstraint<ISPO>, 
            Serializable {
        
        /**
         * generated serial version UID
         */
        private static final long serialVersionUID = 997244773880938817L;

        /**
         * Variable or constant P for the constraint.
         */
        protected final IVariableOrConstant<IV> p;
        
        /**
         * Variable or constant O for the constraint.
         */
        protected final IVariableOrConstant<IV> o;
        
        /**
         * Is the constraint optional or non-optional.
         */
        protected final boolean optional;
        
        /**
         * Construct a non-optional SPO star constraint using the supplied P and
         * O.
         * 
         * @param p
         * @param o
         */
        public SPOStarConstraint(final IVariableOrConstant<IV> p, 
                final IVariableOrConstant<IV> o) {
            
            this(p, o, false /* optional */);
            
        }
        
        /**
         * Fully specified ctor.
         * 
         * @param p
         * @param o
         * @param optional
         */
        public SPOStarConstraint(final IVariableOrConstant<IV> p, 
                final IVariableOrConstant<IV> o, final boolean optional) {
        
            this.p = p;
            
            this.o = o;
            
            this.optional = optional;
            
        }
        
        final public IVariableOrConstant<IV> p() {
            
            return p;
            
        }

        final public IVariableOrConstant<IV> o() {
            
            return o;
            
        }
        
        final public boolean isOptional() {
            
            return optional;
            
        }
        
        final public int getNumVars() {
            
            return (p.isVar() ? 1 : 0) + (o.isVar() ? 1 : 0);

        }
        
        /**
         * Tests the P and O of the supplied SPO against the constraint.  Return
         * true for a match.
         */
        final public boolean isMatch(ISPO spo) {
            
            return ((p.isVar() || IVUtility.equals(p.get(), spo.p())) &&
                    (o.isVar() || IVUtility.equals(o.get(), spo.o())));    
            
        }
        
        /**
         * Use the supplied SPO to create variable bindings for supplied
         * binding set.
         */
        final public void bind(IBindingSet bs, ISPO spo) {
            
            if (p.isVar()) {
                
                bs.set((IVariable) p, 
                    new Constant<IV>(spo.p()));
                
            }
            
            if (o.isVar()) {
                
                bs.set((IVariable) o, 
                    new Constant<IV>(spo.o()));
                
            }
            
        }
        
        /**
         * Return an as-bound version of this SPO star constraint for the
         * supplied binding set.
         */
        public IStarConstraint<ISPO> asBound(IBindingSet bindingSet) {
            
            final IVariableOrConstant<IV> p;
            {
                if (this.p.isVar() && bindingSet.isBound((IVariable)this.p)) {

                    p = bindingSet.get((IVariable) this.p);

                } else {

                    p = this.p;

                }
            }
            
            final IVariableOrConstant<IV> o;
            {
                if (this.o.isVar() && bindingSet.isBound((IVariable) this.o)) {

                    o = bindingSet.get((IVariable) this.o);

                } else {

                    o = this.o;

                }
            }
            
            return new SPOStarConstraint(p, o, optional);
            
        }
        
        public String toString() {
            
            return toString(null);
            
        }
        
        public String toString(final IBindingSet bindingSet) {

            final StringBuilder sb = new StringBuilder();

            sb.append("(");

            sb.append(p.isConstant() || bindingSet == null
                    || !bindingSet.isBound((IVariable) p) ? p.toString()
                    : bindingSet.get((IVariable) p));

            sb.append(", ");

            sb.append(o.isConstant() || bindingSet == null
                    || !bindingSet.isBound((IVariable) o) ? o.toString()
                    : bindingSet.get((IVariable) o));

            sb.append(")");

            if (optional) {
                
                /*
                 * Something special, so do all this stuff.
                 */
                
                sb.append("[");
                
                if(optional) {
                    sb.append("optional");
                }

                sb.append("]");
                
            }
            
            return sb.toString();

        }

    }
    
}
