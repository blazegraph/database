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
import java.util.UUID;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.IStarJoin;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.Var;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class SPOStarJoin extends SPOPredicate 
        implements IStarJoin<ISPO>, Serializable {

    /**
     * generated serial version UID 
     */
    private static final long serialVersionUID = 981603459301801862L;
    
    protected final Collection<IStarConstraint> starConstraints;

    public SPOStarJoin(final SPOPredicate pred) {
        
        this(pred.relationName, pred.partitionId, pred.s(), Var.var(), 
                Var.var(), pred.c(), pred.isOptional(), 
                pred.getConstraint(), pred.getSolutionExpander());
        
    }
    
    public SPOStarJoin(final String relationName,
            final IVariableOrConstant<Long> s,
            final IVariableOrConstant<Long> p, 
            final IVariableOrConstant<Long> o) {

        this(new String[] { relationName }, -1/* partitionId */, s, p, o,
                null/* c */, false/* optional */, null/* constraint */, 
                null/* expander */);

    }
    
    public SPOStarJoin(final String[] relationName, //
            final int partitionId, //
            final IVariableOrConstant<Long> s,//
            final IVariableOrConstant<Long> p,//
            final IVariableOrConstant<Long> o,//
            final IVariableOrConstant<Long> c,//
            final boolean optional, //
            final IElementFilter<ISPO> constraint,//
            final ISolutionExpander<ISPO> expander//
            ) {
        
        super(relationName, partitionId, s, p, o, c, optional, constraint, 
                expander);
    
        this.starConstraints = new LinkedList<IStarConstraint>();
        
    }
    
    public void addStarConstraint(IStarConstraint constraint) {
        
        starConstraints.add(constraint);
        
    }
    
    public Iterator<IStarConstraint> getStarConstraints() {
        
        return starConstraints.iterator();
        
    }
    
    public int getNumStarConstraints() {
        
        return starConstraints.size();
        
    }
    
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
    
    public static class SPOStarConstraint implements IStarConstraint<SPO>, 
            Serializable {
        
        /**
         * generated serial version UID
         */
        private static final long serialVersionUID = 997244773880938817L;

        protected final IVariableOrConstant<Long> p, o;
        
        protected final boolean optional;
        
        public SPOStarConstraint(final IVariableOrConstant<Long> p, 
                final IVariableOrConstant<Long> o) {
            
            this(p, o, false /* optional */);
            
        }
        
        public SPOStarConstraint(final IVariableOrConstant<Long> p, 
                final IVariableOrConstant<Long> o, final boolean optional) {
        
            this.p = p;
            
            this.o = o;
            
            this.optional = optional;
            
        }
        
        final public IVariableOrConstant<Long> p() {
            
            return p;
            
        }

        final public IVariableOrConstant<Long> o() {
            
            return o;
            
        }
        
        final public boolean isOptional() {
            
            return optional;
            
        }
        
        final public int getNumVars() {
            
            return (p.isVar() ? 1 : 0) + (o.isVar() ? 1 : 0);

        }
        
        final public boolean isMatch(SPO spo) {
            
            return ((p.isVar() || p.get() == spo.p) &&
                    (o.isVar() || o.get() == spo.o));    
            
        }
        
        final public void bind(IBindingSet bs, SPO spo) {
            
            if (p.isVar()) {
                
                bs.set((IVariable) p, 
                    new Constant<Long>(spo.p));
                
            }
            
            if (o.isVar()) {
                
                bs.set((IVariable) o, 
                    new Constant<Long>(spo.o));
                
            }
            
        }
        
        public IStarConstraint asBound(IBindingSet bindingSet) {
            
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
