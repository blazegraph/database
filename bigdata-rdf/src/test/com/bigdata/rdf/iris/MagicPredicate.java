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
package com.bigdata.rdf.iris;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

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
public class MagicPredicate implements IPredicate<IMagicTuple> {

    protected static final Logger log = Logger.getLogger(MagicPredicate.class);
    
    public MagicPredicate copy() {
        
        return new MagicPredicate(this, relationName);
        
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1396017399712849975L;

    private final String[] relationName;

    private final int partitionId;
    
    /**
     * The terms (variables or constants) associated with this predicate.
     */
    private final List<IVariableOrConstant<Long>> terms;

    private final IElementFilter constraint;

    private final ISolutionExpander expander;
    
    public String getOnlyRelationName() {
        
        if (relationName.length != 1)
            throw new IllegalStateException();
        
        return relationName[0];
        
    }
    
    public String getRelationName(int index) {
        
        return relationName[index];
        
    }
    
    final public int getRelationCount() {
        
        return relationName.length;
        
    }

    final public int getPartitionId() {
        
        return partitionId;
        
    }
    
    /**
     * The arity is determined by the number of terms supplied in the ctor.
     */
    public final int arity() {
        
        return terms.size();
        
    }

    /**
     * Partly specified ctor. No constraint is specified. No expander is 
     * specified.
     * 
     * @param relationName
     * @param predicateName
     * @param terms
     */
    public MagicPredicate(String relationName, IVariableOrConstant<Long>... terms) {

        this(new String[] { relationName }, -1/* partitionId */,
                null/* constraint */, null/* expander */, terms);

    }

    /**
     * Partly specified ctor. No constraint is specified.
     * 
     * @param relationName
     * @param expander
     *            MAY be <code>null</code>.
     * @param predicateName
     * @param terms
     */
    public MagicPredicate(String relationName, ISolutionExpander expander, 
            IVariableOrConstant<Long>... terms) {

        this(new String[] { relationName }, -1/* partitionId */,
                null/* constraint */, expander,
                terms);

    }

    /**
     * Fully specified ctor.
     * 
     * @param relationName
     * @param partitionId
     * @param constraint
     *            MAY be <code>null</code>.
     * @param expander
     *            MAY be <code>null</code>.
     * @param predicateName
     * @param terms
     */
    public MagicPredicate(String[] relationName, //
            final int partitionId, //
            IElementFilter constraint,//
            ISolutionExpander expander,//
            IVariableOrConstant<Long>... terms//
            ) {
        
        if (relationName == null)
            throw new IllegalArgumentException();
       
        for (int i = 0; i < relationName.length; i++) {
            
            if (relationName[i] == null)
                throw new IllegalArgumentException();
            
        }
        
        if (relationName.length == 0)
            throw new IllegalArgumentException();
        
        if (partitionId < -1)
            throw new IllegalArgumentException();
        
        this.relationName = relationName;
        
        this.partitionId = partitionId;
        
        this.constraint = constraint; /// MAY be null.
        
        this.expander = expander;// MAY be null.
        
        this.terms = Arrays.asList(terms);
        
    }

    /**
     * Copy constructor overrides the relation name(s).
     * 
     * @param relationName
     *            The new relation name(s).
     */
    protected MagicPredicate(MagicPredicate src, String[] relationName) {
        
        if (relationName == null)
            throw new IllegalArgumentException();
       
        for(int i=0; i<relationName.length; i++) {
            
            if (relationName[i] == null)
                throw new IllegalArgumentException();
            
        }
        
        if (relationName.length == 0)
            throw new IllegalArgumentException();
 
        this.partitionId = src.partitionId;
        
        this.relationName = relationName; // override.
     
        this.constraint = src.constraint;
        
        this.expander = src.expander;
        
        this.terms = new LinkedList<IVariableOrConstant<Long>>();
        
        this.terms.addAll(src.terms);
        
    }

    /**
     * Copy constructor sets the index partition identifier.
     * 
     * @param partitionId
     *            The index partition identifier.
     *            
     * @throws IllegalArgumentException
     *             if the index partition identified is a negative integer.
     * @throws IllegalStateException
     *             if the index partition identifier was already specified.
     */
    protected MagicPredicate(final MagicPredicate src, final int partitionId) {

        //@todo uncomment the other half of this test to make it less paranoid.
        if (src.partitionId != -1 ) {//&& this.partitionId != partitionId) {
            
            throw new IllegalStateException();

        }

        if (partitionId < 0) {

            throw new IllegalArgumentException();

        }

        this.relationName = src.relationName;
        
        this.partitionId = partitionId;
        
        this.constraint = src.constraint;
        
        this.expander = src.expander;
        
        this.terms = new LinkedList<IVariableOrConstant<Long>>();
        
        this.terms.addAll(src.terms);
        
    }

    public final IVariableOrConstant<Long> get(int index) {
        
        return terms.get(index);
        
    }
    
    /**
     * Return true iff all terms of the predicate are bound (vs
     * variables).
     */
    final public boolean isFullyBound() {

        for (IVariableOrConstant<Long> term : terms) {

            if (term.isVar()) {
                
                return false;
                
            }
            
        }
        
        return true;

    }

    /**
     * The #of arguments in the predicate that are variables (vs constants) (the
     * context position is NOT counted).
     */
    final public int getVariableCount() {
        
        int i = 0;
        
        for (IVariableOrConstant<Long> term : terms) {

            if (term.isVar()) {
                
                i++;
                
            }
            
        }
        
        return i;
        
    }
    
    public MagicPredicate asBound(IBindingSet bindingSet) {
        
        final IVariableOrConstant<Long>[] newTerms =
            new IVariableOrConstant[this.terms.size()];

        int i = 0;
        for (IVariableOrConstant<Long> term : this.terms) {
            IVariableOrConstant<Long> newTerm;
            if (term.isVar() && bindingSet.isBound((IVariable<Long>) term)) {
                newTerm = bindingSet.get((IVariable<Long>) term);
            } else {
                newTerm = term;
            }
            newTerms[i++] = newTerm;
        }
        
        return new MagicPredicate(relationName, partitionId, 
                constraint, expander, newTerms);
        
    }

    public MagicPredicate setRelationName(String[] relationName) {

        return new MagicPredicate(this, relationName);
        
    }
    
    public MagicPredicate setPartitionId(int partitionId) {

        return new MagicPredicate(this, partitionId);

    }
    
    public IMagicTuple toMagicTuple() {
        
        final long[] terms = new long[this.terms.size()];
        int i = 0;
        for (IVariableOrConstant<Long> term : this.terms) {
            if (term.isVar()) {
                throw new RuntimeException("predicate not fully bound");
            } else {
                terms[i++] = term.get();
            }
        }
        
        return new MagicTuple(terms);
        
    }

    public String toString() {

        return toString(null);
        
    }

    public String toString(IBindingSet bindingSet) {

        StringBuilder sb = new StringBuilder();

        sb.append("(");

        sb.append(Arrays.toString(relationName));

        sb.append(", ");
        
        for (IVariableOrConstant<Long> term : terms) {
            
            sb.append(term.isConstant() || bindingSet == null
                    || !bindingSet.isBound((IVariable) term) ? term.toString()
                    : bindingSet.get((IVariable) term));

            sb.append(", ");
            
        }

        if (terms.size() > 0) {
            
            sb.setLength(sb.length()-2);
            
        }
        
        sb.append("))");

        if (constraint != null || expander != null
                || partitionId != -1) {
            
            /*
             * Something special, so do all this stuff.
             */
            
            boolean first = true;
            
            sb.append("[");
            
            if(constraint!=null) {
                if(!first) sb.append(", ");
                sb.append(constraint.toString());
                first = false;
            }
            
            if(expander!=null) {
                if(!first) sb.append(", ");
                sb.append(expander.toString());
                first = false;
            }
            
            if(partitionId!=-1) {
                if(!first) sb.append(", ");
                sb.append("partitionId="+partitionId);
                first = false;
            }
            
            sb.append("]");
            
        }
        
        return sb.toString();

    }

    final public boolean isOptional() {
        
        return false;
        
    }
    
    final public IElementFilter getConstraint() {

        return constraint;
        
    }

    final public ISolutionExpander getSolutionExpander() {
        
        return expander;
        
    }

    public boolean equals(Object other) {
        
        if (this == other)
            return true;

        final IPredicate o = (IPredicate)other;
        
        final int arity = arity();
        
        if(arity != o.arity()) return false;
        
        for(int i=0; i<arity; i++) {
            
            final IVariableOrConstant x = get(i);
            
            final IVariableOrConstant y = o.get(i);
            
            if (x != y && !(x.equals(y))) {
                
                return false;
            
            }
            
        }
        
        return true;
        
    }

    public int hashCode() {
        
        int h = hash;

        if (h == 0) {

            final int n = arity();

            for (int i = 0; i < n; i++) {
        
                h = 31 * h + get(i).hashCode();
                
            }
            
            hash = h;
            
        }
        
        return h;

    }

    /**
     * Caches the hash code.
     */
    private int hash = 0;

}
