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
/*
 * Created on Aug 25, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.IPassesMaterialization;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;

/**
 * Computed {@link INeedsMaterialization} metadata for an
 * {@link IValueExpression}.
 * 
 * TODO This should also reason about datatype constraints on variables. If we
 * know that a variable is constrained in a given scope to only take on a data
 * type which is associated with an {@link FullyInlineTypedLiteralIV} or a
 * specific numeric data type, then some operators may be able to operate
 * directly on that {@link IV}. This is especially interesting for aggregates.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ComputedMaterializationRequirement.java 5179 2011-09-12
 *          20:13:25Z thompsonbry $
 */
public class ComputedMaterializationRequirement implements
        INeedsMaterialization, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final Requirement requirement;

    private final Set<IVariable<IV>> varsToMaterialize;

    public String toString() {

        return "{requirement=" + requirement + ", vars=" + varsToMaterialize
                + "}";

    }
    
    public ComputedMaterializationRequirement(final Requirement requirement,
            final Set<IVariable<IV>> varsToMaterialize) {
    
        this.requirement = requirement;
        
        this.varsToMaterialize = varsToMaterialize;
    }

    public ComputedMaterializationRequirement(final IValueExpression<?> ve) {

//        if (ve instanceof IVariable<?>) {
//
//            /*
//             * A single variable evaluated in a FILTER is handled by computing
//             * its expected boolean value. We need to materialize the variable
//             * to do that.
//             */
//            
//            varsToMaterialize = Collections.singleton((IVariable<IV>) ve);
//
//            requirement = Requirement.ALWAYS;
//            
//        } else {
//
//        /*
//         * Something more complicated.
//         */
        
        varsToMaterialize = new LinkedHashSet<IVariable<IV>>();

        requirement = gatherVarsToMaterialize(ve, varsToMaterialize);

//        }

    }

    public Requirement getRequirement() {
        
        return requirement;
        
    }

    public Set<IVariable<IV>> getVarsToMaterialize() {
        
        return varsToMaterialize;

    }

    public static Set<IVariable<IV>> getVarsFromArguments(final BOp c) {

        final int arity = c.arity();
        
        final Set<IVariable<IV>> terms = new LinkedHashSet<IVariable<IV>>(arity);

        for (int i = 0; i < arity; i++) {

            final BOp arg = c.get(i);

            if (arg != null) {

                if (arg instanceof IValueExpression
                        && arg instanceof IPassesMaterialization) {

                    terms.addAll(getVarsFromArguments(arg));

                } else if (arg instanceof IVariable) {

                    terms.add((IVariable) arg);

                }

            }

        }

        return terms;

    }

    /**
     * Static helper used to determine materialization requirements.
     */
    static INeedsMaterialization.Requirement gatherVarsToMaterialize(
            final IValueExpression<?> c, final Set<IVariable<IV>> terms) {

        boolean materialize = false;
        boolean always = false;
        
        final Iterator<BOp> it = BOpUtility.preOrderIterator(c);
        
        while (it.hasNext()) {
            
            final BOp bop = it.next();
            
            if (bop instanceof INeedsMaterialization) {
                
                final INeedsMaterialization bop2 = (INeedsMaterialization) bop;
                
                final Set<IVariable<IV>> t = getVarsFromArguments(bop);
                
                if (t.size() > 0) {
                    
                    terms.addAll(t);
                    
                    materialize = true;
                    
                    // if any bops have terms that always needs materialization
                    // then mark the whole constraint as such
                    if (bop2.getRequirement() == Requirement.ALWAYS) {
                        
                        always = true;
                        
                    }
                    
                }
                
            }

        }

        return materialize ? (always ? Requirement.ALWAYS
                : Requirement.SOMETIMES) : Requirement.NEVER;

    }
    
    public boolean equals(final Object o) {
        
        if (this == o)
            return true;
        
        if (!(o instanceof ComputedMaterializationRequirement))
            return false;
        
        final ComputedMaterializationRequirement t = (ComputedMaterializationRequirement) o;
        
        if (requirement != t.requirement)
            return false;
        
        if (!varsToMaterialize.equals(t.varsToMaterialize))
            return false;
        
        return true;
    
    }
    
}
