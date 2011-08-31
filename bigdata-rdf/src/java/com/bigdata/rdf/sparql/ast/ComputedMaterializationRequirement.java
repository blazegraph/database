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

/**
 * Computed {@link INeedsMaterialization} metadata for an
 * {@link IValueExpression}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ComputedMaterializationRequirement implements
        INeedsMaterialization {

    private final INeedsMaterialization.Requirement requirement;

    private final Set<IVariable<IV>> varsToMaterialize;

    public ComputedMaterializationRequirement(IValueExpression ve) {

        varsToMaterialize = new LinkedHashSet<IVariable<IV>>();

        requirement = gatherVarsToMaterialize(ve,
                varsToMaterialize);

    }

    public Requirement getRequirement() {
        
        return requirement;
        
    }

    public Set<IVariable<IV>> getVarsToMaterialize() {
        
        return varsToMaterialize;
        
    }

    public static Set<IVariable<IV>> getVarsFromArguments(BOp c) {
        Set<IVariable<IV>> terms = new LinkedHashSet<IVariable<IV>>(c.arity());
        for (int i = 0; i < c.arity(); i++) {
            BOp arg = c.get(i);
            if (arg != null) {
                if (arg instanceof IValueExpression && arg instanceof IPassesMaterialization) {
                    terms.addAll(getVarsFromArguments( arg));
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
    static INeedsMaterialization.Requirement gatherVarsToMaterialize(final IValueExpression<?> c, final Set<IVariable<IV>> terms) {
     
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

        return materialize ? (always ? Requirement.ALWAYS : Requirement.SOMETIMES) : Requirement.NEVER;

    }
    
}
