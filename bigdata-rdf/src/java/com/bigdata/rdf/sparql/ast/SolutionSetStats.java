/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Feb 29, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.impl.TermId;

/**
 * A set of interesting statistics on a solution set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SolutionSetStats implements ISolutionStats {

    /**
     * The #of solutions.
     */
    private final int nsolutions;
    
    /**
     * The set of variables observed across all solutions.
     */
    private final Set<IVariable<?>> usedVars;

    /**
     * The set of variables which are NOT bound in at least one solution (e.g.,
     * MAYBE bound semantics).
     */
    private final Set<IVariable<?>> notAlwaysBound;

    /**
     * The set of variables which are bound in ALL solutions.
     */
    private final Set<IVariable<?>> alwaysBound;

    /**
     * 
     * TODO Compute the distinct values for each variable for which we have a
     * binding, or at least the #of such values and offer a method to obtain the
     * distinct values which could cache them. We can do the exact #of distinct
     * values trivially for small solution sets. For very large solution sets
     * this is more expensive and approximate techniques for obtaining the
     * distinct set only when it is likely to be small would be appropriate.
     * <p>
     * Note that this must correctly handle {@link TermId#mockIV(VTE)}s.
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/490"> Mock
     *      IV / TermId hashCode()/equals() problems</a>
     */
    public SolutionSetStats(final Iterator<IBindingSet> bindingSets) {

        if (bindingSets == null)
            throw new IllegalArgumentException();
        
        int nsolutions = 0;
        
        this.usedVars = new HashSet<IVariable<?>>();
        
        this.notAlwaysBound = new HashSet<IVariable<?>>();

        final Set<IVariable<?>> currentVars = new HashSet<IVariable<?>>();
        final Set<IVariable<?>> notBoundThisSolution = new HashSet<IVariable<?>>();

        while (bindingSets.hasNext()) {
            
            final IBindingSet bset = bindingSets.next();
            
            if(bset == null)
                throw new IllegalArgumentException();
            
            nsolutions++;
            
            // Collect all variables used in this solution.
            currentVars.clear();
            {

                @SuppressWarnings("rawtypes")
                final Iterator<IVariable> vitr = bset.vars();
                
                while (vitr.hasNext()) {

                    final IVariable<?> v = vitr.next();

                    if (usedVars.add(v) && nsolutions > 1) {
                        
                        /*
                         * This variable was not used in solutions prior to this 
                         * one.
                         */
                    
                        notAlwaysBound.add(v);
                        
                    }

                    currentVars.add(v);

                }
                
            }
            
            /*
             * Figure out which observed variables were not bound in this
             * solution and add them to the set of variables which are not
             * always bound.
             */
            notBoundThisSolution.clear();
            notBoundThisSolution.addAll(usedVars);
            notBoundThisSolution.removeAll(currentVars);
            notAlwaysBound.addAll(notBoundThisSolution);
            
        }

        // #of solutions counted.
        this.nsolutions = nsolutions;
        
        // Figure out which variables were bound in every solution.
        this.alwaysBound = new HashSet<IVariable<?>>(usedVars);
        this.alwaysBound.removeAll(notAlwaysBound);
        
    }
    
    public final int getSolutionSetSize() {

        return nsolutions;
        
    }
    
    public final Set<IVariable<?>> getUsedVars() {
        
        return usedVars;
        
    }

    public final Set<IVariable<?>> getAlwaysBound() {
        
        return alwaysBound;
        
    }

    public final Set<IVariable<?>> getNotAlwaysBound() {
        
        return notAlwaysBound;
        
    }

}
