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
/*
 * Created on Feb 29, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;

/**
 * A set of interesting statistics on a solution set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated by {@link SolutionSetStatserator} since the latter can compute
 *             all of the same information can be used over streaming solution
 *             sets.
 */
public class SolutionSetStats implements ISolutionSetStats {

    /**
     * The #of solutions.
     */
    private final long nsolutions;
    
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
     * The set of variables whose {@link IVCache} association is always
     * present when the variable is bound in a solution.
     */
    private final Set<IVariable<?>> materialized;
    
    /**
     * The set of variables which are effective constants (they are bound in
     * every solution and always to the same value) together with their constant
     * bindings.
     */
    private final Map<IVariable<?>,IConstant<?>> constants;
    
    /**
     * @param bindingSets
     *            The exogenous solutions flowing into the query.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>
     * @throws IllegalArgumentException
     *             if any element of the source solution array is
     *             <code>null</code>
     */
    public SolutionSetStats(final IBindingSet[] bindingSets) {

        if (bindingSets == null)
            throw new IllegalArgumentException();
        
        int nsolutions = 0;
        
        final Set<IVariable<?>> usedVars = new HashSet<IVariable<?>>();
        final Set<IVariable<?>> notAlwaysBound = new HashSet<IVariable<?>>();
        final Set<IVariable<?>> notMaterialized = new HashSet<IVariable<?>>();
        final Set<IVariable<?>> currentVars = new HashSet<IVariable<?>>();
        final Set<IVariable<?>> notBoundThisSolution = new HashSet<IVariable<?>>();

        for(IBindingSet bset : bindingSets) {
            
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

					/*
					 * Check for a variable which has a bound value but the
					 * bound value is not materialized.
					 */
					if (!notMaterialized.contains(v)) {

						@SuppressWarnings("unchecked")
						final IConstant<IV<?, ?>> c = bset.get(v);

						if (c != null) {

							/*
							 * Note: ClassCastException if bound value is not an
							 * IV.
							 */
							final IV<?, ?> iv = c.get();

							if (!iv.hasValue()) {

								notMaterialized.add(v);

							}

						}
					
					}

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
		final Set<IVariable<?>> alwaysBound = new HashSet<IVariable<?>>(
				usedVars);
		alwaysBound.removeAll(notAlwaysBound);

		// Figure out which variables were always materialized when they were
		// bound.
		final Set<IVariable<?>> materialized = new HashSet<IVariable<?>>(
				usedVars);
		materialized.removeAll(notMaterialized);

        // Expose immutable versions of these collections.
        this.usedVars = Collections.unmodifiableSet(usedVars);
        this.alwaysBound = Collections.unmodifiableSet(alwaysBound);
        this.notAlwaysBound = Collections.unmodifiableSet(notAlwaysBound);
        this.materialized = Collections.unmodifiableSet(materialized);
        
        /*
         * Figure out which bindings are the same in every solution. These are
         * effective constants.
         */
        if (alwaysBound.isEmpty()) {
        
            constants = Collections.emptyMap();
            
        } else {
            
            final Map<IVariable<?>, IConstant<?>> constants = new HashMap<IVariable<?>, IConstant<?>>(
                    alwaysBound.size());

            for (IVariable<?> v : alwaysBound) {

                final IConstant<?> firstValue = bindingSets[0].get(v);

                boolean isConstant = true;

                for (int i = 1; i < bindingSets.length; i++) {

                    final IBindingSet bset = bindingSets[i];

                    if (!firstValue.equals(bset.get(v))) {

                        // Proven not a constant.
                        isConstant = false;

                        break;

                    }

                } // next solution

                if(isConstant) {

                    // This variable is always bound to the same value.
                    constants.put(v, firstValue);
                    
                }
                
            } // next always bound variable.
       
            // Expose immutable version of this collection.
            this.constants = Collections.unmodifiableMap(constants);
            
        }

    }

	public final long getSolutionSetSize() {

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

    public final Set<IVariable<?>> getMaterialized() {
    	
    		return materialized;
    	
    }
    
    @Override
    public final Map<IVariable<?>, IConstant<?>> getConstants() {
        
        return constants;
        
    }

}
