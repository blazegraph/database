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
 * Created on April 15, 2012
 */
package com.bigdata.rdf.sparql.ast;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IVCache;

/**
 * Class models the compiled statistics based on the observed solutions.
 */
public class CompiledSolutionSetStats implements ISolutionSetStats,
		Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The #of solutions.
	 */
	private final long nsolutions;

	/**
	 * The set of variables observed across all solutions.
	 */
	private final Set<IVariable<?>> usedVars;

	/**
	 * The set of variables which are bound in ALL solutions.
	 */
	private final Set<IVariable<?>> alwaysBound;

	/**
	 * The set of variables which are NOT bound in at least one solution (e.g.,
	 * MAYBE bound semantics).
	 */
	private final Set<IVariable<?>> notAlwaysBound;

	/**
	 * The set of variables whose bound value has its {@link IVCache}
	 * association set whenever the variable is bound in a solution.
	 */
	private final Set<IVariable<?>> materialized;
	
    /**
     * The set of variables which are effective constants (they are bound in
     * every solution and always to the same value) together with their constant
     * bindings.
     */
    private final Map<IVariable<?>,IConstant<?>> constants;
    
    /**
	 * Constructor exposes unmodifable versions of its arguments.
	 * 
	 * @param nsolutions
	 *            The #of solutions.
	 * @param usedVars
	 *            The set of variables observed across all solutions.
	 * @param alwaysBound
	 *            The set of variables which are bound in ALL solutions.
	 * @param notAlwaysBound
	 *            The set of variables which are NOT bound in at least one
	 *            solution (e.g., MAYBE bound semantics).
	 * @param materialized
	 *            The set of variables whose bound value has its {@link IVCache}
	 *            association set in all solutions in which that variable is
	 *            bound.
	 * @param constants
	 *            The set of variables which are effective constants (they are
	 *            bound in every solution and always to the same value) together
	 *            with their constant bindings.
	 */
	public CompiledSolutionSetStats(final long nsolutions,
			final Set<IVariable<?>> usedVars,
			final Set<IVariable<?>> alwaysBound,
			final Set<IVariable<?>> notAlwaysBound,
			final Set<IVariable<?>> materialized,
			final Map<IVariable<?>, IConstant<?>> constants) {

		this.nsolutions = nsolutions;
		
		// Expose unmodifiable versions of these collections.
		this.usedVars = Collections.unmodifiableSet(usedVars);
		this.alwaysBound = Collections.unmodifiableSet(alwaysBound);
		this.notAlwaysBound = Collections.unmodifiableSet(notAlwaysBound);
		this.materialized = Collections.unmodifiableSet(materialized);
		this.constants = Collections.unmodifiableMap(constants);

	}

	@Override
	public long getSolutionSetSize() {
		return nsolutions;
	}

	@Override
	public Set<IVariable<?>> getUsedVars() {
		return usedVars;
	}

	@Override
	public Set<IVariable<?>> getAlwaysBound() {
		return alwaysBound;
	}

	@Override
	public Set<IVariable<?>> getNotAlwaysBound() {
		return notAlwaysBound;
	}

	@Override
	public Set<IVariable<?>> getMaterialized() {
		return materialized;
	}
	
	@Override
	public Map<IVariable<?>, IConstant<?>> getConstants() {

		return constants;

	}

	@Override
	public String toString() {

		final StringBuilder sb = new StringBuilder();

		sb.append(super.toString());
		sb.append("{size=" + nsolutions);
		sb.append(",usedVars=" + usedVars);
		sb.append(",alwaysBound=" + alwaysBound);
		sb.append(",notAlwaysBound=" + notAlwaysBound);
		sb.append(",materialized=" + materialized);
		sb.append(",constants=" + constants);
		sb.append("}");

		return sb.toString();
		
	}
	
}
