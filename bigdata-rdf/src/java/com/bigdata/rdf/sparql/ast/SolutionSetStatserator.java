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
 * Created on April 15, 2012
 */
package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Class populates an {@link ISolutionSetStats} object from a stream of
 * solutions. The summary is available from {@link #getStats()} once the source
 * solutions have been fully consumed.
 */
public class SolutionSetStatserator implements
		ICloseableIterator<IBindingSet[]> {

	private final Iterator<IBindingSet[]> src;
	private boolean open = true;
	private ISolutionSetStats compiledStats;

    /**
     * The #of solutions.
     */
	protected long nsolutions = 0;
    
    /**
     * The set of variables observed across all solutions.
     */
	protected final Set<IVariable<?>> usedVars = new HashSet<IVariable<?>>();

    /**
     * The set of variables which are NOT bound in at least one solution (e.g.,
     * MAYBE bound semantics).
     */
	protected final Set<IVariable<?>> notAlwaysBound = new HashSet<IVariable<?>>();

	protected final Set<IVariable<?>> currentVars = new HashSet<IVariable<?>>();
	protected final Set<IVariable<?>> notBoundThisSolution = new HashSet<IVariable<?>>();

	public SolutionSetStatserator(final Iterator<IBindingSet[]> src) {

		if (src == null)
			throw new IllegalArgumentException();

		this.src = src;

	}

	/**
	 * Compute incremental statistics from an observed chunk of solutions.
	 */
	protected void filter(final IBindingSet[] a) {

		for (IBindingSet bset : a) {

			if (bset == null)
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

	}

	/**
	 * Compile the statistics collected from the observed solutions.
	 * 
	 * @return The compiled statistics.
	 */
	protected ISolutionSetStats compile() {

		// Figure out which variables were bound in every solution.
		final Set<IVariable<?>> alwaysBound = new HashSet<IVariable<?>>(
				usedVars);
		alwaysBound.removeAll(notAlwaysBound);

		// Expose immutable versions of these collections.
		return new CompiledSolutionSetStats(//
				nsolutions,//
				usedVars,//
				alwaysBound,//
				notAlwaysBound//
		);

	}

	/**
	 * Return the compiled statistics.
	 * 
	 * @return The compiled statistics.
	 * 
	 * @throws {@link IllegalStateException} if the statistics have not yet been
	 *         compiled (they are automatically compiled when the source
	 *         iterator has been fully consumed).
	 */
	public ISolutionSetStats getStats() {

		if (compiledStats == null) {

			throw new IllegalStateException();

		}

		return compiledStats;

	}

	/*
	 * Closeable iterator pattern.
	 */

	@Override
	public void close() {
		if (open) {
			open = false;
			if (src instanceof ICloseableIterator) {
				((ICloseableIterator<?>) src).close();
			}
		}
	}

	@Override
	public boolean hasNext() {
		if (open && !src.hasNext()) {
			// Close this iterator.
			close();
			// Compile the statistics and expose via getStats()
			compiledStats = compile();
			return false;
		}
		return open;
	}

	@Override
	public IBindingSet[] next() {
		if (!hasNext())
			throw new NoSuchElementException();
		final IBindingSet[] a = src.next();
		filter(a);
		return a;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}