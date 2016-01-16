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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Class populates an {@link ISolutionSetStats} object from a stream of
 * solutions. The summary is available from {@link #getStats()} once the source
 * solutions have been fully consumed.
 * 
 * TODO Compute the distinct values for each variable for which we have a
 * binding, or at least the #of such values and offer a method to obtain the
 * distinct values which could cache them. We can do the exact #of distinct
 * values trivially for small solution sets. For very large solution sets this
 * is more expensive and approximate techniques for obtaining the distinct set
 * only when it is likely to be small would be appropriate.
 * <p>
 * Note that this must correctly handle {@link TermId#mockIV(VTE)}s.
 * <p>
 * Or compute a bloom filter for a statistical summary.
 * 
 * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/490"> Mock IV /
 *      TermId hashCode()/equals() problems</a>
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
	
	/**
	 * The set of variables whose {@link IVCache} association is NOT set is at
	 * least one solution in which the variable is bound.
	 */
	protected final Set<IVariable<?>> notMaterialized = new HashSet<IVariable<?>>();
	
	/**
	 * A map from the variable to the first bound value for that variable. This
	 * is used to identify variables which are effective constants (they are
	 * bound in the first solution and in each solution thereafter and always to
	 * the same value).
	 */
    protected final Map<IVariable<?>, IConstant<?>> firstBoundValue = new HashMap<IVariable<?>, IConstant<?>>();

    /**
	 * The set of variables which have been proven to not be effective
	 * constants. In order to be an effective constant, the variable must be
	 * bound in all solutions and it must be bound to the same value in each
	 * solution.
	 */
    protected final Set<IVariable<?>> notConstant = new HashSet<IVariable<?>>();
	
	protected final Set<IVariable<?>> currentVars = new HashSet<IVariable<?>>();
	protected final Set<IVariable<?>> notBoundThisSolution = new HashSet<IVariable<?>>();

	/**
	 * Convenience method.
	 * 
	 * @param bindingSets
	 *            The source solutions.
	 *            
	 * @return The computed statistics.
	 */
	static public ISolutionSetStats get(final IBindingSet[] bindingSets) {

		final SolutionSetStatserator itr = new SolutionSetStatserator(
				BOpUtility.asIterator(bindingSets));

		try {

			while (itr.hasNext()) {

				itr.next();

			}

			return itr.getStats();

		} finally {
			
			itr.close();
			
		}

	}

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

					// Look for the bound value for this variable.
					@SuppressWarnings("unchecked")
					final IConstant<IV<?, ?>> c = bset.get(v);

					if(nsolutions == 1) {
						
						/*
						 * Record the binding for each variable in the first
						 * solution. This is used to identify variables which
						 * are effective constants (they are bound to the same
						 * value in all solutions).
						 */

						firstBoundValue.put(v, c);

					} else {

						/*
						 * Look at the first bound value for this variable. If
						 * it was not bound or if the variable was not bound to
						 * the same constant, then this variable is not an
						 * effective constant for this set of solutions.
						 */						
						if (!notConstant.contains(v)) {

							final IConstant<?> c2 = firstBoundValue.get(v);

							if (c2 == null || !c2.equals(c)) {

								// Proven not a constant.
								notConstant.add(v);

							}

						}

					}

					/*
					 * Check for a variable which has a bound value but the
					 * bound value is not materialized.
					 */
					if (!notMaterialized.contains(v)) {


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
			 * always bound. We also do this for possible constants.
			 */
			notBoundThisSolution.clear();
			notBoundThisSolution.addAll(usedVars);
			notBoundThisSolution.removeAll(currentVars);
			notAlwaysBound.addAll(notBoundThisSolution);
			notConstant.addAll(notBoundThisSolution);

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

		/*
		 * Figure out which variables were always materialized when they were
		 * bound.
		 */
		final Set<IVariable<?>> materialized = new HashSet<IVariable<?>>(
				usedVars);
		
		materialized.removeAll(notMaterialized);

		/*
		 * Figure out which variables were effective constants. We start with
		 * the bindings for the first solution and then remove any entry where
		 * we have proven that the variable is not always bound to the same
		 * constant.
		 */
		final Map<IVariable<?>, IConstant<?>> constants = new HashMap<IVariable<?>, IConstant<?>>(
				firstBoundValue);
		
		for (IVariable<?> v : notConstant) {
		
			constants.remove(v);
			
		}
				
		// Expose immutable versions of these collections.
		return new CompiledSolutionSetStats(//
				nsolutions,//
				usedVars,//
				alwaysBound,//
				notAlwaysBound,//
				materialized,//
				constants//
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
