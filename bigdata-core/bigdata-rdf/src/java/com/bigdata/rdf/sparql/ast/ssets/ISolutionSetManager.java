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
 * Created on Mar 26, 2012
 */

package com.bigdata.rdf.sparql.ast.ssets;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.spo.ISPO;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A management interface for named solution sets.
 * <p>
 * Note: This is an internal interface that may evolve substantially.
 * 
 * @see <a
 *      href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=SPARQL_Update">
 *      SPARQL Update </a>
 *      
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/531"> SPARQL
 *      UPDATE for NAMED SOLUTION SETS </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISolutionSetManager {

    /**
     * One time initialization.
     */
    void init();
    
    /**
     * Shutdown.
     */
    void close();

    /**
     * Clear the specified named solution set.
     * 
     * @param solutionSet
     *            The name of the solution set.
     * 
     * @return <code>true</code> iff a solution set by that name existed and was
     *         cleared.
     */
    boolean clearSolutions(String solutionSet);

    /**
     * Clear all named solution sets.
     */
    void clearAllSolutions();

    /**
     * Create a named solution set.
     * 
     * @param solutionSet
     *            The name of the solution set.
     * @param params
     *            The configuration parameters (optional).
     * 
     * @throws RuntimeException
     *             if a solution set exists for that name.
     */
    void createSolutions(String solutionSet, ISPO[] params);

    /**
     * Save the solutions into a named solution set. If there is an existing
     * solution set having that name, then its solutions will be replaced by the
     * new solutions (replace not append).
     * 
     * @param solutionSet
     *            The name of the solution set.
     * @param src
     *            The solutions.
     */
	void putSolutions(String solutionSet, ICloseableIterator<IBindingSet[]> src);

    /**
     * Read the solutions from a named solution set.
     * 
     * @param solutionSet
     *            The name of the solution set.
     * 
     * @return An iterator from which the solutions may be drained.
     * 
     * @throws IllegalStateException
     *             if no solution set with that name exists.
     */
	ICloseableIterator<IBindingSet[]> getSolutions(String solutionSet);

    /**
	 * Return computed statistics for a named solution set.
	 * 
	 * @param solutionSet
	 *            The name of the solution set.
	 *            
	 * @return The statistics -or- <code>null</code> if there is no such named
	 *         solution set.
	 */
	ISolutionSetStats getSolutionSetStats(String solutionSet);

    /**
     * Return <code>true</code> iff a named solution set exists.
     * 
     * @param solutionSet
     *            The name of the solution set.
     *            
     * @return <code>true</code> iff a solution set having that name exists.
     */
    boolean existsSolutions(String solutionSet);
    
}
