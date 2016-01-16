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

import java.util.Map;
import java.util.Set;

import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IVCache;

/**
 * A set of interesting statistics on a solution set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISolutionSetStats {

    /**
     * Return the #of solutions.
     */
    long getSolutionSetSize();

    /**
     * Return the set of variables which in at least one solution.
     */
    Set<IVariable<?>> getUsedVars();

    /**
     * Return the subset of the variables which are bound in all solutions.
     */
    Set<IVariable<?>> getAlwaysBound();

    /**
     * Return the subset of the variables which are NOT bound in all solutions.
     */
    Set<IVariable<?>> getNotAlwaysBound();

    /**
	 * Return the subset of the variables which are materialized in all
	 * solutions in which they appear (the variables do not have to be bound in
	 * every solution, but if they are bound then their {@link IVCache}
	 * association is always set).
	 */
    Set<IVariable<?>> getMaterialized();
    
    /**
	 * The set of variables which are effective constants (they are bound in
	 * every solution and always to the same value) together with their constant
	 * bindings.
	 * 
	 * @return The set of variables which are effective constants.
	 */
    Map<IVariable<?>, IConstant<?>> getConstants();

}
