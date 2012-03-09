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

import java.util.Map;
import java.util.Set;

import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;

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
    int getSolutionSetSize();

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
     * The set of variables which are effective constants (they are bound in
     * every solution and always to the same value) together with their constant
     * bindings.
     */
    Map<IVariable<?>, IConstant<?>> getConstants();

}
