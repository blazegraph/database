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
 * Created on Mar 27, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Set;

import com.bigdata.bop.IVariable;

/**
 * Interface and annotations for AST nodes which have a {@link ProjectionNode}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IProjectionDecl {

    public interface Annotations {
        
        /**
         * The {@link ProjectionNode} (optional). This is also used for DESCRIBE
         * queries to capture the list of variables and IRIs which are then used
         * to rewrite the DESCRIBE query into what amounts to a CONSTRUCT query.
         * The resulting CONSTRUCT query will have a different
         * {@link ProjectionNode} suitable for use with the generated
         * {@link ConstructNode}.
         */
        String PROJECTION = "projection";

    }

    /**
     * Set or clear the projection.
     * 
     * @param projection
     *            The projection (may be <code>null</code>).
     */
    void setProjection(final ProjectionNode projection);

    /**
     * Return the projection -or- <code>null</code> if there is no projection.
     */
    ProjectionNode getProjection();

    /**
     * Return the set of variables projected out of this query (this is a NOP if
     * there is no {@link ProjectionNode} for the query, which can happen for an
     * ASK query). This DOES NOT report the variables which are used by the
     * SELECT expressions, just the variables which are PROJECTED out by the
     * BINDs.
     * 
     * @param vars
     *            The projected variables are added to this set.
     * 
     * @return The caller's set.
     */
    Set<IVariable<?>> getProjectedVars(final Set<IVariable<?>> vars);

}
