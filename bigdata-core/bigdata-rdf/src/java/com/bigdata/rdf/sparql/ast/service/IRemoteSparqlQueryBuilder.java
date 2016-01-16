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
 * Created on Mar 3, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

import org.openrdf.query.BindingSet;

/**
 * Interface for objects which can construct a valid SPARQL query for execution
 * against a remote SPARQL end point.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: IRemoteSparqlQueryBuilder.java 6071 2012-03-04 18:08:57Z
 *          thompsonbry $
 */
public interface IRemoteSparqlQueryBuilder {

//    /**
//     * When <code>true</code>, the implementation will process all source
//     * solutions. Otherwise, it must be invoked for each source solution in
//     * turn.
//     * 
//     * @return
//     */
//    boolean isVectored();

    /**
     * Return the SPARQL query that will be sent to the remote SPARQL end point.
     * 
     * @param bindingSets
     *            The solutions to flow into the remote SPARQL end point.
     * 
     * @return The query.
     */
    String getSparqlQuery(final BindingSet[] bindingSets);
 
    // /**
    // * If necessary, decorrelate, filter, or otherwise translate the solutions
    // from the service. For
    // * example, this may be used to "reverse" the UNION pattern in which the
    // * variables were rewritten. If the query was produced using the simplest
    // * transform (no correlated blank nodes in the solutions and the end point
    // * supports BINDINGS) then just return the argument.
    // *
    // * @param bindingSets
    // * The solutions received from the remote SPARQL end point.
    // *
    // * @return The solutions.
    // */
    // BindingSet[] getSolutions(final BindingSet[] serviceSolutions);

}
