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
 * Created on Mar 3, 2012
 */

package com.bigdata.rdf.sail.sparql.service;

import com.bigdata.bop.IBindingSet;

/**
 * Interface for objects which can construct a valid SPARQL query for execution
 * against a SPARQL end point and which can interpret the solutions flowing back
 * from that end point.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRemoteSparqlQueryBuilder {

    /**
     * Return the SPARQL query that will be sent to the remote SPARQL end point.
     *            
     * @return The query.
     */
    String getSparqlQuery();
 
    /**
     * If necessary, decorrelate or filter the solutions from the service. For
     * example, this may be used to "reverse" the UNION pattern in which the
     * variables were rewritten. If the query was produced using the simplest
     * transform (no correlated blank nodes in the solutions and the end point
     * supports BINDINGS) then just return the argument.
     * 
     * @return The solutions.
     */
    IBindingSet[] getSolutions(final IBindingSet[] serviceSolutions);
    
}
