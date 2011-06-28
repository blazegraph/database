/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jun 20, 2011
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

/**
 * Interface providing access to more state of the original SPARQL query AST.
 * <p>
 * Note: This interface is supported by various overrides of the openrdf SPARQL
 * parser. Those overrides are required in order to gain access to the details
 * of the AST.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BigdataSailQuery
 */
public interface IBigdataParsedQuery {
    
    /**
     * The type of query.
     */
    QueryType getQueryType();
    
    /**
     * Return query hints associated with this query. Query hints are embedded
     * in query strings as namespaces. See {@link QueryHints#PREFIX} for more
     * information.
     */
    Properties getQueryHints(); 
    
}
