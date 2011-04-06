/**
Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Extension API for bigdata queries.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 */
public interface BigdataSailQuery {
    
    /**
     * Returns a copy of the Sesame operator tree that will or would be
     * evaluated by this query.
     */
    TupleExpr getTupleExpr() throws QueryEvaluationException;
    
	/**
	 * Return query hints associated with this query. Query hints are embedded
	 * in query strings as namespaces. See {@link QueryHints#PREFIX} for more
	 * information.
	 */
    Properties getQueryHints(); 
    
}
