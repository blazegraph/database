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
 * Created on Jan 2, 2011
 */

package com.bigdata.rdf.sail;

import com.bigdata.bop.BOp;

/**
 * Query hint directives understood by a bigdata SPARQL end point.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QueryHints {

    /**
     * The namespace prefix used in SPARQL queries to signify query hints. You
     * can embed query hints into a SPARQL query as follows:
     * 
     * <pre>
     * PREFIX BIGDATA_QUERY_HINTS: &lt;http://www.bigdata.com/queryHints#name1=value1&amp;name2=value2&gt;
     * </pre>
     * 
     * where <i>name</i> is the name of a query hint and <i>value</i> is the
     * value associated with that query hint. Multiple query hints can be
     * specified (as shown in this example) using a <code>&amp;</code> character
     * to separate each name=value pair.
     * <p>
     * Query hints are either directives understood by the SPARQL end point or
     * {@link BOp.Annotations}. A list of the known directives is declared by
     * this interface.
     */
    String NAMESPACE = "BIGDATA_QUERY_HINTS";
    
    String PREFIX = "http://www.bigdata.com/queryHints#";

	/**
	 * Specify the query optimizer. For example, you can disable the query
	 * optimizer using
	 * 
	 * <pre>
	 * PREFIX BIGDATA_QUERY_HINTS: &lt;http://www.bigdata.com/queryHints#com.bigdata.rdf.sail.QueryHints.optimizer=None&gt;
	 * </pre>
	 * 
	 * Disabling the query optimizer can be useful if you have a query for which
	 * the static query optimizer is producing a inefficient join ordering. With
	 * the query optimizer disabled for that query, the joins will be run in the
	 * order given.  This makes it possible for you to decide on the right join
	 * ordering for that query.
	 * 
	 * @see QueryOptimizerEnum
	 */
    String OPTIMIZER = QueryHints.class.getName() + ".optimizer";

	/**
	 * A label which may be used to tag the instances of some SPARQL query
	 * template in manner which makes sense to the application. The tag is used
	 * to aggregate performance statistics for tagged queries.
	 * 
	 * <pre>
	 * PREFIX BIGDATA_QUERY_HINTS: &lt;http://www.bigdata.com/queryHints#com.bigdata.rdf.sail.QueryHints.tag=Query12&gt;
	 * </pre>
	 * 
	 * @see http://sourceforge.net/apps/trac/bigdata/ticket/207
	 */
    String TAG = QueryHints.class.getName() + ".tag";

}
