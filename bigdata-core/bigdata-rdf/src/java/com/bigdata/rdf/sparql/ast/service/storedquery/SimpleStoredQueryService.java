/*

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
package com.bigdata.rdf.sparql.ast.service.storedquery;

import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;

/**
 * Simple stored query consisting of a parameterized SPARQL query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
abstract public class SimpleStoredQueryService extends StoredQueryService {

    /**
     * Return the SPARQL query to be evaluated.
     */
    abstract protected String getQuery(
            final ServiceCallCreateParams createParams,
            final ServiceParams serviceParams);

    /**
     * Executes the SPARQL query returned by
     * {@link #getQuery(ServiceCallCreateParams, ServiceParams)}
     */
    @Override
    protected TupleQueryResult doQuery(
            final BigdataSailRepositoryConnection cxn,
            final ServiceCallCreateParams createParams,
            final ServiceParams serviceParams) throws Exception {

        final String queryStr = getQuery(createParams, serviceParams);

        final String baseURI = createParams.getServiceURI().stringValue();

        final BigdataSailTupleQuery query = (BigdataSailTupleQuery) cxn
                .prepareTupleQuery(QueryLanguage.SPARQL, queryStr, baseURI);

        return query.evaluate();

    }

} // SimpleStoredQueryService
