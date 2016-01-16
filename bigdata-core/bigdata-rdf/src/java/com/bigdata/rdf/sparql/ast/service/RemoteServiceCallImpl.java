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
 * Created on Mar 1, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

import java.util.UUID;

import org.eclipse.jetty.client.HttpClient;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.rdf.sail.Sesame2BigdataIterator;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * This class handles vectored remote service invocation by generating an
 * appropriate SPARQL query (with BINDINGS) and an appropriate HTTP request. The
 * behavior of this class may be configured in the {@link ServiceRegistry} by
 * adjusting the {@link RemoteServiceOptions} for the service URI.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class RemoteServiceCallImpl implements RemoteServiceCall {

//    private static final Logger log = Logger
//            .getLogger(RemoteServiceCallImpl.class);
    
    private final ServiceCallCreateParams params;

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{params=" + params);
        sb.append("}");
        return sb.toString();
        
    }
    
    public RemoteServiceCallImpl(final ServiceCallCreateParams params) {

        if (params == null)
            throw new IllegalArgumentException();

        this.params = params;

    }
    
    @Override
    public RemoteServiceOptions getServiceOptions() {
        
        return (RemoteServiceOptions) params.getServiceOptions();
        
    }

    @Override
    public ICloseableIterator<BindingSet> call(final BindingSet[] bindingSets)
            throws Exception {

        final String uriStr = params.getServiceURI().stringValue();
        
        final RemoteServiceOptions serviceOptions = getServiceOptions();

        final ConnectOptions o = new ConnectOptions(uriStr);

        {

            final String acceptHeader = serviceOptions.getAcceptHeader();

            if (acceptHeader != null) {

                o.setAcceptHeader(acceptHeader);
                
            } else {
                
                o.setAcceptHeader(ConnectOptions.DEFAULT_SOLUTIONS_ACCEPT_HEADER);

            }
            
        }
        
        o.method = serviceOptions.isGET() ? "GET" : "POST";
        
        /*
         * Note: This uses a factory pattern to handle each of the possible ways
         * in which we have to vector solutions to the service end point.
         */
        final IRemoteSparqlQueryBuilder queryBuilder = RemoteSparqlBuilderFactory
                .get(serviceOptions, params.getServiceNode(), bindingSets);
        
        final String queryStr = queryBuilder.getSparqlQuery(bindingSets);
        
        final UUID queryId = UUID.randomUUID();
        
        o.addRequestParam("query", queryStr);
        
        o.addRequestParam("queryId", queryId.toString());
        
        final HttpClient client = params.getClientConnectionManager();
        
       /*
        * Note: While this constructs one instance per remote SERVICE call, the
        * instance uses the existing HTTPClient and Executor and is basically
        * flyweight as a result.
        */
        final RemoteRepositoryManager repo = new RemoteRepositoryManager(//
                uriStr,//
                params.getServiceOptions().isBigdataLBS(),// useLBS
                client,
                params.getTripleStore().getExecutorService()
                );
        
        /*
         * Note: This does not stream chunks back. The ServiceCallJoin currently
         * materializes all solutions from the service in a single chunk, so
         * there is no point doing something incremental here unless it is
         * coordinated with the ServiceCallJoin.
         */

        final TupleQueryResult queryResult;

        try {

            queryResult = repo.tupleResults(o, queryId, null);
            
        } finally {

            repo.close();
            
        }

        return new Sesame2BigdataIterator<BindingSet, QueryEvaluationException>(
                        queryResult);

    }

}
