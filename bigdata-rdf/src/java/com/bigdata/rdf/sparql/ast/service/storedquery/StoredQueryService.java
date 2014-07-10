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
package com.bigdata.rdf.sparql.ast.service.storedquery;

import java.util.Arrays;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sail.Sesame2BigdataIterator;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.bigdata.rdf.sparql.ast.service.BigdataNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ExternalServiceCall;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.task.AbstractApiTask;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A SERVICE that exposes a stored query for execution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/989">Stored Query Service</a>
 * 
 *      TODO Wiki page.
 * 
 *      TODO Implicit prefix declaration for bsq.
 * 
 * TODO Reconcile with the REST API (group commit task pattern). 
 * 
 *      TODO Why does this work?
 * 
 *      <pre>
 * SELECT ?book ?title ?price
 * {
 *    SERVICE <http://www.bigdata.com/rdf/stored-query#test_stored_query_001> {
 *    }
 * }
 * </pre>
 * 
 *      while this does not work
 * 
 *      <pre>
 * PREFIX bsq:  <http://www.bigdata.com/rdf/stored-query#>
 * 
 * SELECT ?book ?title ?price
 * {
 *    SERVICE <bsq#test_stored_query_001> {
 *    }
 * }
 * </pre>
 * 
 *      TODO Example
 * 
 *      <pre>
 * PREFIX bsq: <http://www.bigdata.com/rdf/stored-query#>
 * #...
 * SERVICE &lt;bsq#my-service&gt; {
 *    bsq:queryParam bsq:gasClass "com.bigdata.rdf.graph.analytics.BFS" .
 *    gas:program gas:in &lt;IRI&gt; . # one or more times, specifies the initial frontier.
 *    gas:program gas:out ?out . # exactly once - will be bound to the visited vertices.
 *    gas:program gas:maxIterations 4 . # optional limit on breadth first expansion.
 *    gas:program gas:maxVisited 2000 . # optional limit on the #of visited vertices.
 *    gas:program gas:nthreads 4 . # specify the #of threads to use (optional)
 * }
 * </pre>
 * 
 */
abstract public class StoredQueryService implements ServiceFactory {

    public interface Options {
        
        /**
         * The namespace used for stored query service.
         */
        String NAMESPACE = "http://www.bigdata.com/rdf/stored-query#";

    }

    static private transient final Logger log = Logger
            .getLogger(StoredQueryService.class);

    private final BigdataNativeServiceOptions serviceOptions;

    public StoredQueryService() {

        serviceOptions = new BigdataNativeServiceOptions();
        
//        /*
//         * TODO This should probably be metadata set for each specific
//         * stored query.
//         */
//        serviceOptions.setRunFirst(true);
        
    }

    @Override
    public IServiceOptions getServiceOptions() {

        return serviceOptions;
        
    }

    @Override
    final public ExternalServiceCall create(final ServiceCallCreateParams params) {

        if (params == null)
            throw new IllegalArgumentException();

        final AbstractTripleStore store = params.getTripleStore();

        if (store == null)
            throw new IllegalArgumentException();

        final ServiceNode serviceNode = params.getServiceNode();

        if (serviceNode == null)
            throw new IllegalArgumentException();

        final ServiceParams serviceParams = ServiceParams.gatherServiceParams(params);
        
        return create(params, serviceParams);
        
    }

    public ExternalServiceCall create(
            final ServiceCallCreateParams createParams,
            final ServiceParams serviceParams) {

        /*
         * Create and return the ServiceCall object which will execute this
         * query.
         */

        return new StoredQueryServiceCall(createParams, serviceParams);

    }
    
    /**
     * Return the SPARQL query to be evaluated.
     */
    abstract protected String getQuery();
    
    private class StoredQueryServiceCall implements ExternalServiceCall {

        private final ServiceCallCreateParams createParams;
        private final ServiceParams serviceParams;

        public StoredQueryServiceCall(
                final ServiceCallCreateParams createParams,
                final ServiceParams serviceParams) {

            if (createParams == null)
                throw new IllegalArgumentException();

            if (serviceParams == null)
                throw new IllegalArgumentException();

            this.createParams = createParams;
            this.serviceParams = serviceParams;

        }

        @Override
        public IServiceOptions getServiceOptions() {
            
            return createParams.getServiceOptions();
            
        }

        /**
         * TODO We could use {@link ASTEvalHelper} to evaluate at the bigdata
         * level without forcing the materialization of any variable bindings
         * from the lexicon indices. This would be faster for some purposes,
         * especially if the stored procedure is only used to JOIN into an outer
         * query as in <code>SELECT * { SERVICE bsq:my-service {} }</code>
         * 
         * FIXME Generalize to allow arbitrary application logic that has easy
         * methods permitting it to invoke multiple queries and operate on the
         * results.
         * 
         * FIXME Generalize to support groovy scripting.
         */
        @Override
        public ICloseableIterator<BindingSet> call(final BindingSet[] bindingSets)
                throws Exception {

            if (log.isInfoEnabled()) {
                log.info(bindingSets.length);
                log.info(Arrays.toString(bindingSets));
                log.info(serviceParams);
            }

            final AbstractTripleStore tripleStore = createParams.getTripleStore();
            
            final String queryStr = getQuery();
            
            /*
             * FIXME What about incoming bindings? They need to flow into the
             * SERVICE.
             */
            
            // TODO Should the baseURI be the SERVICE URI?  Decide and document.
            final String baseURI = createParams.getServiceURI().stringValue();
            
            final Future<TupleQueryResult> ft = AbstractApiTask.submitApiTask(
                    tripleStore.getIndexManager(),
                    new SparqlApiTask(tripleStore.getNamespace(), tripleStore
                            .getTimestamp(), queryStr, baseURI, bindingSets));

            try {

                final TupleQueryResult tupleQueryResult = ft.get();
                
                return new Sesame2BigdataIterator<BindingSet, QueryEvaluationException>(
                        tupleQueryResult);
                
            } finally {
            
                ft.cancel(true/* mayInterruptIfRunning */);
                
            }

        }

    } // StoredQueryServiceCall

    /**
     * Task to execute a SPARQL query.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class SparqlApiTask extends
            AbstractApiTask<TupleQueryResult> {

        private final String queryStr;
        private final String baseURI;

        /**
         * 
         * FIXME This is ignoring the exogenous bindings. This is more or less
         * equivalent to bottom-up evaluation. It would be more efficient if we
         * could flow in the exogenous bindings but this is not supported before
         * openrdf 2.7 (we hack this in {@link BigdataSailTupleQuery}).
         */
        private final BindingSet[] bindingSets;

        public SparqlApiTask(final String namespace, final long timestamp,
                final String queryStr, final String baseURI,
                final BindingSet[] bindingSets) {

            super(namespace, timestamp);

            this.queryStr = queryStr;
            this.baseURI = baseURI;
            this.bindingSets = bindingSets;

        }

        @Override
        public TupleQueryResult call() throws Exception {
            BigdataSailRepositoryConnection cxn = null;
            boolean success = false;
            try {
                // Note: Will be UPDATE connection if UPDATE request!!!
                cxn = getQueryConnection();
                if (log.isTraceEnabled())
                    log.trace("Query running...");
                final TupleQueryResult ret = doQuery(cxn);
                success = true;
                if (log.isTraceEnabled())
                    log.trace("Query done.");
                return ret;
            } finally {
                if (cxn != null) {
                    if (!success && !cxn.isReadOnly()) {
                        /*
                         * Force rollback of the connection.
                         * 
                         * Note: It is possible that the commit has already
                         * been processed, in which case this rollback()
                         * will be a NOP. This can happen when there is an
                         * IO error when communicating with the client, but
                         * the database has already gone through a commit.
                         */
                        try {
                            // Force rollback of the connection.
                            cxn.rollback();
                        } catch (Throwable t) {
                            log.error(t, t);
                        }
                    }
                    try {
                        // Force close of the connection.
                        cxn.close();
                    } catch (Throwable t) {
                        log.error(t, t);
                    }
                }
            }
        }

        protected TupleQueryResult doQuery(
                final BigdataSailRepositoryConnection cxn) throws Exception {

            final BigdataSailTupleQuery query = (BigdataSailTupleQuery) cxn
                    .prepareTupleQuery(QueryLanguage.SPARQL, queryStr,
                            baseURI);

            return query.evaluate();

        }

    } // SparqlApiTask

}
