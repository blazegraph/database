/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Sep 9, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.lexicon.IFulltextSearch.SolrSearchQuery;
import com.bigdata.rdf.lexicon.SolrFulltextSearchImpl;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.service.BigdataNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.FTS;
import com.bigdata.search.IHit;
import com.bigdata.search.IFulltextSearchHit;
import com.bigdata.search.FulltextSearchHit;
import com.bigdata.search.FulltextSearchHiterator;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A factory for an external fulltext search services (such as Solr).
 * It accepts a group consisting of external Solr search magic predicates. 
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class FulltextSearchServiceFactory implements ServiceFactory {

    private static final Logger log = Logger
            .getLogger(FulltextSearchServiceFactory.class);

    /*
     * Note: This could extend the base class to allow for search service
     * configuration options.
     */
    private final BigdataNativeServiceOptions serviceOptions;

    public FulltextSearchServiceFactory() {
        
        serviceOptions = new BigdataNativeServiceOptions();
        
        serviceOptions.setRunLast(true); // override default
    }
    
    @Override
    public BigdataNativeServiceOptions getServiceOptions() {

        return serviceOptions;
        
    }
    
    public BigdataServiceCall create(final ServiceCallCreateParams params) {

        if (params == null)
            throw new IllegalArgumentException();

        final AbstractTripleStore store = params.getTripleStore();

        if (store == null)
            throw new IllegalArgumentException();

        final ServiceNode serviceNode = params.getServiceNode();

        if (serviceNode == null)
            throw new IllegalArgumentException();

        /*
         * Validate the search predicates for a given search variable.
         */
        
        final Map<IVariable<?>, Map<URI, StatementPatternNode>> map = verifyGraphPattern(
                store, serviceNode.getGraphPattern());

        if (map == null)
            throw new RuntimeException("Not a search request.");

        if (map.size() != 1)
            throw new RuntimeException(
                    "Multiple search requests may not be combined.");

        final Map.Entry<IVariable<?>, Map<URI, StatementPatternNode>> e = map
                .entrySet().iterator().next();
        
        final IVariable<?> searchVar = e.getKey();
        
        final Map<URI, StatementPatternNode> statementPatterns = e.getValue();
        
        validateSearch(searchVar, statementPatterns);

        /*
         * Create and return the ServiceCall object which will execute this
         * query.
         */

        return new ExternalServiceSearchCall(store, searchVar, statementPatterns,
                getServiceOptions());
        
    }

    /**
     * Validate the search request. This looks for external search magic
     * predicates and returns them all. It is an error if anything else is
     * found in the group. All such search patterns are reported back by this
     * method, but the service can only be invoked for one a single search
     * variable at a time. The caller will detect both the absence of any search
     * and the presence of more than one search and throw an exception.
     */
    private Map<IVariable<?>, Map<URI, StatementPatternNode>> verifyGraphPattern(
            final AbstractTripleStore database,
            final GroupNodeBase<IGroupMemberNode> group) {

        // lazily allocate iff we find some search predicates in this group.
        Map<IVariable<?>, Map<URI, StatementPatternNode>> tmp = null;

        final int arity = group.arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = group.get(i);

            if (child instanceof GroupNodeBase<?>) {

                throw new RuntimeException("Nested groups are not allowed.");

            }

            if (child instanceof StatementPatternNode) {

                final StatementPatternNode sp = (StatementPatternNode) child;

                final TermNode p = sp.p();

                if (!p.isConstant())
                    throw new RuntimeException("Expecting search predicate: "
                            + sp);

                final URI uri = (URI) ((ConstantNode) p).getValue();

                if (!uri.stringValue().startsWith(FTS.NAMESPACE))
                    throw new RuntimeException("Expecting search predicate: "
                            + sp);

                /*
                 * Some search predicate.
                 */

                if (!ASTFulltextSearchOptimizer.searchUris.contains(uri))
                    throw new RuntimeException("Unknown search predicate: "
                            + uri);

                final TermNode s = sp.s();

                if (!s.isVariable())
                    throw new RuntimeException(
                            "Subject of search predicate is constant: " + sp);

                final IVariable<?> searchVar = ((VarNode) s)
                        .getValueExpression();

                // Lazily allocate map.
                if (tmp == null) {

                    tmp = new LinkedHashMap<IVariable<?>, Map<URI, StatementPatternNode>>();
                    
                }

                // Lazily allocate set for that searchVar.
                Map<URI, StatementPatternNode> statementPatterns = tmp
                        .get(searchVar);
                
                if (statementPatterns == null) {

                    tmp.put(searchVar,
                            statementPatterns = new LinkedHashMap<URI, StatementPatternNode>());
                    
                }

                // Add search predicate to set for that searchVar.
                statementPatterns.put(uri, sp);

            }

        }
        
        return tmp;

    }

    /**
     * Validate the search. There must be exactly one {@link BD#SEARCH}
     * predicate. There should not be duplicates of any of the search predicates
     * for a given searchVar.
     */
    private void validateSearch(final IVariable<?> searchVar,
            final Map<URI, StatementPatternNode> statementPatterns) {

        final Set<URI> uris = new LinkedHashSet<URI>();

        for(StatementPatternNode sp : statementPatterns.values()) {
        
            final URI uri = (URI)(sp.p()).getValue();
            
            if (!uris.add(uri))
                throw new RuntimeException(
                        "Search predicate appears multiple times for same search variable: predicate="
                                + uri + ", searchVar=" + searchVar);

            // all input variables must have been bound and point to literals
            if (uri.equals(FTS.SEARCH) ||
                uri.equals(FTS.ENDPOINT) ||
                uri.equals(FTS.PARAMS) ||
                uri.equals(FTS.TARGET_TYPE) ||
                uri.equals(FTS.TIMEOUT)) {
               
                assertObjectIsLiteralOrVariable(sp);

            // all output variables must not have been bound and point to vars
            } else if (uri.equals(FTS.SCORE) ||
                        uri.equals(FTS.SNIPPET)) {
                
                assertObjectIsVariable(sp);
                
            } else {

                throw new AssertionError("Unverified search predicate: " + sp);
                
            }
            
        }
        
        if (!uris.contains(FTS.SEARCH)) {
            throw new RuntimeException("Required search predicate not found: "
                    + FTS.SEARCH + " for searchVar=" + searchVar);
        }
        
    }
    
    private void assertObjectIsLiteralOrVariable(final StatementPatternNode sp) {

        final TermNode o = sp.o();

        boolean isNotLiterale =
            !o.isConstant()
                || !(((ConstantNode) o).getValue() instanceof Literal);
        boolean isNotVariable = !o.isVariable();
        
        if (isNotLiterale && isNotVariable)
        {

            throw new IllegalArgumentException(
                "Object is not literal or variable: " + sp);
            
        }

    }

    private void assertObjectIsVariable(final StatementPatternNode sp) {

        final TermNode o = sp.o();

        if (!o.isVariable()) {

            throw new IllegalArgumentException("Object must be variable: " + sp);
            
        }

    }

    /**
     * 
     * Note: This has the {@link AbstractTripleStore} reference attached. This
     * is not a {@link Serializable} object. It MUST run on the query
     * controller.
     */
    private static class ExternalServiceSearchCall implements BigdataServiceCall {

        private final AbstractTripleStore store;
        private final IServiceOptions serviceOptions;
        private final Literal query;
        private final Literal endpoint;
        private final Literal endpointType;
        private final Literal params;
        private final Literal fieldType;
        private final Literal searchTimeosut;
        
        private final IVariable<?>[] vars;
        
        public ExternalServiceSearchCall(
                final AbstractTripleStore store,
                final IVariable<?> searchVar,
                final Map<URI, StatementPatternNode> statementPatterns,
                final IServiceOptions serviceOptions) {

            if(store == null)
                throw new IllegalArgumentException();

            if(searchVar == null)
                throw new IllegalArgumentException();

            if(statementPatterns == null)
                throw new IllegalArgumentException();

            if(serviceOptions == null)
                throw new IllegalArgumentException();

            this.store = store;
            
            this.serviceOptions = serviceOptions;
            
            /*
             * Unpack the "search" magic predicate:
             * 
             * [?searchVar solr:search objValue]
             */
            final StatementPatternNode sp =
               statementPatterns.get(FTS.SEARCH);

            query = (Literal) sp.o().getValue();
            
            /*
             * Unpack the search service request parameters.
             */

            Literal endpoint = null;
            Literal endpointType = null;
            Literal params = null;
            Literal fieldType = null;
            Literal searchTimeosut = null;
            IVariable<?> score = null;
            IVariable<?> snippet = null;

            for (StatementPatternNode meta : statementPatterns.values()) {

                final URI p = (URI) meta.p().getValue();

                final Literal oVal = meta.o().isConstant() ? (Literal) meta.o()
                        .getValue() : null;

                final IVariable<?> oVar = meta.o().isVariable() ? (IVariable<?>) meta
                        .o().getValueExpression() : null;

                if (FTS.ENDPOINT.equals(p)) {
                   endpoint = oVal;
                } else if (FTS.ENDPOINT_TYPE.equals(p)) {
                      endpointType = oVal;
                } else if (FTS.PARAMS.equals(p)) {
                    params = oVal;
                } else if (FTS.TARGET_TYPE.equals(p)) {
                    fieldType = oVal;
                } else if (FTS.TIMEOUT.equals(p)) {
                    searchTimeosut = oVal;
                } else if (FTS.SCORE.equals(p)) {
                    score = oVar;
                } else if (FTS.SNIPPET.equals(p)) {
                    snippet = oVar;
                } 
            }

            this.vars = new IVariable[] {//
                  searchVar,//
	               score == null ? Var.var() : score,// must be non-null.
	               snippet == null ? Var.var() : snippet // must be non-null.
	         };

            this.endpoint = endpoint;
            this.endpointType = endpointType;
            this.params = params;
            this.fieldType = fieldType;
            this.searchTimeosut = searchTimeosut;

        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        private FulltextSearchHiterator<IFulltextSearchHit<?>> 
           getSolrSearchResultIterator(IBindingSet[] bindingsClause) {

           
           /*
           final ITextIndexer<IHit> textIndex = (ITextIndexer) 
	    				store.getLexiconRelation().getSearchEngine();
        	
            if (textIndex == null)
                throw new UnsupportedOperationException("No free text index?");

            String s = query.getLabel();
            final boolean prefixMatch;
            if (s.indexOf('*') >= 0) {
                prefixMatch = true;
                s = s.replaceAll("\\*", "");
            } else {
                prefixMatch = false;
            }
            */
           // TODO: merge in bindings and pass in variables (rather than
           // hardcoded strings below)
            
            SolrFulltextSearchImpl si = new SolrFulltextSearchImpl();
            SolrSearchQuery sq = new SolrSearchQuery("query", "params", "endpoint");
            return (FulltextSearchHiterator) si.search(sq);
        
        }

        /**
         * {@inheritDoc}
         * 
         * FIXME The bindingsClause is ignored. If someone were to bind the
         * subject, rank, or relevance variables then we would to notice that
         * here.  We would also have to produce one solution for each binding
         * set input to the service.
         */
        @Override
        public ICloseableIterator<IBindingSet> call(
                final IBindingSet[] bindingsClause) {


           /*
            if (bindingsClause.length > 1) {


                throw new UnsupportedOperationException();

            }

            if (bindingsClause.length == 1 && !bindingsClause[0].isEmpty()) {


//                throw new UnsupportedOperationException();

            }
            */
         	return new HitConverter(getSolrSearchResultIterator(bindingsClause));

        }
            
        /**
         * Converts {@link FulltextSearchHit}s into {@link IBindingSet}
         */
        private class HitConverter implements ICloseableIterator<IBindingSet> {
            
            private final FulltextSearchHiterator<IFulltextSearchHit<?>> src;

            private IFulltextSearchHit<?> current = null;
            private boolean open = true;
            
            public HitConverter(final FulltextSearchHiterator<IFulltextSearchHit<?>> src) {
                
                this.src = src;
                
            }
            
            public void close() {
                if (open) {
                    open = false;
                }
            }

            public boolean hasNext() {
                if (!open)
                    return false;
                if (current != null)
                    return true;
                while (src.hasNext()) {
                    current = src.next();
                    return true;
                }
                return current != null;
            }

            public IBindingSet next() {

                if (!hasNext())
                    throw new NoSuchElementException();
                
                final IFulltextSearchHit<?> tmp = current;
                
                current = null;
                
                return newBindingSet(tmp);
                
            }

            /**
             * Convert an {@link IHit} into an {@link IBindingSet}.
             */
            // TODO: verify if this is inline with the variable order,
            // do we also need to pass in the URI nodes?!
            private IBindingSet newBindingSet(final IFulltextSearchHit<?> hit) {
                
               // TODO: replace dummy.ns
                BigdataValueFactory vf = 
                   BigdataValueFactoryImpl.getInstance(
                      store.getLexiconRelation().getNamespace());
                
                BigdataLiteral litSnippet = vf.createLiteral(hit.getSnippet());

                BigdataLiteral uriRes = vf.createLiteral(hit.getRes());

                @SuppressWarnings({ "unchecked", "rawtypes" })
                final IConstant<?>[] vals = 
                   new IConstant[] {
                      new Constant(new Constant(DummyConstantNode.toDummyIV((BigdataValue)uriRes))),  // uriIv
                      new Constant(new XSDNumericIV(hit.getScore())), 
                      new Constant(new Constant(DummyConstantNode.toDummyIV((BigdataValue)litSnippet)))                      
                };
            
                final ListBindingSet bs = new ListBindingSet(vars, vals);
                
                if (log.isTraceEnabled()) {
                	log.trace(bs);
                	log.trace(query.getClass());
                	log.trace(((BigdataLiteral) query).getIV());
                	log.trace(((BigdataLiteral) query).getIV().getClass());
                }
                
                return bs;
                
            }

            public void remove() {
                
                throw new UnsupportedOperationException();
                
            }

        } // class HitConverter

        @Override
        public IServiceOptions getServiceOptions() {
            
            return serviceOptions;
            
        }
        
    }

}
