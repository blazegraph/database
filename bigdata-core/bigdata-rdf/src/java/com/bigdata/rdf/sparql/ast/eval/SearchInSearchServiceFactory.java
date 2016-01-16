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
 * Created on Sep 9, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.btree.IIndex;
import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.ITextIndexer;
import com.bigdata.rdf.lexicon.ITextIndexer.FullTextQuery;
import com.bigdata.rdf.sparql.ast.ConstantNode;
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
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.BDS;
import com.bigdata.relation.accesspath.EmptyCloseableIterator;
import com.bigdata.relation.accesspath.ThickCloseableIterator;
import com.bigdata.search.Hiterator;
import com.bigdata.search.IHit;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A factory for a "search in search" service. 
 * It accepts a group that have a single triple pattern in it:
 * 
 * service bd:searchInSearch {
 *   ?s bd:searchInSearch "search" .
 * }
 * 
 * This service will then use the full text index to filter out incoming
 * bindings for ?s that do not link to a Literal that is found via the full
 * text index with the supplied search string.  If there are no incoming
 * bindings (or none that have ?s bound), this service will produce no output.
 */
public class SearchInSearchServiceFactory extends AbstractServiceFactoryBase {

    private static final Logger log = Logger
            .getLogger(SearchInSearchServiceFactory.class);

    /*
     * Note: This could extend the base class to allow for search service
     * configuration options.
     */
    private final BigdataNativeServiceOptions serviceOptions;

    public SearchInSearchServiceFactory() {
        
        serviceOptions = new BigdataNativeServiceOptions();
        
//        serviceOptions.setRunFirst(true);
        
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

        return new SearchCall(store, searchVar, statementPatterns,
                getServiceOptions());
        
    }

    /**
     * Validate the search request. This looks for search magic predicates and
     * returns them all. It is an error if anything else is found in the group.
     * All such search patterns are reported back by this method, but the
     * service can only be invoked for one a single search variable at a time.
     * The caller will detect both the absence of any search and the presence of
     * more than one search and throw an exception.
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

                if (!uri.stringValue().startsWith(BDS.NAMESPACE))
                    throw new RuntimeException("Expecting search predicate: "
                            + sp);

                /*
                 * Some search predicate.
                 */

                if (!ASTSearchOptimizer.searchUris.contains(uri) &&
                    !BDS.SEARCH_IN_SEARCH.equals(uri)) {
                    throw new RuntimeException("Unknown search predicate: "
                            + uri);
                }

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

            if (uri.equals(BDS.SEARCH_IN_SEARCH)) {

                assertObjectIsLiteral(sp);

            } else if (uri.equals(BDS.RELEVANCE) || uri.equals(BDS.RANK)) {
                
                assertObjectIsVariable(sp);
                
            } else if(uri.equals(BDS.MIN_RANK) || uri.equals(BDS.MAX_RANK)) {

                assertObjectIsLiteral(sp);

            } else if (uri.equals(BDS.MIN_RELEVANCE) || uri.equals(BDS.MAX_RELEVANCE)) {

                assertObjectIsLiteral(sp);

            } else if(uri.equals(BDS.MATCH_ALL_TERMS)) {
                
                assertObjectIsLiteral(sp);
                
            } else if(uri.equals(BDS.MATCH_EXACT)) {
                
                assertObjectIsLiteral(sp);
                
            } else if(uri.equals(BDS.SEARCH_TIMEOUT)) {
                
                assertObjectIsLiteral(sp);
                
            } else if(uri.equals(BDS.MATCH_REGEX)) {
                
            	// a variable for the object is equivalent to regex = null
//                assertObjectIsLiteral(sp);
                
            } else {

                throw new AssertionError("Unverified search predicate: " + sp);
                
            }
            
        }
        
        if (!uris.contains(BDS.SEARCH_IN_SEARCH)) {
            throw new RuntimeException("Required search predicate not found: "
                    + BDS.SUBJECT_SEARCH + " for searchVar=" + searchVar);
        }
        
    }
    
    private void assertObjectIsLiteral(final StatementPatternNode sp) {

        final TermNode o = sp.o();

        if (!o.isConstant()
                || !(((ConstantNode) o).getValue() instanceof Literal)) {

            throw new IllegalArgumentException("Object is not literal: " + sp);
            
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
    private static class SearchCall implements BigdataServiceCall {

        private final AbstractTripleStore store;
        private final IIndex osp;
        private final IServiceOptions serviceOptions;
        private final Literal query;
        private final IVariable<?>[] vars;
        private final Literal minRank;
        private final Literal maxRank;
        private final Literal minRelevance;
        private final Literal maxRelevance;
        private final boolean matchAllTerms;
        private final boolean matchExact;
        private final Literal searchTimeout;
        private final Literal matchRegex;
        
        public SearchCall(
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
            this.osp = store.getSPORelation().getIndex(SPOKeyOrder.OSP);
            
            this.serviceOptions = serviceOptions;
            
            /*
             * Unpack the "search" magic predicate:
             * 
             * [?searchVar bd:search objValue]
             */
            final StatementPatternNode sp = statementPatterns.get(BDS.SEARCH_IN_SEARCH);

            query = (Literal) sp.o().getValue();

            /*
             * Unpack the search service request parameters.
             */

            IVariable<?> relVar = null;
            IVariable<?> rankVar = null;
            Literal minRank = null;
            Literal maxRank = null;
            Literal minRelevance = null;
            Literal maxRelevance = null;
            boolean matchAllTerms = false;
            boolean matchExact = false;
            Literal searchTimeout = null;
            Literal matchRegex = null;

            for (StatementPatternNode meta : statementPatterns.values()) {

                final URI p = (URI) meta.p().getValue();

                final Literal oVal = meta.o().isConstant() ? (Literal) meta.o()
                        .getValue() : null;

                final IVariable<?> oVar = meta.o().isVariable() ? (IVariable<?>) meta
                        .o().getValueExpression() : null;

                if (BDS.RELEVANCE.equals(p)) {
                    relVar = oVar;
                } else if (BDS.RANK.equals(p)) {
                    rankVar = oVar;
                } else if (BDS.MIN_RANK.equals(p)) {
                    minRank = (Literal) oVal;
                } else if (BDS.MAX_RANK.equals(p)) {
                    maxRank = (Literal) oVal;
                } else if (BDS.MIN_RELEVANCE.equals(p)) {
                    minRelevance = (Literal) oVal;
                } else if (BDS.MAX_RELEVANCE.equals(p)) {
                    maxRelevance = (Literal) oVal;
                } else if (BDS.MATCH_ALL_TERMS.equals(p)) {
                    matchAllTerms = ((Literal) oVal).booleanValue();
                } else if (BDS.MATCH_EXACT.equals(p)) {
                    matchExact = ((Literal) oVal).booleanValue();
                } else if (BDS.SEARCH_TIMEOUT.equals(p)) {
                    searchTimeout = (Literal) oVal;
                } else if (BDS.MATCH_REGEX.equals(p)) {
                    matchRegex = (Literal) oVal;
                }
            }

            this.vars = new IVariable[] {//
                    searchVar,//
                    relVar == null ? Var.var() : relVar,// must be non-null.
                    rankVar == null ? Var.var() : rankVar // must be non-null.
            };

            this.minRank = minRank;
            this.maxRank = maxRank;
            this.minRelevance = minRelevance;
            this.maxRelevance = maxRelevance;
            this.matchAllTerms = matchAllTerms;
            this.matchExact = matchExact;
            this.searchTimeout = searchTimeout;
            this.matchRegex = matchRegex;

        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        private Hiterator<IHit<?>> getHiterator() {

//            final IValueCentricTextIndexer<IHit> textIndex = (IValueCentricTextIndexer) store
//                    .getLexiconRelation().getSearchEngine();
            
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

            return (Hiterator) textIndex.search(new FullTextQuery(
        		s,//
                query.getLanguage(),// 
                prefixMatch,//
                matchRegex == null ? null : matchRegex.stringValue(),
                matchAllTerms,
                matchExact,
                minRelevance == null ? BDS.DEFAULT_MIN_RELEVANCE : minRelevance.doubleValue()/* minCosine */, 
                maxRelevance == null ? BDS.DEFAULT_MAX_RELEVANCE : maxRelevance.doubleValue()/* maxCosine */, 
                minRank == null ? BDS.DEFAULT_MIN_RANK/*1*/ : minRank.intValue()/* minRank */,
                maxRank == null ? BDS.DEFAULT_MAX_RANK/*Integer.MAX_VALUE*/ : maxRank.intValue()/* maxRank */,
                searchTimeout == null ? BDS.DEFAULT_TIMEOUT/*0L*/ : searchTimeout.longValue()/* timeout */,
                TimeUnit.MILLISECONDS
                ));
        
        }

        private static final ConcurrentWeakValueCacheWithTimeout<String, Set<IV>> cache =
        		new ConcurrentWeakValueCacheWithTimeout<String, Set<IV>>(
        				10, 1000*60);
        		
        private Set<IV> getSubjects() {
        	
        	final String s = query.getLabel();
        	
        	if (cache.containsKey(s)) {
        		
        		return cache.get(s);
        		
        	}
        	
        	if (log.isInfoEnabled()) {
        		log.info("entering full text search...");
        	}
        	
        	// query the full text index
            final Hiterator<IHit<?>> src = getHiterator();
            
        	if (log.isInfoEnabled()) {
        		log.info("done with full text search.");
        	}
            
        	if (log.isInfoEnabled()) {
        		log.info("starting subject collection...");
        	}
            
        	final Set<IV> subjects = new LinkedHashSet<IV>();
        	
            while (src.hasNext()) {
            	
            	final IV o = (IV) src.next().getDocId();
            	
            	final Iterator<ISPO> it = 
            			store.getAccessPath((IV)null, (IV)null, o).iterator();
            	
            	while (it.hasNext()) {
            		
            		subjects.add(it.next().s());
            		
            	}

            }
        	
        	if (log.isInfoEnabled()) {
        		log.info("done with subject collection: " + subjects.size());
        	}
        	
        	cache.put(s, subjects);
        	
        	return subjects;
            
        }
        
        /**
         * {@inheritDoc}
         * 
         * Iterate the incoming binding set.  If it does not contain a binding
         * for the searchVar, prune it.  Then iterate the full text search
         * results.  For each result (O binding), test the incoming binding sets
         * to see if there is a link between the binding for the searchVar and
         * the O.  If there is, add the binding set to the output and remove it
         * from the set to be tested against subsequent O bindings.
         */
        @Override
        public ICloseableIterator<IBindingSet> call(
                final IBindingSet[] bindingsClause) {

        	if (log.isInfoEnabled()) {
        		log.info(bindingsClause.length);
        		log.info(Arrays.toString(bindingsClause));
        	}
        	
        	final IVariable<?> searchVar = vars[0];
        	
//        	final IBindingSet[] tmp = new IBindingSet[bindingsClause.length];
//        	System.arraycopy(bindingsClause, 0, tmp, 0, bindingsClause.length);

//        	final boolean[] tmp = new boolean[bindingsClause.length];
        	
        	boolean foundOne = false;
        	
        	/*
			 * We are filtering out incoming binding sets that don't have a
			 * binding for the search var
			 */
        	for (int i = 0; i < bindingsClause.length; i++) {
        		
        		final IBindingSet bs = bindingsClause[i];
        		
        		if (bs.isBound(searchVar)) {
        			
        			// we need to test this binding set
//        			tmp[i] = true;
        			
        			// we have at least one binding set to test
        			foundOne = true;
        			
        		}
        			
        	}
        	
        	// filtered everything out
        	if (!foundOne) {
        		
        		return new EmptyCloseableIterator<IBindingSet>();
        		
        	}
        	
        	final IBindingSet[] out = new IBindingSet[bindingsClause.length];
        	
        	int numAccepted = 0;
        	
//        	if (log.isInfoEnabled()) {
//        		log.info("entering full text search...");
//        	}
//        	
//        	// query the full text index
//            final Hiterator<IHit<?>> src = getHiterator();
//            
//        	if (log.isInfoEnabled()) {
//        		log.info("done with full text search.");
//        	}
//            
//            while (src.hasNext()) {
//            	
//            	final IV o = (IV) src.next().getDocId();
//            	
//            	for (int i = 0; i < bindingsClause.length; i++) {
//            		
//            		/*
//					 * The binding set has either been filtered out or already
//					 * accepted.
//					 */
//            		if (!tmp[i])
//            			continue;
//            		
//            		/*
//            		 * We know it's bound.  If it weren't it would have been
//            		 * filtered out above.
//            		 */
//            		final IV s = (IV) bindingsClause[i].get(searchVar).get();
//            		
//            		final IKeyBuilder kb = KeyBuilder.newInstance();
//            		o.encode(kb);
//            		s.encode(kb);
//            		
//            		final byte[] fromKey = kb.getKey();
//            		final byte[] toKey = SuccessorUtil.successor(fromKey.clone());
//            		
//                	if (log.isInfoEnabled()) {
//                		log.info("starting range count...");
//                	}
//                	
//                	final long rangeCount = osp.rangeCount(fromKey, toKey);
//                	
//                	if (log.isInfoEnabled()) {
//                		log.info("done with range count: " + rangeCount);
//                	}
//                	
//            		/*
//            		 * Test the OSP index to see if we have a link.
//            		 */
////            		if (!store.getAccessPath(s, null, o).isEmpty()) {
//            		if (rangeCount > 0) {
//            			
//            			// add the binding set to the output
//            			out[numAccepted++] = bindingsClause[i];
//            			
//            			// don't need to test this binding set again
//            			tmp[i] = false;
//            			
//            		}
//            		
//            	}
//            	
//            }
            
        	final Set<IV> subjects = getSubjects();
        	
        	for (int i = 0; i < bindingsClause.length; i++) {
        		
        		/*
        		 * We know it's bound.  If it weren't it would have been
        		 * filtered out above.
        		 */
        		final IV s = (IV) bindingsClause[i].get(searchVar).get();
        		
        		if (subjects.contains(s)) {
        		
        			// add the binding set to the output
        			out[numAccepted++] = bindingsClause[i];
        			
        		}

        	}
            
        	if (log.isInfoEnabled()) {
        		log.info("finished search in search.");
        	}
            
            return new ThickCloseableIterator<IBindingSet>(out, numAccepted); 

        }
            
        @Override
        public IServiceOptions getServiceOptions() {
            
            return serviceOptions;
            
        }
        
    }

}
