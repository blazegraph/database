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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.RangeBOp;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.lexicon.ITextIndexer;
import com.bigdata.rdf.lexicon.ITextIndexer.FullTextQuery;
import com.bigdata.rdf.model.BigdataLiteral;
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
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.BDS;
import com.bigdata.search.Hiterator;
import com.bigdata.search.IHit;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.SingleValueChunkedIterator;

import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * A factory for a search service. It accepts a group consisting of search magic
 * predicates. See {@link BD#SEARCH}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Add support for slicing the join using tools like CUTOFF (to
 *          limit the #of solutions) and specifying a {@link RangeBOp} (to
 *          impose the desired key range). CUTOFF and a single-threaded pipeline
 *          join can guarantee that we report out the top-N subjects in terms of
 *          the hit relevance. {@link RangeBOp} would work on a cluster. None of
 *          this is perfectly satisfactory.
 */
public class SearchServiceFactory extends AbstractServiceFactoryBase {

    private static final Logger log = Logger
            .getLogger(SearchServiceFactory.class);

    /*
     * Note: This could extend the base class to allow for search service
     * configuration options.
     */
    private final BigdataNativeServiceOptions serviceOptions;

    public SearchServiceFactory() {
        
        serviceOptions = new BigdataNativeServiceOptions();
        
        serviceOptions.setRunFirst(true);
        
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

                if (!ASTSearchOptimizer.searchUris.contains(uri))
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

            if (uri.equals(BDS.SEARCH)) {

                assertObjectIsLiteral(sp);

            } else if (uri.equals(BDS.RELEVANCE) || uri.equals(BDS.RANK)) {
                
                assertObjectIsVariable(sp);
                
            } else if(uri.equals(BDS.MIN_RANK)||uri.equals(BDS.MAX_RANK)) {

                assertObjectIsLiteral(sp);

            } else if (uri.equals(BDS.MIN_RELEVANCE) || uri.equals(BDS.MAX_RELEVANCE)) {

                assertObjectIsLiteral(sp);

            } else if(uri.equals(BDS.MATCH_ALL_TERMS)) {
                
                assertObjectIsLiteral(sp);
                
            } else if(uri.equals(BDS.MATCH_EXACT)) {
                
                assertObjectIsLiteral(sp);
                
            } else if(uri.equals(BDS.SUBJECT_SEARCH)) {
                
                assertObjectIsLiteral(sp);
                
            } else if(uri.equals(BDS.SEARCH_TIMEOUT)) {
                
                assertObjectIsLiteral(sp);
                
            } else if (uri.equals(BDS.RANGE_COUNT)) {
                
                assertObjectIsVariable(sp);
                
            } else if(uri.equals(BDS.MATCH_REGEX)) {
                
            	// a variable for the object is equivalent to regex = null
//                assertObjectIsLiteral(sp);
                
            } else {

                throw new AssertionError("Unverified search predicate: " + sp);
                
            }
            
        }
        
        if (!uris.contains(BDS.SEARCH)) {
            throw new RuntimeException("Required search predicate not found: "
                    + BDS.SEARCH + " for searchVar=" + searchVar);
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
        private final IServiceOptions serviceOptions;
        private final Literal query;
        private final IVariable<IV> search;
        private final IVariable<?>[] vars;
        private final Literal minRank;
        private final Literal maxRank;
        private final Literal minRelevance;
        private final Literal maxRelevance;
        private final boolean matchAllTerms;
        private final boolean matchExact;
        /**
		 * @deprecated Feature was never completed due to scalability issues.
		 * See BZLG-1548, BLZG-563.
		 */
        @Deprecated
        private final boolean subjectSearch;
        private final Literal searchTimeout;
        private final Literal matchRegex;
        private final IVariable<?> rangeCountVar;
        
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
            
            this.serviceOptions = serviceOptions;
            
            /*
             * Unpack the "search" magic predicate:
             * 
             * [?searchVar bd:search objValue]
             */
            final StatementPatternNode sp = statementPatterns.get(BDS.SEARCH);

            query = (Literal) sp.o().getValue();
            
            final TermNode searchQuery = sp.o();
            if (searchQuery instanceof ConstantNode) {
            	final ConstantNode cNode = (ConstantNode) searchQuery;
            	final IConstant<IV> c = cNode.getValueExpression();
            	this.search = (IVariable<IV>)
            			c.getProperty(Constant.Annotations.VAR);
            } else {
            	this.search = null;
            }

            /*
             * Unpack the search service request parameters.
             */

            IVariable<?> relVar = null;
            IVariable<?> rankVar = null;
            IVariable<?> rangeCountVar = null;
            Literal minRank = null;
            Literal maxRank = null;
            Literal minRelevance = null;
            Literal maxRelevance = null;
            boolean matchAllTerms = false;
            boolean matchExact = false;
            boolean subjectSearch = false;
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
                } else if (BDS.RANGE_COUNT.equals(p)) {
                    rangeCountVar = oVar;
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
                } else if (BDS.SUBJECT_SEARCH.equals(p)) {
                    subjectSearch = ((Literal) oVal).booleanValue();
                } else if (BDS.SEARCH_TIMEOUT.equals(p)) {
                    searchTimeout = (Literal) oVal;
                } else if (BDS.MATCH_REGEX.equals(p)) {
                    matchRegex = (Literal) oVal;
                }
            }

            if (search == null) {
	            this.vars = new IVariable[] {//
	                    searchVar,//
	                    relVar == null ? Var.var() : relVar,// must be non-null.
	                    rankVar == null ? Var.var() : rankVar // must be non-null.
	            };
            } else {
	            this.vars = new IVariable[] {//
	                    searchVar,//
	                    relVar == null ? Var.var() : relVar,// must be non-null.
	                    rankVar == null ? Var.var() : rankVar, // must be non-null.
	                    search
	            };
            }

            this.minRank = minRank;
            this.maxRank = maxRank;
            this.minRelevance = minRelevance;
            this.maxRelevance = maxRelevance;
            this.matchAllTerms = matchAllTerms;
            this.matchExact = matchExact;
            this.subjectSearch = subjectSearch;
            this.searchTimeout = searchTimeout;
            this.matchRegex = matchRegex;
            this.rangeCountVar = rangeCountVar;

        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        private Hiterator<IHit<?>> getHiterator() {

//            final IValueCentricTextIndexer<IHit> textIndex = (IValueCentricTextIndexer) store
//                    .getLexiconRelation().getSearchEngine();
            
        	final ITextIndexer<IHit> textIndex = (ITextIndexer) 
	    		(this.subjectSearch ?
	    			store.getLexiconRelation().getSubjectCentricSearchEngine() :
	    				store.getLexiconRelation().getSearchEngine());
        	
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

        @SuppressWarnings({ "rawtypes", "unchecked" })
        private int getRangeCount() {

//            final IValueCentricTextIndexer<IHit> textIndex = (IValueCentricTextIndexer) store
//                    .getLexiconRelation().getSearchEngine();
            
        	final ITextIndexer<IHit> textIndex = (ITextIndexer) 
	    		(this.subjectSearch ?
	    			store.getLexiconRelation().getSubjectCentricSearchEngine() :
	    				store.getLexiconRelation().getSearchEngine());
        	
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

            return textIndex.count(new FullTextQuery(
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

        /**
         * {@inheritDoc}
         * 
         * FIXME The bindingsClause is ignored. If someone were to bind the
         * subject, rank, or relevance variables then we would to notice that
         * here.  We would also have to produce one solution for each binding
         * set input to the service.
         */
        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public ICloseableIterator<IBindingSet> call(
                final IBindingSet[] bindingsClause) {

            if (bindingsClause.length > 1) {

                /*
                 * FIXME This case is not supported.  We need to run
                 * the search engine for each of the source solutions.
                 */

                /*
                 * Fixed this to allow an incoming binding stream that does not
                 * include any of the search variables.
                 */
//                throw new UnsupportedOperationException();

                for (IBindingSet bs : bindingsClause) {
                    if (rangeCountVar != null) {
                        if (bs.isBound(rangeCountVar)) {
                            /*
                             * FIXME This case is not supported.  We need to run
                             * the search engine for each of the source solutions.
                             */
                            throw new UnsupportedOperationException();
                        }
                    } else {
                        for (int i = 0; i < vars.length; i++) {
                            if (bs.isBound(vars[i])) {
                                /*
                                 * FIXME This case is not supported.  We need to run
                                 * the search engine for each of the source solutions.
                                 */
                                throw new UnsupportedOperationException();
                            }
                        }
                    }
                }
                
            }

            if (bindingsClause.length == 1 && !bindingsClause[0].isEmpty()) {

                /*
                 * Fixed this by putting the ASTBindingAssigner before the
                 * ASTSearchOptimizer in the DefaultOptimizerList. 
                 */

//                throw new UnsupportedOperationException();

            }
            
            if (rangeCountVar != null) {
            
            	final int i = getRangeCount();
            	
                final ListBindingSet bs = new ListBindingSet(
                        new IVariable[] { rangeCountVar },
                        new IConstant[] { new Constant(new XSDNumericIV(i)) });

                return new SingleValueChunkedIterator<IBindingSet>(bs);
                
            } else {
            
                final Striterator sitr = new Striterator(getHiterator());
                sitr.addFilter(new Resolver() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    protected Object resolve(final Object obj) {
                        return bind((IHit<?>) obj);
                    }
                    
                });
            	
                return new ChunkedWrappedIterator<IBindingSet>(sitr);

            }
            
        }

        /**
         * Convert an {@link IHit} into an {@link IBindingSet}.
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private IBindingSet bind(final IHit<?> hit) {
            
            final IConstant<?>[] vals = search == null ?
                new IConstant[] {
                    new Constant(hit.getDocId()), // searchVar
                    new Constant(new XSDNumericIV(hit.getCosine())), // cosine
                    new Constant(new XSDNumericIV(hit.getRank())) // rank
                } :
                new IConstant[] {
                    new Constant(hit.getDocId()), // searchVar
                    new Constant(new XSDNumericIV(hit.getCosine())), // cosine
                    new Constant(new XSDNumericIV(hit.getRank())), // rank
                    new Constant(((BigdataLiteral) query).getIV())
                };
        
            final IBindingSet bs = new ListBindingSet(vars, vals);
            
            if (log.isTraceEnabled()) {
                log.trace(bs);
                log.trace(query.getClass());
                log.trace(((BigdataLiteral) query).getIV());
                log.trace(((BigdataLiteral) query).getIV().getClass());
            }
            
            return bs;
            
        }

//        private class HitConverter implements ICloseableIterator<IBindingSet> {
//            
//            private final Hiterator<IHit<?>> src;
//
//            private IHit<?> current = null;
//            private boolean open = true;
//            
//            public HitConverter(final Hiterator<IHit<?>> src) {
//                
//                this.src = src;
//                
//            }
//            
//            public void close() {
//                if (open) {
//                    open = false;
//                }
//            }
//
//            public boolean hasNext() {
//                if (!open)
//                    return false;
//                if (current != null)
//                    return true;
//                while (src.hasNext()) {
//                    current = src.next();
//                    return true;
//                }
//                return current != null;
//            }
//
//            public IBindingSet next() {
//
//                if (!hasNext())
//                    throw new NoSuchElementException();
//                
//                final IHit<?> tmp = current;
//                
//                current = null;
//                
//                return newBindingSet(tmp);
//                
//            }
//
//            public void remove() {
//                
//                throw new UnsupportedOperationException();
//                
//            }
//
//        } // class HitConverter

        @Override
        public IServiceOptions getServiceOptions() {
            
            return serviceOptions;
            
        }
        
    }

}
