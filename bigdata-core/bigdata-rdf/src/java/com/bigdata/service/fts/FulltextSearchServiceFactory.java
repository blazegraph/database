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
package com.bigdata.service.fts;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
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
import com.bigdata.rdf.sparql.ast.eval.ASTFulltextSearchOptimizer;
import com.bigdata.rdf.sparql.ast.eval.AbstractServiceFactoryBase;
import com.bigdata.rdf.sparql.ast.service.BigdataNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.MockIVReturningServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.search.IHit;
import com.bigdata.service.fts.FTS.EndpointType;
import com.bigdata.service.fts.FTS.SearchResultType;
import com.bigdata.service.fts.IFulltextSearch.FulltextSearchQuery;
import com.bigdata.service.fts.impl.SolrFulltextSearchImpl;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A factory for an external fulltext search services (such as Solr). It accepts
 * a group consisting of external Solr search magic predicates.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class FulltextSearchServiceFactory extends AbstractServiceFactoryBase {

   private static final Logger log = Logger
         .getLogger(FulltextSearchServiceFactory.class);

   /*
    * Note: This could extend the base class to allow for search service
    * configuration options.
    */
   private final BigdataNativeServiceOptions serviceOptions;   

   public FulltextSearchServiceFactory() {

      serviceOptions = new BigdataNativeServiceOptions();

      serviceOptions.setRunFirst(true);

   }

   @Override
   public BigdataNativeServiceOptions getServiceOptions() {

      return serviceOptions;

   }

   public MockIVReturningServiceCall create(final ServiceCallCreateParams params) {

      if (params == null)
         throw new IllegalArgumentException();

      final AbstractTripleStore store = params.getTripleStore();
      
      final Properties props =
         store.getIndexManager()!=null && 
         store.getIndexManager() instanceof AbstractJournal  ?
         ((AbstractJournal)store.getIndexManager()).getProperties() : null;
      
      final FulltextSearchDefaults deflts = new FulltextSearchDefaults(props);

      final ServiceNode serviceNode = params.getServiceNode();

      if (serviceNode == null)
         throw new IllegalArgumentException();

      /*
       * Validate the search predicates for a given search variable.
       */

      final Map<IVariable<?>, Map<URI, StatementPatternNode>> map = verifyGraphPattern(
            store, serviceNode.getGraphPattern());

      if (map == null)
         throw new FulltextSearchException("Not a search request.");

      if (map.size() != 1)
         throw new FulltextSearchException(
               "Multiple search requests may not be combined.");

      final Map.Entry<IVariable<?>, Map<URI, StatementPatternNode>> e = map
            .entrySet().iterator().next();

      final IVariable<?> searchVar = e.getKey();

      final Map<URI, StatementPatternNode> statementPatterns = e.getValue();

      validateSearch(searchVar, statementPatterns);

      /*
       * Create and return the ServiceCall object which will execute this query.
       */
      return new FulltextSearchServiceCall(store, searchVar, statementPatterns,
            getServiceOptions(), deflts, params);

   }

   /**
    * Validate the search request. This looks for external search magic
    * predicates and returns them all. It is an error if anything else is found
    * in the group. All such search patterns are reported back by this method,
    * but the service can only be invoked for one a single search variable at a
    * time. The caller will detect both the absence of any search and the
    * presence of more than one search and throw an exception.
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

            throw new FulltextSearchException("Nested groups are not allowed.");

         }

         if (child instanceof StatementPatternNode) {

            final StatementPatternNode sp = (StatementPatternNode) child;

            final TermNode p = sp.p();

            if (!p.isConstant())
               throw new FulltextSearchException("Expecting search predicate: " + sp);

            final URI uri = (URI) ((ConstantNode) p).getValue();

            if (!uri.stringValue().startsWith(FTS.NAMESPACE))
               throw new FulltextSearchException("Expecting search predicate: " + sp);

            /*
             * Some search predicate.
             */

            if (!ASTFulltextSearchOptimizer.searchUris.contains(uri))
               throw new FulltextSearchException("Unknown search predicate: " + uri);

            final TermNode s = sp.s();

            if (!s.isVariable())
               throw new FulltextSearchException(
                     "Subject of search predicate is constant: " + sp);

            final IVariable<?> searchVar = ((VarNode) s).getValueExpression();

            // Lazily allocate map.
            if (tmp == null) {

               tmp = new LinkedHashMap<IVariable<?>, Map<URI, StatementPatternNode>>();

            }

            // Lazily allocate set for that searchVar.
            Map<URI, StatementPatternNode> statementPatterns = tmp
                  .get(searchVar);

            if (statementPatterns == null) {

               tmp.put(
                     searchVar,
                     statementPatterns = new LinkedHashMap<URI, StatementPatternNode>());

            }

            // Add search predicate to set for that searchVar.
            statementPatterns.put(uri, sp);

         }

      }

      return tmp;

   }

   /**
    * Validate the search. There must be exactly one {@link FTS#SEARCH}
    * predicate. There should not be duplicates of any of the search predicates
    * for a given searchVar.
    */
   private void validateSearch(final IVariable<?> searchVar,
         final Map<URI, StatementPatternNode> statementPatterns) {

      final Set<URI> uris = new LinkedHashSet<URI>();

      for (StatementPatternNode sp : statementPatterns.values()) {

         final URI uri = (URI) (sp.p()).getValue();

         if (!uris.add(uri))
            throw new FulltextSearchException(
                  "Search predicate appears multiple times for same search"
                  + "variable: predicate=" + uri + ", searchVar=" + searchVar);

         // all input variables must have been bound and point to literals
         if (uri.equals(FTS.SEARCH) || uri.equals(FTS.ENDPOINT)
               || uri.equals(FTS.ENDPOINT_TYPE) || uri.equals(FTS.PARAMS)
               || uri.equals(FTS.SEARCH_RESULT_TYPE) || uri.equals(FTS.SEARCH_FIELD)
               || uri.equals(FTS.SCORE_FIELD) || uri.equals(FTS.SNIPPET_FIELD)
               || uri.equals(FTS.TIMEOUT)) {

            assertObjectIsLiteralOrVariable(sp);

            // all output variables must not have been bound and point to vars
         } else if (uri.equals(FTS.SCORE) || uri.equals(FTS.SNIPPET)) {

            assertObjectIsVariable(sp);

         } else {

            throw new AssertionError("Unverified search predicate: " + sp);

         }

      }

      if (!uris.contains(FTS.SEARCH)) {
         throw new FulltextSearchException(
               FulltextSearchException.NO_QUERY_SPECIFIED);
      }

   }

   private void assertObjectIsLiteralOrVariable(final StatementPatternNode sp) {

      final TermNode o = sp.o();

      boolean isNotLiterale = !o.isConstant()
            || !(((ConstantNode) o).getValue() instanceof Literal);
      boolean isNotVariable = !o.isVariable();

      if (isNotLiterale && isNotVariable) {

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
    * Note: This has the {@link AbstractTripleStore} reference attached. This is
    * not a {@link Serializable} object. It MUST run on the query controller.
    */
   private static class FulltextSearchServiceCall implements
         MockIVReturningServiceCall {

      private final AbstractTripleStore store;
      private final IServiceOptions serviceOptions;
      private final TermNode query;
      private final TermNode endpoint;
      private final TermNode endpointType;
      private final TermNode params;
      private final TermNode searchResultType;
      private final TermNode searchTimeout;
      private final TermNode searchField;
      private final TermNode scoreField;
      private final TermNode snippetField;
      private final FulltextSearchDefaults defaults;
      private final ServiceCallCreateParams serviceCallParams;

      private final IVariable<IV>[] vars;

      public FulltextSearchServiceCall(final AbstractTripleStore store,
            final IVariable<?> searchVar,
            final Map<URI, StatementPatternNode> statementPatterns,
            final IServiceOptions serviceOptions,
            final FulltextSearchDefaults defaults,
            final ServiceCallCreateParams serviceCallParams) {

         if (store == null)
            throw new IllegalArgumentException();

         if (searchVar == null)
            throw new IllegalArgumentException();

         if (statementPatterns == null)
            throw new IllegalArgumentException();

         if (serviceOptions == null)
            throw new IllegalArgumentException();

         this.store = store;

         this.serviceOptions = serviceOptions;
         
         this.defaults = defaults;

         this.serviceCallParams = serviceCallParams;
         
         /*
          * Unpack the "search" magic predicate: [?searchVar solr:search
          * objValue]
          */
         final StatementPatternNode sp = statementPatterns.get(FTS.SEARCH);
         query = sp.o();

         /*
          * Unpack the search service request parameters.
          */
         IVariable<?> score = null;
         IVariable<?> snippet = null;
         TermNode endpoint = null;
         TermNode endpointType = null;
         TermNode params = null;
         TermNode searchResultType = null;
         TermNode searchTimeout = null;
         TermNode searchField = null;
         TermNode scoreField = null;
         TermNode snippetField = null;

         /*
          * Unpack the remaining magic predicates
          */
         for (StatementPatternNode meta : statementPatterns.values()) {

            final URI p = (URI) meta.p().getValue();

            final IVariable<?> oVar = meta.o().isVariable() ? (IVariable<?>) meta
                  .o().getValueExpression() : null;

            if (FTS.ENDPOINT.equals(p)) {
               endpoint = meta.o();
            } else if (FTS.ENDPOINT_TYPE.equals(p)) {
               endpointType = meta.o();
            } else if (FTS.PARAMS.equals(p)) {
               params = meta.o();
            } else if (FTS.SEARCH_RESULT_TYPE.equals(p)) {
               searchResultType = meta.o();
            } else if (FTS.TIMEOUT.equals(p)) {
               searchTimeout = meta.o();
            } else if (FTS.SEARCH_FIELD.equals(p)) {
               searchField = meta.o();
            } else if (FTS.SNIPPET_FIELD.equals(p)) {
               snippetField = meta.o();
            } else if (FTS.SCORE_FIELD.equals(p)) {
               scoreField = meta.o();
            } else if (FTS.SCORE.equals(p)) {
               score = oVar;
            } else if (FTS.SNIPPET.equals(p)) {
               snippet = oVar;
            }
            
         }

         final Var<?> dummyScoreVar = Var.var();
         final Var<?> dummySnippetVar = Var.var();
         dummyScoreVar.setAnonymous(true);
         dummySnippetVar.setAnonymous(true);

         this.vars = new IVariable[] {//
         searchVar,//
               score == null ? dummyScoreVar : score,// must be non-null.
               snippet == null ? dummySnippetVar : snippet // must be non-null.
         };

         this.endpoint = endpoint;
         this.endpointType = endpointType;
         this.params = params;
         this.searchResultType = searchResultType;
         this.searchTimeout = searchTimeout;
         this.searchField = searchField;
         this.scoreField = scoreField;
         this.snippetField = snippetField;

      }

      @SuppressWarnings({ "rawtypes", "unchecked" })
      private FulltextSearchMultiHiterator<IFulltextSearchHit<?>> getSolrSearchResultIterator(
            IBindingSet[] bsList) {

         return new FulltextSearchMultiHiterator(bsList, query, endpoint,
               endpointType, params, searchField, scoreField, snippetField, 
               searchResultType, searchTimeout, defaults, serviceCallParams);

      }

      @Override
      public IServiceOptions getServiceOptions() {

         return serviceOptions;

      }

      @Override
      public ICloseableIterator<IBindingSet> call(IBindingSet[] incomingBs) {

         final FulltextSearchMultiHiterator<IFulltextSearchHit<?>> hiterator = 
               getSolrSearchResultIterator(incomingBs);

         return new FulltextSearchHitConverter(hiterator);

      }

      /**
       * Converts {@link FulltextSearchHit}s into {@link IBindingSet}
       */
      private class FulltextSearchHitConverter implements
            ICloseableIterator<IBindingSet> {

         private final FulltextSearchMultiHiterator<IFulltextSearchHit<?>> src;

         private IFulltextSearchHit<?> current = null;
         private boolean open = true;

         public FulltextSearchHitConverter(
               final FulltextSearchMultiHiterator<IFulltextSearchHit<?>> src) {

            this.src = src;

         }

         /** TODO: closing logics */
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
         private IBindingSet newBindingSet(final IFulltextSearchHit<?> hit) {

            final BigdataValueFactory vf = BigdataValueFactoryImpl
                  .getInstance(store.getLexiconRelation().getNamespace());

            /**
             * The searchResultTyppe determines the type to which we cast results
             */
            final BigdataValue val;
            switch (hit.getSearchResultType()) {
            case LITERAL:
               val = vf.createLiteral(hit.getRes());
               break;
            case URI:
            default:
               try {
                  val = vf.createURI(hit.getRes());
               } catch (IllegalArgumentException e) {
                  
                  throw new FulltextSearchException(
                        FulltextSearchException.TYPE_CAST_EXCEPTION 
                        + ":" + hit.getRes());
               }
               break;
            }

            IBindingSet bs = new ListBindingSet();

            bs.set(vars[0], new Constant(DummyConstantNode.toDummyIV(val)));
            
            if (hit.getScore()!=null) {
               bs.set(vars[1], new Constant(new XSDNumericIV(hit.getScore())));
            }
            
            if (hit.getSnippet()!=null) {
               
               final BigdataLiteral litSnippet = 
                  vf.createLiteral(hit.getSnippet());
               bs.set(vars[2], 
                  new Constant(new Constant(
                     DummyConstantNode.toDummyIV(
                        (BigdataValue) litSnippet))));
            }
            
            final IBindingSet baseBs = hit.getIncomingBindings();
            final Iterator<IVariable> varIt = baseBs.vars();
            while (varIt.hasNext()) {

               final IVariable var = varIt.next();

               if (bs.isBound(var)) {
                  throw new FulltextSearchException(
                        "Illegal use of search service. Variable ?"
                              + var
                              + " must not be bound from outside. If you need to "
                              + " join on the variable, you may try nesting the"
                              + " SERVICE in a WITH block and join outside.");
               }

               bs.set(var, baseBs.get(var));

            }

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

      } // class FulltextSearchHitConverter

      @Override
      public List<IVariable<IV>> getMockVariables() {

         List<IVariable<IV>> externalVars = new LinkedList<IVariable<IV>>();
         for (int i = 0; i < vars.length; i++) {

            if (!vars[i].isAnonymous()) {

               externalVars.add(vars[i]);

            }
         }

         return externalVars;

      }

   }

   /**
    * Wrapper around {@link FulltextSearchHiterator}, delegating requests for
    * multiple binding sets to the latter one.
    */
   public static class FulltextSearchMultiHiterator<A extends IFulltextSearchHit> {

      final IBindingSet[] bindingSet;
      final TermNode query;
      final TermNode endpoint;
      final TermNode endpointType;
      final TermNode params;
      final TermNode searchResultType;
      final TermNode searchTimeout;
      final TermNode searchField;
      final TermNode scoreField;
      final TermNode snippetField;
      final ServiceCallCreateParams serviceCallParams;
      
      final FulltextSearchDefaults defaults;
      
      int nextBindingSetItr = 0;

      FulltextSearchHiterator<IFulltextSearchHit> curDelegate;

      public FulltextSearchMultiHiterator(final IBindingSet[] bindingSet,
            final TermNode query, final TermNode endpoint,
            final TermNode endpointType, final TermNode params,
            final TermNode searchField, final TermNode scoreField, 
            final TermNode snippetField, final TermNode searchResultType, 
            final TermNode searchTimeout, final FulltextSearchDefaults defaults,
            final ServiceCallCreateParams serviceCallParams) {

         this.query = query;
         this.bindingSet = bindingSet;
         this.endpoint = endpoint;
         this.endpointType = endpointType;
         this.params = params;
         this.searchResultType = searchResultType;
         this.searchTimeout = searchTimeout;
         this.searchField = searchField;
         this.scoreField = scoreField;
         this.snippetField = snippetField;
         
         this.defaults = defaults;
         this.serviceCallParams = serviceCallParams;

         init();

      }

      /**
       * Checks whether there are more results available.
       * 
       * @return
       */
      public boolean hasNext() {

         /*
          * Delegate will be set to null once all binding sets have been
          * processed
          */
         if (curDelegate == null) {

            return false;

         }

         /*
          * If there is a delegate set, ask him if there are results
          */
         if (curDelegate.hasNext()) {

            return true;

         } else {

            // if not, we set the next delegate
            if (nextDelegate()) { // in case there is one ...

               return hasNext(); // go into recursion

            }
         }

         return false; // fallback
      }

      public A next() {

         if (curDelegate == null) {

            return null; // no more results

         }

         if (curDelegate.hasNext()) {

            return (A) curDelegate.next();

         } else {

            if (nextDelegate()) {

               return next();
            }
         }

         return null; // reached the end

      }

      /**
       * @return true if a new delegate has been set successfully, false
       *         otherwise
       */
      private boolean nextDelegate() {

         if (bindingSet == null || nextBindingSetItr >= bindingSet.length) {
            curDelegate = null;
            return false;
         }

         final IBindingSet bs = bindingSet[nextBindingSetItr++];
         final String query = resolveQuery(bs);
         final String endpoint = resolveEndpoint(bs);
         final EndpointType endpointType = resolveEndpointType(bs);
         final String params = resolveParams(bs);
         final SearchResultType searchResultType = resolveSearchResultType(bs);
         final Integer searchTimeout = resolveSearchTimeout(bs);
         final String searchField = resolveSearchField(bs);
         final String scoreField = resolveScoreField(bs);
         final String snippetField = resolveSnippetField(bs);

         /*
          * Though we currently, we only support Solr, here we might easily hook
          * in other implementations based on the magic predicate
          */
         final IFulltextSearch ftSearch;
         switch (endpointType) {

         case SOLR:
         default:
            ftSearch = new SolrFulltextSearchImpl();
            break;
         }

         FulltextSearchQuery sq = new FulltextSearchQuery(
               query, params, endpoint, searchTimeout, searchField,
               scoreField, snippetField, bs, searchResultType);
         curDelegate = (FulltextSearchHiterator) 
               ftSearch.search(sq, serviceCallParams.getClientConnectionManager());

         return true;
      }

      private void init() {

         nextDelegate();

      }

      private String resolveQuery(IBindingSet bs) {

         String queryStr = resolveAsString(query, bs);

         if (queryStr == null || queryStr.isEmpty()) {

            throw new FulltextSearchException(
                  FulltextSearchException.NO_QUERY_SPECIFIED);

         }

         return queryStr;

      }

      private SearchResultType resolveSearchResultType(IBindingSet bs) {

         String searchResultTypeStr = resolveAsString(searchResultType, bs);

         // try override with system default, if not set
         if (searchResultTypeStr==null || searchResultTypeStr.isEmpty()) {
            searchResultTypeStr = defaults.getDefaultSearchResultType();
         }
                  
         if (searchResultTypeStr != null && !searchResultTypeStr.isEmpty()) {

            try {

               return SearchResultType.valueOf(searchResultTypeStr);

            } catch (Exception e) {

               // illegal, ignore and proceed
               if (log.isDebugEnabled()) {
                  log.warn("Illegal target type: " + searchResultTypeStr);
               }

            }

         }

         return FTS.Options.DEFAULT_SEARCH_RESULT_TYPE; // fallback

      }

      private Integer resolveSearchTimeout(IBindingSet bs) {

         String searchTimeoutStr = resolveAsString(searchTimeout, bs);

         // try override with system default, if not set
         if (searchTimeoutStr==null) {
            searchTimeoutStr = defaults.getDefaultTimeout();
         }
         
         if (searchTimeoutStr != null && !searchTimeoutStr.isEmpty()) {

            try {

               return Integer.valueOf(searchTimeoutStr);

            } catch (NumberFormatException e) {

               // illegal, ignore and proceed
               if (log.isInfoEnabled()) {
                  log.info("Illegal timeout string: " + searchTimeoutStr +
                        " -> will be ignored, using default.");

               }

            }
         }

         return FTS.Options.DEFAULT_TIMEOUT; // fallback
      }

      private String resolveParams(IBindingSet bs) {

         String paramStr = resolveAsString(params, bs);
         
         // try override with system default, if not set
         if (paramStr==null) { // allow for paramStr being empty string override
            paramStr = defaults.getDefaultParams();
         }
         
         return paramStr==null || paramStr.isEmpty() ?
            FTS.Options.DEFAULT_PARAMS : paramStr;

      }

      /**
       * Resolves the endpoint type, which is either a constant or a variable to
       * be looked up in the binding set.
       */
      private EndpointType resolveEndpointType(IBindingSet bs) {

         String endpointTypeStr = resolveAsString(endpointType, bs);

         // try override with system default, if not set
         if (endpointTypeStr==null) {
            endpointTypeStr = defaults.getDefaultEndpointType();
         }
         
         if (endpointTypeStr != null && !endpointTypeStr.isEmpty()) {

            try {

               return EndpointType.valueOf(endpointTypeStr);

            } catch (Exception e) {

               // illegal, ignore and proceed
               if (log.isDebugEnabled()) {
                  log.warn("Illegal endpoint type: " + endpointTypeStr + 
                        " -> will be ignored, using default.");
               }

            }
         }

         return FTS.Options.DEFAULT_ENDPOINT_TYPE; // fallback

      }

      /**
       * Resolves the endpoint, which is either a constant or a variable to be
       * looked up in the binding set.
       */
      private String resolveEndpoint(IBindingSet bs) {

         String endpointStr = resolveAsString(endpoint, bs);
         
         // try override with system default, if not set
         if (endpointStr==null || endpointStr.isEmpty()) {
            endpointStr = defaults.getDefaultEndpoint();
         }
         
         
         if (endpointStr != null && !endpointStr.isEmpty()) {

            return endpointStr;

         } else {

            throw new FulltextSearchException(
                  FulltextSearchException.NO_ENDPOINT_SPECIFIED);

         }

      }
      
      /**
       * Resolves the search field, which is either a constant or a variable
       * to be looked up in the binding set.
       */
      private String resolveSearchField(IBindingSet bs) {

         String searchFieldStr = resolveAsString(searchField, bs);
         
         // try override with system default, if not set
         if (searchFieldStr==null || searchFieldStr.isEmpty()) {
            searchFieldStr = defaults.getDefaultSearchField();
         }
         
         return searchFieldStr==null || searchFieldStr.isEmpty() ?
            FTS.Options.DEFAULT_SEARCH_FIELD : searchFieldStr;

      }
      
      /**
       * Resolves the search field, which is either a constant or a variable
       * to be looked up in the binding set.
       */
      private String resolveScoreField(IBindingSet bs) {

         String scoreFieldStr = resolveAsString(scoreField, bs);
         
         // try override with system default, if not set
         if (scoreFieldStr==null || scoreFieldStr.isEmpty()) {
            scoreFieldStr = defaults.getDefaultScoreField();
         }
         
         return scoreFieldStr==null || scoreFieldStr.isEmpty() ?
            FTS.Options.DEFAULT_SCORE_FIELD : scoreFieldStr;

      }
      
      /**
       * Resolves the search field, which is either a constant or a variable
       * to be looked up in the binding set.
       */
      private String resolveSnippetField(IBindingSet bs) {

         String snippetFieldStr = resolveAsString(snippetField, bs);
         
         // try override with system default, if not set
         if (snippetFieldStr==null || snippetFieldStr.isEmpty()) {
            snippetFieldStr = defaults.getDefaultSnippetField();
         }
         
         return snippetFieldStr==null || snippetFieldStr.isEmpty() ?
            FTS.Options.DEFAULT_SNIPPET_FIELD : snippetFieldStr;

      }

      private String resolveAsString(TermNode termNode, IBindingSet bs) {

         if (termNode == null) { // term node not set explicitly
            return null;
         }

         if (termNode.isConstant()) {

            final Literal lit = (Literal) termNode.getValue();
            return lit == null ? null : lit.stringValue();

         } else {

            if (bs == null) {
               return null; // shouldn't happen, but just in case...
            }

            final IVariable<?> var = (IVariable<?>) termNode
                  .getValueExpression();
            if (bs.isBound(var)) {
               IConstant<?> c = bs.get(var);
               if (c == null || c.get() == null
                     || !(c.get() instanceof TermId<?>)) {
                  return null;
               }

               TermId<?> cAsTerm = (TermId<?>) c.get();
               return cAsTerm.stringValue();

            } else {
               throw new FulltextSearchException(
                  FulltextSearchException.SERVICE_VARIABLE_UNBOUND + ":" + var);
            }
         }
      }

   }
   
   /**
    * Default values for external fulltext search, as defined in configuration
    * @author msc
    *
    */
   public static class FulltextSearchDefaults {

      final String defaultEndpoint;
      final String defaultEndpointType;
      final String defaultSearchResultType;
      final String defaultTimeout;
      final String defaultParams;
      final String defaultSearchField;
      final String defaultScoreField;
      final String defaultSnippetField;

      public FulltextSearchDefaults(final Properties p) {
         
         this.defaultEndpoint = 
               p.getProperty(FTS.Options.FTS_ENDPOINT);
         
         this.defaultEndpointType = 
               p.getProperty(FTS.Options.FTS_ENDPOINT_TYPE);
         
         this.defaultSearchResultType = 
               p.getProperty(FTS.Options.FTS_SEARCH_RESULT_TYPE);
         
         this.defaultTimeout =
               p.getProperty(FTS.Options.FTS_TIMEOUT);
         
         this.defaultParams =
               p.getProperty(FTS.Options.FTS_PARAMS);

         this.defaultSearchField =
               p.getProperty(FTS.Options.FTS_SEARCH_FIELD);
         
         this.defaultScoreField =
               p.getProperty(FTS.Options.FTS_SCORE_FIELD);
         
         this.defaultSnippetField =
               p.getProperty(FTS.Options.FTS_SNIPPET_FIELD);
      }
      
      
      public String getDefaultEndpoint() {
         return defaultEndpoint;
      }

      public String getDefaultEndpointType() {
         return defaultEndpointType;
      }

      public String getDefaultSearchResultType() {
         return defaultSearchResultType;
      }

      public String getDefaultTimeout() {
         return defaultTimeout;
      }

      public String getDefaultParams() {
         return defaultParams;
      }

      public String getDefaultSearchField() {
         return defaultSearchField;
      }

      public String getDefaultScoreField() {
         return defaultScoreField;
      }

      public String getDefaultSnippetField() {
         return defaultSnippetField;
      }
   }

   @Override
   public Set<IVariable<?>> getRequiredBound(final ServiceNode serviceNode) {

      /**
       * This method extracts exactly those variables that are incoming,
       * i.e. must be bound before executing the execution of the service.
       */
      final Set<IVariable<?>> requiredBound = new HashSet<IVariable<?>>();
      for (StatementPatternNode sp : getStatementPatterns(serviceNode)) {
            
         final URI predicate = (URI) (sp.p()).getValue();
         final IVariableOrConstant<?> object = sp.o().getValueExpression();
            
         if (object instanceof IVariable<?>) {
            
            if (predicate.equals(FTS.SEARCH) || predicate.equals(FTS.ENDPOINT)
               || predicate.equals(FTS.ENDPOINT_TYPE) || predicate.equals(FTS.PARAMS)
               || predicate.equals(FTS.SEARCH_RESULT_TYPE) || predicate.equals(FTS.SEARCH_FIELD)
               || predicate.equals(FTS.SCORE_FIELD) || predicate.equals(FTS.SNIPPET_FIELD)
               || predicate.equals(FTS.TIMEOUT)) {
               
               requiredBound.add((IVariable<?>)object); // the subject var is what we return                  
            }
         }
      }

      return requiredBound;
   }
   
   /**
    * Returns the statement patterns contained in the service node.
    * 
    * @param serviceNode
    * @return
    */
   Collection<StatementPatternNode> getStatementPatterns(final ServiceNode serviceNode) {

      final List<StatementPatternNode> statementPatterns = 
         new ArrayList<StatementPatternNode>();
      
      for (IGroupMemberNode child : serviceNode.getGraphPattern()) {
         
         if (child instanceof StatementPatternNode) {      
            statementPatterns.add((StatementPatternNode)child);
         } else {
            throw new FulltextSearchException("Nested groups are not allowed.");            
         }
      }
      
      return statementPatterns;
   }
}
