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
 * Created on July 27, 2015
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.rdf.internal.impl.TermId;
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
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.search.IHit;
import com.bigdata.service.fts.FulltextSearchException;
import com.bigdata.service.fts.FulltextSearchHit;
import com.bigdata.service.fts.FulltextSearchHiterator;
import com.bigdata.service.geospatial.GeoSpatial;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;
import com.bigdata.service.geospatial.GeoSpatial.Point2D;
import com.bigdata.service.geospatial.GeoSpatial.SpatialUnit;
import com.bigdata.service.geospatial.GeoSpatial.TimeUnit;
import com.bigdata.service.geospatial.GeoSpatialQueryHiterator;
import com.bigdata.service.geospatial.IGeoSpatialQuery;
import com.bigdata.service.geospatial.IGeoSpatialQuery.GeoSpatialSearchQuery;
import com.bigdata.service.geospatial.IGeoSpatialQueryHit;
import com.bigdata.service.geospatial.impl.GeoSpatialQueryImpl;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A factory for a geospatial service, see {@link GeoSpatial#SEARCH}.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialServiceFactory extends AbstractServiceFactoryBase {

    private static final Logger log = Logger
            .getLogger(GeoSpatialServiceFactory.class);

    /*
     * Note: This could extend the base class to allow for search service
     * configuration options.
     */
    private final BigdataNativeServiceOptions serviceOptions;

    public GeoSpatialServiceFactory() {
        
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

        final Properties props =
              store.getIndexManager()!=null && 
              store.getIndexManager() instanceof AbstractJournal  ?
              ((AbstractJournal)store.getIndexManager()).getProperties() : null;
           
        final GeoSpatialDefaults dflts = new GeoSpatialDefaults(props);
        
        
        if (store == null)
            throw new IllegalArgumentException();

        final ServiceNode serviceNode = params.getServiceNode();

        if (serviceNode == null)
            throw new IllegalArgumentException();

        /*
         * Validate the geospatial predicates for a given search variable.
         */
        final Map<IVariable<?>, Map<URI, StatementPatternNode>> map =
           verifyGraphPattern(store, serviceNode.getGraphPattern());

        if (map == null)
            throw new RuntimeException("Not a geospatial service request.");

        if (map.size() != 1)
            throw new RuntimeException(
                    "Multiple geospatial service requests may not be combined.");

        final Map.Entry<IVariable<?>, Map<URI, StatementPatternNode>> e =
           map.entrySet().iterator().next();
        
        final IVariable<?> searchVar = e.getKey();
        
        final Map<URI, StatementPatternNode> statementPatterns = e.getValue();
        
        validateSearch(searchVar, statementPatterns);

        /*
         * Create and return the geospatial service call object, 
         * which will execute this search request.
         */

        return new SerivceCall(store, searchVar, statementPatterns,
                getServiceOptions(), dflts);
        
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
                    throw new RuntimeException("Expecting geospatial predicate: "
                            + sp);

                final URI uri = (URI) ((ConstantNode) p).getValue();

                if (!uri.stringValue().startsWith(GeoSpatial.NAMESPACE))
                    throw new RuntimeException("Expecting search predicate: "
                            + sp);

                /*
                 * Some search predicate.
                 */

                if (!ASTGeoSpatialSearchOptimizer.searchUris.contains(uri))
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

            assertObjectIsLiteralOrVariable(sp);
            
        }
        
        if (!uris.contains(GeoSpatial.SEARCH)) {
            throw new RuntimeException("Required search predicate not found: "
                    + GeoSpatial.SEARCH + " for searchVar=" + searchVar);
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

    /**
     * 
     * Note: This has the {@link AbstractTripleStore} reference attached. This
     * is not a {@link Serializable} object. It MUST run on the query
     * controller.
     */
    private static class SerivceCall implements BigdataServiceCall {

        private final AbstractTripleStore store;
        private final IServiceOptions serviceOptions;
        private final TermNode searchFunction;
        private TermNode spatialPoint = null;
        private TermNode spatialDistance = null;
        private TermNode spatialDistanceUnit = null;
        private TermNode timePoint = null;
        private TermNode timeDistance = null;
        private TermNode timeDistanceUnit = null;
        private IVariable<?>[] vars;
        private final GeoSpatialDefaults defaults;
        
        public SerivceCall(
                final AbstractTripleStore store,
                final IVariable<?> searchVar,
                final Map<URI, StatementPatternNode> statementPatterns,
                final IServiceOptions serviceOptions,
                final GeoSpatialDefaults dflts) {

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
            
            this.defaults = dflts;
            
            /*
             * Unpack the "search" magic predicate:
             * 
             * [?searchVar bd:search objValue]
             */
            final StatementPatternNode sp = statementPatterns.get(GeoSpatial.SEARCH);

            searchFunction = sp.o();
            
            TermNode spatialPoint = null;
            TermNode spatialDistance = null;
            TermNode spatialDistanceUnit = null;
            TermNode timePoint = null;
            TermNode timeDistance = null;
            TermNode timeDistanceUnit = null;
            for (StatementPatternNode meta : statementPatterns.values()) {

               final URI p = (URI) meta.p().getValue();

               if (GeoSpatial.SPATIAL_POINT.equals(p)) {
                  spatialPoint = meta.o();
               } else if (GeoSpatial.SPATIAL_DISTANCE.equals(p)) {
                  spatialDistance = meta.o();
               } else if (GeoSpatial.SPATIAL_DISTANCE_UNIT.equals(p)) {
                  spatialDistanceUnit = meta.o();
               } else if (GeoSpatial.TIME_POINT.equals(p)) {
                  timePoint = meta.o();
               } else if (GeoSpatial.TIME_DISTANCE.equals(p)) {
                  timeDistance = meta.o();
               } else if (GeoSpatial.TIME_DISTANCE_UNIT.equals(p)) {
                  timeDistanceUnit = meta.o();
               }
               
            }

            this.vars = new IVariable[] { searchVar };
            
            this.spatialPoint = spatialPoint;
            this.spatialDistance = spatialDistance;
            this.spatialDistanceUnit = spatialDistanceUnit;
            this.timePoint = timePoint;
            this.timeDistance = timeDistance;
            this.timeDistanceUnit = timeDistanceUnit;

        }

        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public ICloseableIterator<IBindingSet> call(
                final IBindingSet[] incomingBs) {

           final GeoSpatialMultiHiterator<IGeoSpatialQueryHit<?>> hiterator = 
                 getGeoSpatialSearchMultiHiterator(incomingBs);

           return new GeoSpatialSearchHitConverter(hiterator);
            
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        private GeoSpatialMultiHiterator getGeoSpatialSearchMultiHiterator(
              IBindingSet[] bsList) {

           return new GeoSpatialMultiHiterator(bsList,  searchFunction,
              spatialPoint, spatialDistance, spatialDistanceUnit,
              timePoint, timeDistance, timeDistanceUnit, defaults);

        }
        

        /**
         * Converts {@link FulltextSearchHit}s into {@link IBindingSet}
         */
        private class GeoSpatialSearchHitConverter implements ICloseableIterator<IBindingSet> {

           private final GeoSpatialMultiHiterator<IGeoSpatialQueryHit<?>> src;

           private IGeoSpatialQueryHit<?> current = null;
           private boolean open = true;

           public GeoSpatialSearchHitConverter(
                 final GeoSpatialMultiHiterator<IGeoSpatialQueryHit<?>> src) {

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

              final IGeoSpatialQueryHit<?> tmp = current;

              current = null;

              return newBindingSet(tmp);

           }

           /**
            * Convert an {@link IHit} into an {@link IBindingSet}.
            */
           private IBindingSet newBindingSet(final IGeoSpatialQueryHit<?> hit) {

              final BigdataValueFactory vf = BigdataValueFactoryImpl
                    .getInstance(store.getLexiconRelation().getNamespace());

              IBindingSet bs = new ListBindingSet();

              bs.set(vars[0], new Constant(DummyConstantNode.toDummyIV(hit.getRes())));
              
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

              return bs;

           }

           public void remove() {

              throw new UnsupportedOperationException();

           }

        } // class FulltextSearchHitConverter


        @Override
        public IServiceOptions getServiceOptions() {
            
            return serviceOptions;
            
        }
        
    }
    
    
    
    /**
     * Wrapper around {@link FulltextSearchHiterator}, delegating requests for
     * multiple binding sets to the latter one.
     */
    public static class GeoSpatialMultiHiterator<A extends IGeoSpatialQueryHit> {
       
       final IBindingSet[] bindingSet;
       final TermNode searchFunction;
       final TermNode spatialPoint;
       final TermNode spatialDistance;
       final TermNode spatialDistanceUnit;
       final TermNode timePoint;
       final TermNode timeDistance;
       final TermNode timeDistanceUnit;
       final GeoSpatialDefaults defaults;
       
       int nextBindingSetItr = 0;

       GeoSpatialQueryHiterator curDelegate;

       public GeoSpatialMultiHiterator(
             final IBindingSet[] bindingSet,  final TermNode searchFunction, 
             final TermNode spatialPoint, final TermNode spatialDistance, 
             final TermNode spatialDistanceUnit, final TermNode timePoint,
             final TermNode timeDistance, final TermNode timeDistanceUnit,
             final GeoSpatialDefaults defaults) {

          this.bindingSet = bindingSet;
          this.searchFunction = searchFunction;
          this.spatialPoint = spatialPoint;
          this.spatialDistance = spatialDistance;
          this.spatialDistanceUnit = spatialDistanceUnit;          
          this.timePoint = timePoint;
          this.timeDistance = timeDistance;
          this.timeDistanceUnit = timeDistanceUnit;
          this.defaults = defaults;

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

       public IGeoSpatialQueryHit<?> next() {

          if (curDelegate == null) {

             return null; // no more results

          }

          if (curDelegate.hasNext()) {

             return (IGeoSpatialQueryHit<?>) curDelegate.next();

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

          // TODO: refined type resolvment
          final IBindingSet bs = bindingSet[nextBindingSetItr++];
          final GeoFunction searchFunction = resolveAsGeoFunction(this.searchFunction, bs);
          final Point2D spatialPoint = resolveAsPoint(this.spatialPoint, bs);
          final Double spatialDistance = resolveAsDouble(this.spatialDistance, bs);
          final SpatialUnit spatialDistanceUnit = resolveAsSpatialDistanceUnit(this.spatialDistanceUnit, bs);
          final Double timePoint = resolveAsDouble(this.timePoint, bs);
          final Double timeDistance = resolveAsDouble(this.timeDistance, bs);
          final TimeUnit timeDistanceUnit = resolveAsTimeDistanceUnit(this.timeDistanceUnit, bs);

          /*
           * Though we currently, we only support Solr, here we might easily hook
           * in other implementations based on the magic predicate
           */
          final IGeoSpatialQuery geospatialSearch = new GeoSpatialQueryImpl();

          GeoSpatialSearchQuery sq = new GeoSpatialSearchQuery(
                searchFunction, spatialPoint, spatialDistance, spatialDistanceUnit,
                timePoint, timeDistance, timeDistanceUnit);
          
          curDelegate = (GeoSpatialQueryHiterator)  geospatialSearch.search(sq);

          return true;
       }

       private void init() {

          nextDelegate();

       }
       
       private GeoFunction resolveAsGeoFunction(TermNode termNode, IBindingSet bs) {
          
          String geoFunctionStr = resolveAsString(termNode, bs);
          
          // try override with system default, if not set
          if (geoFunctionStr==null) {
             geoFunctionStr = defaults.getDefaultFunction();
          }
          
          if (geoFunctionStr != null && !geoFunctionStr.isEmpty()) {

             try {

                return GeoFunction.valueOf(geoFunctionStr);

             } catch (NumberFormatException e) {

                // illegal, ignore and proceed
                if (log.isInfoEnabled()) {
                   log.info("Illegal geo function: " + geoFunctionStr +
                         " -> will be ignored, using default.");

                }

             }
          }

          return GeoSpatial.Options.DEFAULT_GEO_FUNCTION; // fallback

       }
       
       
       private TimeUnit resolveAsTimeDistanceUnit(TermNode termNode, IBindingSet bs) {
          
          String timeUnitStr = resolveAsString(termNode, bs);
          
          // try override with system default, if not set
          if (timeUnitStr==null) {
             timeUnitStr = defaults.getDefaultTimeDistanceUnit();
          }
          
          if (timeUnitStr != null && !timeUnitStr.isEmpty()) {

             try {

                return TimeUnit.valueOf(timeUnitStr);

             } catch (NumberFormatException e) {

                // illegal, ignore and proceed
                if (log.isInfoEnabled()) {
                   log.info("Illegal time unit: " + timeUnitStr +
                         " -> will be ignored, using default.");

                }

             }
          }

          return GeoSpatial.Options.DEFAULT_GEO_TIME_DISTANCE_UNIT; // fallback

       }
       
       private SpatialUnit resolveAsSpatialDistanceUnit(TermNode termNode, IBindingSet bs) {
          
          String spatialUnitStr = resolveAsString(termNode, bs);
          
          // try override with system default, if not set
          if (spatialUnitStr==null) {
             spatialUnitStr = defaults.getDefaultSpatialDistanceUnit();
          }
          
          if (spatialUnitStr != null && !spatialUnitStr.isEmpty()) {

             try {

                return SpatialUnit.valueOf(spatialUnitStr);

             } catch (NumberFormatException e) {

                // illegal, ignore and proceed
                if (log.isInfoEnabled()) {
                   log.info("Illegal spatial unit: " + spatialUnitStr +
                         " -> will be ignored, using default.");

                }

             }
          }

          return GeoSpatial.Options.DEFAULT_GEO_SPATIAL_DISTANCE_UNIT; // fallback

       }
       
       private Double resolveAsDouble(TermNode termNode, IBindingSet bs) {
          
          String s = resolveAsString(termNode, bs);
          if (s==null || s.isEmpty()) {
             return null;
          }
          
          try {
             return Double.valueOf(s);
          } catch (NumberFormatException e) {
             
             // illegal, ignore and proceed
             if (log.isInfoEnabled()) {
                log.info("Illegal double value: " + s +
                      " -> will be ignored, using default.");

             }
          }
          
          return null; // could not parse
       }
       
       private Point2D resolveAsPoint(TermNode termNode, IBindingSet bs) {
          
          String pointAsStr = resolveAsString(termNode, bs);
          if (pointAsStr==null || pointAsStr.isEmpty()) {
             return null;
          }
          
          try {
             
             return new Point2D(pointAsStr);

          } catch (NumberFormatException e) {
             
             // illegal, ignore and proceed
             if (log.isInfoEnabled()) {
                log.info("Illegal point value: " + pointAsStr +
                      " -> will be ignored, using default.");

             }
          }
          
          return null; // could not parse
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
     * Default values for geospatial service, such as unit definitions.
     * 
     * @author msc
     */
    public static class GeoSpatialDefaults {

       private final String defaultFunction;
       private final String defaultSpatialDistanceUnit;
       private final String defaultTimeDistanceUnit;

       public GeoSpatialDefaults(final Properties p) {
          
          this.defaultFunction = 
                p.getProperty(GeoSpatial.Options.GEO_FUNCTION);
          
          this.defaultSpatialDistanceUnit = 
                p.getProperty(GeoSpatial.Options.GEO_SPATIAL_DISTANCE_UNIT);
          
          this.defaultTimeDistanceUnit = 
                p.getProperty(GeoSpatial.Options.GEO_TIME_DISTANCE_UNIT);
          
       }

      public String getDefaultFunction() {
         return defaultFunction;
      }

      public String getDefaultSpatialDistanceUnit() {
         return defaultSpatialDistanceUnit;
      }

      public String getDefaultTimeDistanceUnit() {
         return defaultTimeDistanceUnit;
      }
    }

}
