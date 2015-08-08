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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.RangeBOp;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.RangeNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.optimizers.ASTRangeOptimizer;
import com.bigdata.rdf.sparql.ast.service.BigdataNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.service.fts.FulltextSearchException;
import com.bigdata.service.geospatial.GeoSpatial;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;
import com.bigdata.service.geospatial.GeoSpatial.SpatialUnit;
import com.bigdata.service.geospatial.GeoSpatial.TimeUnit;
import com.bigdata.service.geospatial.IGeoSpatialQuery.GeoSpatialSearchQuery;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.BoundingBoxLatLonTime;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLon;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLonTime;
import com.bigdata.striterator.IChunkedOrderedIterator;

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
    
    public BigdataServiceCall create(final ServiceCallCreateParams createParams) {

        if (createParams == null)
            throw new IllegalArgumentException();

        final AbstractTripleStore store = createParams.getTripleStore();

        final Properties props =
              store.getIndexManager()!=null && 
              store.getIndexManager() instanceof AbstractJournal  ?
              ((AbstractJournal)store.getIndexManager()).getProperties() : null;
           
        final GeoSpatialDefaults dflts = new GeoSpatialDefaults(props);

        final ServiceNode serviceNode = createParams.getServiceNode();

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
        return new GeoSpatialServiceCall(
           searchVar, statementPatterns, getServiceOptions(), dflts, store);
        
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

            if (uri.equals(GeoSpatial.PREDICATE)) {
               assertObjectIsUri(sp); // the predicate of the triple must be fixed
            } else {
               assertObjectIsLiteralOrVariable(sp);
            }
        }
        
        if (!uris.contains(GeoSpatial.SEARCH)) {
            throw new RuntimeException("Required search predicate not found: "
                    + GeoSpatial.SEARCH + " for searchVar=" + searchVar);
        }
        
    }

    private void assertObjectIsUri(final StatementPatternNode sp) {

       final TermNode o = sp.o();

       if (o instanceof URI) {

          throw new IllegalArgumentException(
                "Object is not literal or variable: " + sp);

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
    * Note: This has the {@link AbstractTripleStore} reference attached. This is
    * not a {@link Serializable} object. It MUST run on the query controller.
    */
   private static class GeoSpatialServiceCall implements BigdataServiceCall {

      private final IServiceOptions serviceOptions;
      private final TermNode searchFunction;
      private TermNode predicate = null;
      private TermNode spatialPoint = null;
      private TermNode spatialDistance = null;
      private TermNode spatialDistanceUnit = null;
      private TermNode timePoint = null;
      private TermNode timeDistance = null;
      private TermNode timeDistanceUnit = null;
      private IVariable<?>[] vars;
      private final GeoSpatialDefaults defaults;
      private final AbstractTripleStore kb;

      public GeoSpatialServiceCall(
            final IVariable<?> searchVar,
            final Map<URI, StatementPatternNode> statementPatterns,
            final IServiceOptions serviceOptions,
            final GeoSpatialDefaults dflts, final AbstractTripleStore kb) {

         if (searchVar == null)
            throw new IllegalArgumentException();

         if (statementPatterns == null)
            throw new IllegalArgumentException();

         if (serviceOptions == null)
            throw new IllegalArgumentException();

         if (kb == null)
            throw new IllegalArgumentException();

         this.serviceOptions = serviceOptions;

         this.defaults = dflts;

         /*
          * Unpack the "search" magic predicate:
          * 
          * [?searchVar bd:search objValue]
          */
         final StatementPatternNode sp = statementPatterns
               .get(GeoSpatial.SEARCH);

         searchFunction = sp.o();

         TermNode predicate = null;
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
            } else if (GeoSpatial.PREDICATE.equals(p)) {
               predicate = meta.o();
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

         // for now: a single variable containing the result
         this.vars = new IVariable[] { searchVar };

         this.predicate = predicate;
         this.spatialPoint = spatialPoint;
         this.spatialDistance = spatialDistance;
         this.spatialDistanceUnit = spatialDistanceUnit;
         this.timePoint = timePoint;
         this.timeDistance = timeDistance;
         this.timeDistanceUnit = timeDistanceUnit;
         this.kb = kb;

      }
        
      @SuppressWarnings({ "unchecked", "rawtypes" })
      public ICloseableIterator<IBindingSet> search(
            final GeoSpatialSearchQuery query,
            final AbstractTripleStore kb) {

         final BOpContextBase context = new BOpContextBase(
               QueryEngineFactory.getQueryController(kb.getIndexManager()));

         final GlobalAnnotations globals = new GlobalAnnotations(
               kb.getLexiconRelation().getNamespace(),
               kb.getSPORelation().getTimestamp());

         final PointLatLonTime centerPoint = new PointLatLonTime(
               query.getSpatialPoint(), query.getTimePoint());

         final BoundingBoxLatLonTime boundingBox = new BoundingBoxLatLonTime(
               centerPoint, query.getSpatialDistance() /* lat */,
               query.getSpatialDistance() /* lon */, query.getTimeDistance() /* time */
         );

         final Var oVar = Var.var(); // object position variable
         final StatementPatternNode sp = 
            new StatementPatternNode(
               new VarNode(vars[0].getName()), query.getPredicate(), new VarNode(oVar));
         
         // construct the RangeBOp and attach to triple pattern
         GeoSpatialLiteralExtension<BigdataValue> litExt = 
            new GeoSpatialLiteralExtension<BigdataValue>(kb.getLexiconRelation());
         
         final Object[] lowerBorderComponents = 
            PointLatLonTime.toComponentString(boundingBox.getLowerBorder());
            
         final Object[] upperBorderComponents = 
            PointLatLonTime.toComponentString(boundingBox.getUpperBorder());
         
         // set up range scan node
         final RangeNode range = new RangeNode(
            new VarNode(oVar),
            new ConstantNode(
               litExt.createIV(lowerBorderComponents)),
            new ConstantNode(
               litExt.createIV(upperBorderComponents))
         );

         final RangeBOp rangeBop = 
            ASTRangeOptimizer.toRangeBOp(context, range, globals);
                  
         range.setRangeBOp(rangeBop);
         sp.setRange(range);

         IPredicate<ISPO> pred = (IPredicate<ISPO>)
               kb.getPredicate(
                     sp.s() != null && sp.s().isConstant() ? (Resource) sp.s().getValue() : null, 
                     sp.p() != null && sp.p().isConstant() ? (URI) sp.p().getValue() : null, 
                     sp.o() != null && sp.o().isConstant() ? (Value) sp.o().getValue() : null, 
                     sp.c() != null && sp.c().isConstant() ? (Resource) sp.c().getValue() : null,
                     null, rangeBop);

         /**
          * The predicate is null is the p we pass in does not appear in the
          * database. In that case, we return null to indicate that there are
          * no matches.
          */
         if (pred==null) {
            return null;
         }
         
         pred = (IPredicate<ISPO>) pred.setProperty(
               IPredicate.Annotations.TIMESTAMP, kb.getSPORelation().getTimestamp());

         final IRelation<ISPO> relation = context.getRelation(pred);

         final AccessPath<ISPO> accessPath = 
            (AccessPath<ISPO>) context.getAccessPath(relation, pred);

         IChunkedOrderedIterator<ISPO> apIt = accessPath.iterator();

         return new ISPOIteratorWrapper(apIt,Var.var(vars[0].getName()));
      }

        @Override
        public ICloseableIterator<IBindingSet> call(
                final IBindingSet[] incomingBs) {

           return getGeoSpatialSearchMultiHiterator(incomingBs);
            
        }

        private ICloseableIterator<IBindingSet> getGeoSpatialSearchMultiHiterator(
              IBindingSet[] bsList) {

           return new GeoSpatialInputBindingsIterator(bsList, searchFunction,
              predicate, spatialPoint, spatialDistance, spatialDistanceUnit,
              timePoint, timeDistance, timeDistanceUnit, defaults, kb, this);

        }
        

        @Override
        public IServiceOptions getServiceOptions() {
            
            return serviceOptions;
            
        }
        
    }
    
   public static class ISPOIteratorWrapper implements
         ICloseableIterator<IBindingSet> {

      private final IChunkedOrderedIterator<ISPO> delegate;
      @SuppressWarnings("rawtypes")
      private final Var var;

      /**
       * TODO: pass in addition filter to post-process results (e.g.: circle,
       * bounding box).
       */
      @SuppressWarnings("rawtypes")
      public ISPOIteratorWrapper(
         final IChunkedOrderedIterator<ISPO> delegate, final Var var) {
         
         this.delegate = delegate;
         this.var = var;
      }

      @Override
      public boolean hasNext() {
         return delegate.hasNext();
      }

      @SuppressWarnings("rawtypes")
      @Override
      public IBindingSet next() {
         
         final ISPO elem = delegate.next();
         
         final IBindingSet bs = new ListBindingSet();
         bs.set(var, new Constant<IV>(elem.s()));
         
         return bs;
      }

      @Override
      public void remove() {
         delegate.remove();
      }

      @Override
      public void close() {
         delegate.close();
      }

   }
    
    /**
     * Iterates a geospatial search over a set of input bindings. This is done
     * incrementally, in a binding by binding fashion.
     */
    public static class GeoSpatialInputBindingsIterator 
    implements ICloseableIterator<IBindingSet> {
       
       final IBindingSet[] bindingSet;
       final TermNode searchFunction;
       final TermNode predicate;
       final TermNode spatialPoint;
       final TermNode spatialDistance;
       final TermNode spatialDistanceUnit;
       final TermNode timePoint;
       final TermNode timeDistance;
       final TermNode timeDistanceUnit;
       final GeoSpatialDefaults defaults;
       final AbstractTripleStore kb;
       final GeoSpatialServiceCall serviceCall;
       
       int nextBindingSetItr = 0;

       ICloseableIterator<IBindingSet> curDelegate;

       public GeoSpatialInputBindingsIterator(final IBindingSet[] bindingSet, 
             final TermNode searchFunction, final TermNode predicate,
             final TermNode spatialPoint, final TermNode spatialDistance, 
             final TermNode spatialDistanceUnit, final TermNode timePoint,
             final TermNode timeDistance, final TermNode timeDistanceUnit,
             final GeoSpatialDefaults defaults, final AbstractTripleStore kb,
             GeoSpatialServiceCall serviceCall) {

          this.bindingSet = bindingSet;
          this.searchFunction = searchFunction;
          this.predicate = predicate;
          this.spatialPoint = spatialPoint;
          this.spatialDistance = spatialDistance;
          this.spatialDistanceUnit = spatialDistanceUnit;          
          this.timePoint = timePoint;
          this.timeDistance = timeDistance;
          this.timeDistanceUnit = timeDistanceUnit;
          this.defaults = defaults;
          this.kb = kb;
          this.serviceCall = serviceCall;

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

       public IBindingSet next() {

          if (curDelegate == null) {

             return null; // no more results

          }

          if (curDelegate.hasNext()) {

             return curDelegate.next();

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
          final GeoFunction searchFunction = resolveAsGeoFunction(this.searchFunction, bs);
          final PointLatLon spatialPoint = resolveAsPoint(this.spatialPoint, bs);
          final Double spatialDistance = resolveAsDouble(this.spatialDistance, bs);
          final SpatialUnit spatialDistanceUnit = resolveAsSpatialDistanceUnit(this.spatialDistanceUnit, bs);
          final Long timePoint = resolveAsLong(this.timePoint, bs);
          final Long timeDistance = resolveAsLong(this.timeDistance, bs);
          final TimeUnit timeDistanceUnit = resolveAsTimeDistanceUnit(this.timeDistanceUnit, bs);


          GeoSpatialSearchQuery sq = new GeoSpatialSearchQuery(searchFunction,
                predicate, spatialPoint, spatialDistance, spatialDistanceUnit,
                timePoint, timeDistance, timeDistanceUnit, bs);
          
          curDelegate = serviceCall.search(sq, kb);

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
       
       private Long resolveAsLong(TermNode termNode, IBindingSet bs) {
          
          String s = resolveAsString(termNode, bs);
          if (s==null || s.isEmpty()) {
             return null;
          }
          
          try {
             return Long.valueOf(s);
          } catch (NumberFormatException e) {
             
             // illegal, ignore and proceed
             if (log.isInfoEnabled()) {
                log.info("Illegal double value: " + s +
                      " -> will be ignored, using default.");

             }
          }
          
          return null; // could not parse
       }
       
       
       private PointLatLon resolveAsPoint(TermNode termNode, IBindingSet bs) {
          
          String pointAsStr = resolveAsString(termNode, bs);
          if (pointAsStr==null || pointAsStr.isEmpty()) {
             return null;
          }
          
          try {
             
             return new PointLatLon(pointAsStr);

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

       @Override
       public void remove() {
          
          if (curDelegate!=null) {
             curDelegate.remove();
          }
          
       }

       @Override
       public void close() {
          
          if (curDelegate!=null) {
             curDelegate.close();
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
