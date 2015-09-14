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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.filter.Advancer;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.constraints.RangeBOp;
import com.bigdata.rdf.internal.gis.CoordinateDD;
import com.bigdata.rdf.internal.gis.CoordinateUtility;
import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
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
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.service.fts.FulltextSearchException;
import com.bigdata.service.geospatial.GeoSpatial;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;
import com.bigdata.service.geospatial.IGeoSpatialQuery.GeoSpatialSearchQuery;
import com.bigdata.service.geospatial.ZOrderIndexBigMinAdvancer;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLon;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLonTime;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

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

      final Properties props = store.getIndexManager() != null
            && store.getIndexManager() instanceof AbstractJournal ? ((AbstractJournal) store
            .getIndexManager()).getProperties() : null;

      final GeoSpatialDefaults dflts = new GeoSpatialDefaults(props);

      final ServiceNode serviceNode = createParams.getServiceNode();

      if (serviceNode == null)
         throw new IllegalArgumentException();

      /*
       * Validate the geospatial predicates for a given search variable.
       */
      final Map<IVariable<?>, Map<URI, StatementPatternNode>> map = verifyGraphPattern(
            store, serviceNode.getGraphPattern());

      if (map == null)
         throw new RuntimeException("Not a geospatial service request.");

      if (map.size() != 1)
         throw new RuntimeException(
               "Multiple geospatial service requests may not be combined.");

      final Map.Entry<IVariable<?>, Map<URI, StatementPatternNode>> e = map
            .entrySet().iterator().next();

      final IVariable<?> searchVar = e.getKey();

      final Map<URI, StatementPatternNode> statementPatterns = e.getValue();

      validateSearch(searchVar, statementPatterns);

      /*
       * Create and return the geospatial service call object, which will
       * execute this search request.
       */
      return new GeoSpatialServiceCall(searchVar, statementPatterns,
            getServiceOptions(), dflts, store);

   }

   /**
    * Validate the search request. This looks for search magic predicates and
    * returns them all. It is an error if anything else is found in the group.
    * All such search patterns are reported back by this method, but the service
    * can only be invoked for one a single search variable at a time. The caller
    * will detect both the absence of any search and the presence of more than
    * one search and throw an exception.
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
               throw new RuntimeException("Expecting search predicate: " + sp);

            /*
             * Some search predicate.
             */

            if (!ASTGeoSpatialSearchOptimizer.searchUris.contains(uri))
               throw new RuntimeException("Unknown search predicate: " + uri);

            final TermNode s = sp.s();

            if (!s.isVariable())
               throw new RuntimeException(
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
    * Validate the search. There must be exactly one {@link BD#SEARCH}
    * predicate. There should not be duplicates of any of the search predicates
    * for a given searchVar.
    */
   private void validateSearch(final IVariable<?> searchVar,
         final Map<URI, StatementPatternNode> statementPatterns) {

      final Set<URI> uris = new LinkedHashSet<URI>();

      for (StatementPatternNode sp : statementPatterns.values()) {

         final URI uri = (URI) (sp.p()).getValue();

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
      private TermNode spatialCircleCenter = null;
      private TermNode spatialCircleRadius = null;
      private TermNode spatialRectangleUpperLeft = null;
      private TermNode spatialRectangleLowerRight = null;
      private TermNode spatialUnit = null;
      private TermNode timeStart = null;
      private TermNode timeEnd = null;

      private IVariable<?>[] vars;
      private final GeoSpatialDefaults defaults;
      private final AbstractTripleStore kb;

      public GeoSpatialServiceCall(final IVariable<?> searchVar,
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
         TermNode spatialCircleCenter = null;
         TermNode spatialCircleRadius = null;
         TermNode spatialRectangleUpperLeft = null;
         TermNode spatialRectangleLowerRight = null;
         TermNode spatialUnit = null;
         TermNode timeStart = null;
         TermNode timeEnd = null;

         for (StatementPatternNode meta : statementPatterns.values()) {

            final URI p = (URI) meta.p().getValue();

            if (GeoSpatial.PREDICATE.equals(p)) {
               predicate = meta.o();
            } else if (GeoSpatial.SPATIAL_CIRCLE_CENTER.equals(p)) {
               spatialCircleCenter = meta.o();
            } else if (GeoSpatial.SPATIAL_CIRCLE_RADIUS.equals(p)) {
               spatialCircleRadius = meta.o();
            } else if (GeoSpatial.SPATIAL_RECTANGLE_UPPER_LEFT.equals(p)) {
               spatialRectangleUpperLeft = meta.o();
            } else if (GeoSpatial.SPATIAL_RECTANGLE_LOWER_RIGHT.equals(p)) {
               spatialRectangleLowerRight = meta.o();
            } else if (GeoSpatial.SPATIAL_UNIT.equals(p)) {
               spatialUnit = meta.o();
            } else if (GeoSpatial.TIME_START.equals(p)) {
               timeStart = meta.o();
            } else if (GeoSpatial.TIME_END.equals(p)) {
               timeEnd = meta.o();
            }

         }

         // for now: a single variable containing the result
         this.vars = new IVariable[] { searchVar };

         this.predicate = predicate;
         this.spatialCircleCenter = spatialCircleCenter;
         this.spatialCircleRadius = spatialCircleRadius;
         this.spatialRectangleUpperLeft = spatialRectangleUpperLeft;
         this.spatialRectangleLowerRight = spatialRectangleLowerRight;
         this.spatialUnit = spatialUnit;
         this.timeStart = timeStart;
         this.timeEnd = timeEnd;
         this.kb = kb;

      }

      @Override
      public ICloseableIterator<IBindingSet> call(final IBindingSet[] incomingBs) {

         // iterate over the incoming binding set, issuing search requests
         // for all bindings in the binding set
         return new GeoSpatialInputBindingsIterator(incomingBs, searchFunction,
               predicate, spatialCircleCenter, spatialCircleRadius,
               spatialRectangleUpperLeft, spatialRectangleLowerRight,
               spatialUnit, timeStart, timeEnd, defaults, kb, this);

      }

      @Override
      public IServiceOptions getServiceOptions() {

         return serviceOptions;

      }

      /**
       * The search function itself, implementing a search request for a single
       * query.
       * 
       * @param query
       *           the geospatial search query
       * @param kb
       *           the triple store to issue search against
       * 
       * @return an iterator over the search results
       */
      @SuppressWarnings({ "unchecked", "rawtypes" })
      public ICloseableIterator<IBindingSet> search(
            final GeoSpatialSearchQuery query, final AbstractTripleStore kb) {

         final BOpContextBase context = new BOpContextBase(
               QueryEngineFactory.getQueryController(kb.getIndexManager()));

         final GlobalAnnotations globals = new GlobalAnnotations(kb
               .getLexiconRelation().getNamespace(), kb.getSPORelation()
               .getTimestamp());
         
         // this is needed for any function
         final Long timeStart = query.getTimeStart();
         final Long timeEnd = query.getTimeEnd();

         // the literal extension object used for conversion
         final GeoSpatialLiteralExtension<BigdataValue> litExt = 
            new GeoSpatialLiteralExtension<BigdataValue>(
               kb.getLexiconRelation());

         // construct the bounding boxes and filters, which depend on the
         // function that is specified within the query
         final Object[] lowerBorderComponents;
         final Object[] upperBorderComponents;
         final GeoSpatialFilterBase filter;
         
         switch (query.getSearchFunction()) {
         case IN_CIRCLE: {
            final PointLatLon centerPoint = query.getSpatialCircleCenter();
            
            final CoordinateDD centerPointDD = centerPoint.asCoordinateDD();
            
            final Double distance = query.getSpatialCircleRadius();
            final UNITS spatialUnit = query.getSpatialUnit();
            
            final CoordinateDD upperLeft = 
               CoordinateUtility.boundingBoxUpperLeft(
                  centerPointDD, distance, spatialUnit);
            
            final CoordinateDD lowerRight = 
               CoordinateUtility.boundingBoxLowerRight(
                  centerPointDD, distance, spatialUnit);
            
            // construct the points with time
            final PointLatLonTime upperLeftWithTime = 
               new PointLatLonTime(
                  PointLatLon.fromCoordinateDD(upperLeft), timeStart);
            
            final PointLatLonTime lowerRightWithTime = 
               new PointLatLonTime(
                  PointLatLon.fromCoordinateDD(lowerRight), timeEnd);

            // convert to componet strings
            lowerBorderComponents = PointLatLonTime
                  .toComponentString(upperLeftWithTime);
            
            upperBorderComponents = PointLatLonTime
                  .toComponentString(lowerRightWithTime);

            // set up the filter
            filter = new GeoSpatialInCircleFilter(
               centerPoint, distance, spatialUnit, timeStart, timeEnd, litExt);
         }
            break;

         case IN_RECTANGLE: {
            final PointLatLon upperLeft = query.getSpatialRectangleUpperLeft();
            final PointLatLon lowerRight = query
                  .getSpatialRectangleLowerRight();

            // construct the points with time
            final PointLatLonTime upperLeftWithTime = new PointLatLonTime(
                  upperLeft, timeStart);
            final PointLatLonTime lowerRightWithTime = new PointLatLonTime(
                  lowerRight, timeEnd);

            lowerBorderComponents = PointLatLonTime
                  .toComponentString(upperLeftWithTime);
            upperBorderComponents = PointLatLonTime
                  .toComponentString(lowerRightWithTime);

            filter = new GeoSpatialInRectangleFilter(
               upperLeftWithTime, lowerRightWithTime, litExt);
         }
            break;

         default:
            throw new RuntimeException("Unknown geospatial search function.");
         }
         

//         lowerBorderComponents = new Object[]{ (long)(double)lowerBorderComponents[0], (long)(double)lowerBorderComponents[1] };
//         upperBorderComponents = new Object[]{ (long)(double)upperBorderComponents[0], (long)(double)upperBorderComponents[1] };
         

         
         // set up range scan
         final Var oVar = Var.var(); // object position variable
         final RangeNode range = new RangeNode(new VarNode(oVar),
               new ConstantNode(litExt.createIV(lowerBorderComponents)),
               new ConstantNode(litExt.createIV(upperBorderComponents)));

         final RangeBOp rangeBop = ASTRangeOptimizer.toRangeBOp(context, range,
               globals);

         // set up the predicate
         final VarNode s = new VarNode(vars[0].getName());
         final VarNode o = new VarNode(oVar);

         
         // TODO: take SPORelation and call SPORelation.getPredicate() and remove pred==null check
         IPredicate<ISPO> pred = (IPredicate<ISPO>) kb.getPredicate(
               (URI) s.getValue(), /* subject */
               (URI) query.getPredicate().getValue(), /* predicate */
               o.getValue(), /* object */
               null, /* context */ // TODO: fix to make work for quads
               null, /* filter */
               rangeBop); /* rangeBop */

         
         /**
          * The predicate is null if the p we pass in does not appear in the
          * database. In that case, return null to indicate there aren't
          * matches.
          */
         if (pred == null) {
            return null;
         }

         pred = (IPredicate<ISPO>) pred.setProperty(
               IPredicate.Annotations.TIMESTAMP, kb.getSPORelation()
                     .getTimestamp());

         final IRelation<ISPO> relation = context.getRelation(pred);

         final AccessPath<ISPO> accessPath = (AccessPath<ISPO>) context
               .getAccessPath(relation, pred);


         final byte[] lowerZOrderKey = litExt.toZOrderByteArray(lowerBorderComponents);
         final byte[] upperZOrderKey = litExt.toZOrderByteArray(upperBorderComponents);
         
         final BigdataValueFactory vf = kb.getValueFactory();

         if (log.isInfoEnabled()) {
            log.info("Scanning from key " + litExt.asValue(lowerZOrderKey, vf) );
            log.info("Scanning to   key: " + litExt.asValue(upperZOrderKey, vf) );           
         }


         // extract position of subject and object in index
         final SPOKeyOrder keyOrder = (SPOKeyOrder)accessPath.getKeyOrder();
         final int subjectPos = keyOrder.getPositionInIndex(SPOKeyOrder.S);
         final int objectPos = keyOrder.getPositionInIndex(SPOKeyOrder.O);
         
         // set object (=z-order literal) position in the surrounding filter
         filter.setObjectPos(objectPos);

         // set up the advancer, which iterates exactly over the values in range
         // TODO: we want to have this multithreaded at some point
         final Advancer<SPO> bigMinAdvancer = 
            new ZOrderIndexBigMinAdvancer(
               lowerZOrderKey, upperZOrderKey, litExt, objectPos, vf);
         
         final Var var = Var.var(vars[0].getName());
         final IBindingSet incomingBindingSet = query.getIncomingBindings();
         final Iterator<IBindingSet> itr = 
            new Striterator(accessPath.getIndex().rangeIterator(
               accessPath.getFromKey(), accessPath.getToKey(), 0/* capacity */, 
               IRangeQuery.KEYS | IRangeQuery.CURSOR, bigMinAdvancer))
                  .addFilter(filter)
                  .addFilter(new Resolver() {

                     private static final long serialVersionUID = 1L;
                          
                     /**
                       * Resolve tuple to IV.
                       */
                     @Override
                     protected IBindingSet resolve(final Object obj) {

                        final byte[] key = ((ITuple<?>) obj).getKey();
                             
                        final IV[] ivs = IVUtility.decode(key, subjectPos+1);

                        final IBindingSet bs = incomingBindingSet.clone();
                        bs.set(var, new Constant<IV>(ivs[subjectPos]));

                        return bs;

                     }
                  }
               );

         return (ICloseableIterator<IBindingSet>)itr;
      }
   }
   
   
   /**
    * Iterates a geospatial search over a set of input bindings. This is done
    * incrementally, in a binding by binding fashion.
    */
   public static class GeoSpatialInputBindingsIterator implements
         ICloseableIterator<IBindingSet> {

      private final IBindingSet[] bindingSet;
      private final TermNode searchFunction;
      private final TermNode predicate;
      private final TermNode spatialCircleCenter;
      private final TermNode spatialCircleRadius;
      private final TermNode spatialRectangleUpperLeft;
      private final TermNode spatialRectangleLowerRight;
      private final TermNode spatialUnit;
      private final TermNode timeStart;
      private final TermNode timeEnd;

      final GeoSpatialDefaults defaults;
      final AbstractTripleStore kb;
      final GeoSpatialServiceCall serviceCall;

      int nextBindingSetItr = 0;

      ICloseableIterator<IBindingSet> curDelegate;

      public GeoSpatialInputBindingsIterator(final IBindingSet[] bindingSet,
            final TermNode searchFunction, final TermNode predicate,
            final TermNode spatialCircleCenter,
            final TermNode spatialCircleRadius,
            final TermNode spatialRectangleUpperLeft,
            final TermNode spatialRectangleLowerRight,
            final TermNode spatialUnit, final TermNode timeStart,
            final TermNode timeEnd, final GeoSpatialDefaults defaults,
            final AbstractTripleStore kb,
            final GeoSpatialServiceCall serviceCall) {

         this.bindingSet = bindingSet;
         this.searchFunction = searchFunction;
         this.predicate = predicate;
         this.spatialCircleCenter = spatialCircleCenter;
         this.spatialCircleRadius = spatialCircleRadius;
         this.spatialRectangleUpperLeft = spatialRectangleUpperLeft;
         this.spatialRectangleLowerRight = spatialRectangleLowerRight;
         this.spatialUnit = spatialUnit;
         this.timeStart = timeStart;
         this.timeEnd = timeEnd;
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
         final GeoFunction searchFunction = resolveAsGeoFunction(
               this.searchFunction, bs);
         final PointLatLon spatialCircleCenter = resolveAsPoint(
               this.spatialCircleCenter, bs);
         final Double spatialCircleRadius = resolveAsDouble(
               this.spatialCircleRadius, bs);
         final PointLatLon spatialRectangleUpperLeft = resolveAsPoint(
               this.spatialRectangleUpperLeft, bs);
         final PointLatLon spatialRectangleLowerRight = resolveAsPoint(
               this.spatialRectangleLowerRight, bs);
         final UNITS spatialUnit = resolveAsSpatialDistanceUnit(
               this.spatialUnit, bs);
         final Long timeStart = resolveAsLong(this.timeStart, bs);
         final Long timeEnd = resolveAsLong(this.timeEnd, bs);

         GeoSpatialSearchQuery sq = new GeoSpatialSearchQuery(searchFunction,
               predicate, spatialCircleCenter, spatialCircleRadius,
               spatialRectangleUpperLeft, spatialRectangleLowerRight,
               spatialUnit, timeStart, timeEnd, bs);

         curDelegate = serviceCall.search(sq, kb);

         return true;
      }

      private void init() {

         nextDelegate();

      }

      private GeoFunction resolveAsGeoFunction(TermNode termNode, IBindingSet bs) {

         String geoFunctionStr = resolveAsString(termNode, bs);

         // try override with system default, if not set
         if (geoFunctionStr == null) {
            geoFunctionStr = defaults.getDefaultFunction();
         }

         if (geoFunctionStr != null && !geoFunctionStr.isEmpty()) {

            try {

               return GeoFunction.forName(geoFunctionStr);

            } catch (NumberFormatException e) {

               // illegal, ignore and proceed
               if (log.isInfoEnabled()) {
                  log.info("Illegal geo function: " + geoFunctionStr
                        + " -> will be ignored, using default.");

               }

            }
         }

         return GeoSpatial.Options.DEFAULT_GEO_FUNCTION; // fallback

      }

      private UNITS resolveAsSpatialDistanceUnit(TermNode termNode,
            IBindingSet bs) {

         String spatialUnitStr = resolveAsString(termNode, bs);

         // try override with system default, if not set
         if (spatialUnitStr == null) {
            spatialUnitStr = defaults.getDefaultSpatialDistanceUnit();
         }

         if (spatialUnitStr != null && !spatialUnitStr.isEmpty()) {

            try {

               return UNITS.valueOf(spatialUnitStr);

            } catch (NumberFormatException e) {

               // illegal, ignore and proceed
               if (log.isInfoEnabled()) {
                  log.info("Illegal spatial unit: " + spatialUnitStr
                        + " -> will be ignored, using default.");

               }

            }
         }

         return GeoSpatial.Options.DEFAULT_GEO_SPATIAL_UNIT; // fallback

      }

      private Double resolveAsDouble(TermNode termNode, IBindingSet bs) {

         String s = resolveAsString(termNode, bs);
         if (s == null || s.isEmpty()) {
            return null;
         }

         try {
            return Double.valueOf(s);
         } catch (NumberFormatException e) {

            // illegal, ignore and proceed
            if (log.isInfoEnabled()) {
               log.info("Illegal double value: " + s
                     + " -> will be ignored, using default.");

            }
         }

         return null; // could not parse
      }

      private Long resolveAsLong(TermNode termNode, IBindingSet bs) {

         String s = resolveAsString(termNode, bs);
         if (s == null || s.isEmpty()) {
            return null;
         }

         try {
            return Long.valueOf(s);
         } catch (NumberFormatException e) {

            // illegal, ignore and proceed
            if (log.isInfoEnabled()) {
               log.info("Illegal double value: " + s
                     + " -> will be ignored, using default.");

            }
         }

         return null; // could not parse
      }

      private PointLatLon resolveAsPoint(TermNode termNode, IBindingSet bs) {

         String pointAsStr = resolveAsString(termNode, bs);
         if (pointAsStr == null || pointAsStr.isEmpty()) {
            return null;
         }

         try {

            return new PointLatLon(pointAsStr);

         } catch (NumberFormatException e) {

            // illegal, ignore and proceed
            if (log.isInfoEnabled()) {
               log.info("Illegal point value: " + pointAsStr
                     + " -> will be ignored, using default.");

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
                     FulltextSearchException.SERVICE_VARIABLE_UNBOUND + ":"
                           + var);
            }
         }
      }

      @Override
      public void remove() {

         if (curDelegate != null) {
            curDelegate.remove();
         }

      }

      @Override
      public void close() {

         if (curDelegate != null) {
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
      private final String defaultSpatialUnit;

      public GeoSpatialDefaults(final Properties p) {

         this.defaultFunction = p.getProperty(GeoSpatial.Options.GEO_FUNCTION);

         this.defaultSpatialUnit = p
               .getProperty(GeoSpatial.Options.GEO_SPATIAL_UNIT);
      }

      public String getDefaultFunction() {
         return defaultFunction;
      }

      public String getDefaultSpatialDistanceUnit() {
         return defaultSpatialUnit;
      }

   }

   @SuppressWarnings("rawtypes")
   public abstract static class GeoSpatialFilterBase extends TupleFilter {

      private static final long serialVersionUID = 2271038531634362860L;
      
      protected final GeoSpatialLiteralExtension<BigdataValue> litExt;

      // position of the subject in the tuples
      protected int objectPos = -1;

      public GeoSpatialFilterBase(
         final GeoSpatialLiteralExtension<BigdataValue> litExt) {
         
         this.litExt = litExt;
      }      
      


      public void setObjectPos(int objectPos) {
         this.objectPos = objectPos;
      }


      /**
       * Return the {@link GeoSpatialLiteralExtension} object associated
       * with this filter.
       * 
       * @return the object
       */
      public GeoSpatialLiteralExtension<BigdataValue> getGeoSpatialLiteralExtension() {
         return litExt;
      }

   }

   /**
    * Filter asserting that a given point lies into a specified square, defined
    * by its lower and upper border, plus time frame.
    */
   public static class GeoSpatialInCircleFilter extends GeoSpatialFilterBase {

      private static final long serialVersionUID = -346928614528045113L;

      final private CoordinateDD spatialPointAsCoordinateDD;
      final private Double distance;
      final private UNITS unit;

      final private Long timeMin;
      final private Long timeMax;

      public GeoSpatialInCircleFilter(
         final PointLatLon spatialPoint, Double distance,
         final UNITS unit, final Long timeMin, final Long timeMax, 
         final GeoSpatialLiteralExtension<BigdataValue> litExt) {
         
         super(litExt);

         this.spatialPointAsCoordinateDD = spatialPoint.asCoordinateDD();
         this.distance = distance;
         this.unit = unit;
         this.timeMin = timeMin;
         this.timeMax = timeMax;         
      }

      @Override
      @SuppressWarnings("rawtypes")      
      protected boolean isValid(ITuple tuple) {
         
			final PointLatLonTime p = asPoint(tuple);
			
			if (p==null) {
			   if (log.isInfoEnabled()) {
			      log.info(
			         "Something went wrong extracting the object. " +
			         "Rejecting unprocessable value.");
			   }

			   return false; // reject value
			}
			
         final CoordinateDD pAsDD = p.getSpatialPoint().asCoordinateDD();
			
			boolean ret = 
			   CoordinateUtility.distance(
			      pAsDD, spatialPointAsCoordinateDD, unit) <= distance &&
			      timeMin <= p.getTimestamp() && p.getTimestamp() <= timeMax;
			   
			return ret;
		}

      @SuppressWarnings("rawtypes")
      public PointLatLonTime asPoint(ITuple tuple) {

         final byte[] key = ((ITuple<?>) tuple).getKey();

         final IV[] ivs = IVUtility.decode(key, objectPos+1);
         final IV oIV= ivs[objectPos];

         if (oIV instanceof LiteralExtensionIV) {
            final LiteralExtensionIV lit = (LiteralExtensionIV) oIV;

            long[] longArr = litExt.asLongArray(lit);
            final Object[] components = litExt.longArrAsComponentArr(longArr);

            return new PointLatLonTime(
               new PointLatLon((Double) components[0],
               (Double) components[1]), (Long) components[2]);
         }

         return null; // something went wrong
      }
   
   }

   /**
    * Dummy filter asserting that a point delivered by the advancer lies into
    * a given rectangle. Given the current advancer implementation, there's
    * nothing to do for the filter, since the advancer delivers exactly those
    * points that are actually in this range.
    */
   public static class GeoSpatialInRectangleFilter extends GeoSpatialFilterBase {

      private static final long serialVersionUID = -314581671447912352L;

      @SuppressWarnings("unused") // unused for now, but may become important
      private final PointLatLonTime lowerBorder;
      
      @SuppressWarnings("unused") // unused for now, but may become important
      private final PointLatLonTime upperBorder;
      
      public GeoSpatialInRectangleFilter(
         final PointLatLonTime lowerBorder, final PointLatLonTime upperBorder,
         final GeoSpatialLiteralExtension<BigdataValue> litExt) {
         
         super(litExt);
         
         this.lowerBorder = lowerBorder;
         this.upperBorder = upperBorder;
      }

      @SuppressWarnings("rawtypes")
      @Override
      protected boolean isValid(ITuple tuple) {

         // the advancer by design already returns only those values that
         // are in the rectangle, so there's nothing to do here
         return true;
      }


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
            
            if (predicate.equals(GeoSpatial.PREDICATE)
               ||  predicate.equals(GeoSpatial.SEARCH)
               || predicate.equals(GeoSpatial.SPATIAL_CIRCLE_CENTER)
               || predicate.equals(GeoSpatial.SPATIAL_CIRCLE_RADIUS)
               || predicate.equals(GeoSpatial.SPATIAL_RECTANGLE_LOWER_RIGHT) 
               || predicate.equals(GeoSpatial.SPATIAL_RECTANGLE_UPPER_LEFT)
               || predicate.equals(GeoSpatial.SPATIAL_UNIT)
               || predicate.equals(GeoSpatial.TIME_START)
               || predicate.equals(GeoSpatial.TIME_END)) {
               
               requiredBound.add((IVariable<?>)object); // the subject var is what we return                  
            }
         }
      }

      return requiredBound;
   }
   
}
