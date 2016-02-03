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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.bop.join.BaseJoinStats;
import com.bigdata.bop.join.PipelineJoin.Annotations;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.filter.Advancer;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.constraints.RangeBOp;
import com.bigdata.rdf.internal.gis.CoordinateUtility;
import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
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
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.ChunkConsumerIterator;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.service.fts.FulltextSearchException;
import com.bigdata.service.geospatial.GeoSpatial;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;
import com.bigdata.service.geospatial.GeoSpatialCounters;
import com.bigdata.service.geospatial.IGeoSpatialQuery;
import com.bigdata.service.geospatial.ZOrderIndexBigMinAdvancer;
import com.bigdata.service.geospatial.impl.GeoSpatialQuery;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLon;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLonTime;
import com.bigdata.util.BytesUtil;
import com.bigdata.util.concurrent.Haltable;
import com.bigdata.util.concurrent.LatchedExecutor;

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

      /**
       * Get the service call configuration from annotations (attachable via query hints).
       * Here's how to define the hints:
       * 
       * <code>
          hint:Prior <http://www.bigdata.com/queryHints#maxParallel> "20" .
          hint:Prior <http://www.bigdata.com/queryHints#com.bigdata.relation.accesspath.BlockingBuffer.chunkOfChunksCapacity> "10" .
          hint:Prior <http://www.bigdata.com/queryHints#com.bigdata.relation.accesspath.IBuffer.chunkCapacity> "100" .
          hint:Prior <http://www.bigdata.com/queryHints#com.bigdata.bop.join.PipelineJoin.avgDataPointsPerThread> "25000" .
         </code>
       */
      final Integer maxParallel = 
         serviceNode.getQueryHintAsInteger(
            PipelineOp.Annotations.MAX_PARALLEL, 
            PipelineOp.Annotations.DEFAULT_MAX_PARALLEL);
      final Integer minDatapointsPerTask =
         serviceNode.getQueryHintAsInteger(
            Annotations.MIN_DATAPOINTS_PER_TASK, 
            Annotations.DEFAULT_MIN_DATAPOINTS_PER_TASK);              
      final Integer numTasksPerThread = 
         serviceNode.getQueryHintAsInteger(
            Annotations.NUM_TASKS_PER_THREAD, 
            Annotations.DEFAULT_NUM_TASKS_PER_THREAD);
      final Integer threadLocalBufferCapacity = 
         serviceNode.getQueryHintAsInteger(
            BufferAnnotations.CHUNK_CAPACITY, 
            BufferAnnotations.DEFAULT_CHUNK_CAPACITY);
      final Integer globalBufferChunkOfChunksCapacity = 
         serviceNode.getQueryHintAsInteger(
            BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY, 
            BufferAnnotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);
  
      if (log.isDebugEnabled()) {
         log.debug("maxParallel=" + maxParallel);
         log.debug("numTasksPerThread=" + numTasksPerThread);
         log.debug("threadLocalBufferCapacity=" + threadLocalBufferCapacity);
         log.debug("globalBufferChunkOfChunksCapacity=" + globalBufferChunkOfChunksCapacity);
      }
      
      /*
       * Create and return the geospatial service call object, which will
       * execute this search request.
       */
      return new GeoSpatialServiceCall(searchVar, statementPatterns,
            getServiceOptions(), dflts, store, maxParallel, 
            numTasksPerThread*maxParallel /* max num tasks to generate */, 
            minDatapointsPerTask, threadLocalBufferCapacity, 
            globalBufferChunkOfChunksCapacity, createParams.getStats());

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
    * Validate the search. There must be exactly one {@link GeoSpatial#SEARCH}
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

         if (uri.equals(GeoSpatial.PREDICATE) || uri.equals(GeoSpatial.CONTEXT)) {
            assertObjectIsURI(sp);
         } else if (uri.equals(GeoSpatial.LOCATION_VALUE) ||  uri.equals(GeoSpatial.TIME_VALUE) 
                  || uri.equals(GeoSpatial.LOCATION_AND_TIME_VALUE)) {
            
            assertObjectIsVariable(sp);
            
         } else {
            
            assertObjectIsLiteralOrVariable(sp);
            
         }
      }

      if (!uris.contains(GeoSpatial.SEARCH)) {
         throw new RuntimeException("Required search predicate not found: "
               + GeoSpatial.SEARCH + " for searchVar=" + searchVar);
      }
      
      if (!uris.contains(GeoSpatial.PREDICATE)) {
         throw new RuntimeException(
               "GeoSpatial search with unbound predicate is currenly not supported.");
      }

   }

   private void assertObjectIsURI(final StatementPatternNode sp) {

      final TermNode o = sp.o();

      if (o instanceof URI) {

         throw new IllegalArgumentException(
               "Object is not a URI: " + sp);

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
         throw new IllegalArgumentException(
               "Object is not a variable: " + sp);
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
      private final IVariable<?> searchVar;
      private final IVariable<?> locationVar;
      private final IVariable<?> timeVar;
      private final IVariable<?> locationAndTimeVar;
      private final TermNode predicate;
      private final TermNode context;
      private final TermNode spatialCircleCenter;
      private final TermNode spatialCircleRadius;
      private final TermNode spatialRectangleSouthWest;
      private final TermNode spatialRectangleNorthEast;
      private final TermNode spatialUnit;
      private final TermNode timeStart;
      private final TermNode timeEnd;

      private IVariable<?>[] vars;
      private final GeoSpatialDefaults defaults;
      private final AbstractTripleStore kb;
      private final GeoSpatialCounters geoSpatialCounters;
      
      private final int numTasks;
      private final int minDatapointsPerTask;      
      private final int threadLocalBufferCapacity;
      private final int globalBufferChunkOfChunksCapacity;
      
      private final BaseJoinStats stats;
      
      /**
       * The service used for executing subtasks (optional).
       * 
       * @see #maxParallelChunks
       */
      final private Executor executor;
      
      
      public GeoSpatialServiceCall(final IVariable<?> searchVar,
            final Map<URI, StatementPatternNode> statementPatterns,
            final IServiceOptions serviceOptions,
            final GeoSpatialDefaults dflts, final AbstractTripleStore kb,
            final int maxParallel, final int numTasks,
            final int minDatapointsPerTask,
            final int threadLocalBufferCapacity, 
            final int globalBufferChunkOfChunksCapacity,
            final BaseJoinStats stats) {

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
         
         this.searchVar = searchVar;

         /*
          * Unpack the "search" magic predicate:
          * 
          * [?searchVar bd:search objValue]
          */
         final StatementPatternNode sp = statementPatterns
               .get(GeoSpatial.SEARCH);

         searchFunction = sp.o();

         TermNode predicate = null;
         TermNode context = null;
         TermNode spatialCircleCenter = null;
         TermNode spatialCircleRadius = null;
         TermNode spatialRectangleSouthWest = null;
         TermNode spatialRectangleNorthEast = null;
         TermNode spatialUnit = null;
         TermNode timeStart = null;
         TermNode timeEnd = null;
         IVariable<?> locationVar = null;
         IVariable<?> timeVar = null;
         IVariable<?> locationAndTimeVar = null;

         for (StatementPatternNode meta : statementPatterns.values()) {

            final URI p = (URI) meta.p().getValue();
            
            final IVariable<?> oVar = 
               meta.o().isVariable() ? 
               (IVariable<?>) meta.o().getValueExpression() : null;

            if (GeoSpatial.PREDICATE.equals(p)) {
               predicate = meta.o();
            } else if (GeoSpatial.CONTEXT.equals(p)) {
            	context = meta.o();
            } else if (GeoSpatial.SPATIAL_CIRCLE_CENTER.equals(p)) {
               spatialCircleCenter = meta.o();
            } else if (GeoSpatial.SPATIAL_CIRCLE_RADIUS.equals(p)) {
               spatialCircleRadius = meta.o();
            } else if (GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST.equals(p)) {
               spatialRectangleSouthWest = meta.o();
            } else if (GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST.equals(p)) {
               spatialRectangleNorthEast = meta.o();
            } else if (GeoSpatial.SPATIAL_UNIT.equals(p)) {
               spatialUnit = meta.o();
            } else if (GeoSpatial.TIME_START.equals(p)) {
               timeStart = meta.o();
            } else if (GeoSpatial.TIME_END.equals(p)) {
               timeEnd = meta.o();
            } else if (GeoSpatial.LOCATION_VALUE.equals(p)) {
               locationVar = oVar;
            } else if (GeoSpatial.TIME_VALUE.equals(p)) {
               timeVar = oVar;
            } else if (GeoSpatial.LOCATION_AND_TIME_VALUE.equals(p)) {
               locationAndTimeVar = oVar;
            } 

         }

         // for now: a single variable containing the result
         this.vars = new IVariable[] { searchVar };

         this.predicate = predicate;
         this.context = context;
         this.spatialCircleCenter = spatialCircleCenter;
         this.spatialCircleRadius = spatialCircleRadius;
         this.spatialRectangleSouthWest = spatialRectangleSouthWest;
         this.spatialRectangleNorthEast = spatialRectangleNorthEast;
         this.spatialUnit = spatialUnit;
         this.timeStart = timeStart;
         this.timeEnd = timeEnd;
         this.locationVar = locationVar;
         this.timeVar = timeVar;
         this.locationAndTimeVar = locationAndTimeVar;
         this.kb = kb;
         
         geoSpatialCounters =
            QueryEngineFactory.getInstance().getQueryController(
               kb.getIndexManager()).getGeoSpatialCounters();
            
         this.numTasks = numTasks;
         this.minDatapointsPerTask = minDatapointsPerTask;         
         this.threadLocalBufferCapacity = threadLocalBufferCapacity;
         this.globalBufferChunkOfChunksCapacity = globalBufferChunkOfChunksCapacity;
         
         if (log.isDebugEnabled()) {
             log.debug("Number of threads used for execution: " + maxParallel);
         }

         // set up executor service (not using any if no parallel access desired)
         executor = maxParallel<=1 ? 
            null : new LatchedExecutor(kb.getIndexManager().getExecutorService(), maxParallel);
         
         this.stats = stats;

      }

      @Override
      public ICloseableIterator<IBindingSet> call(final IBindingSet[] incomingBs) {

         // iterate over the incoming binding set, issuing search requests
         // for all bindings in the binding set
         return new GeoSpatialInputBindingsIterator(incomingBs, searchFunction,
               searchVar, predicate, context, spatialCircleCenter, spatialCircleRadius,
               spatialRectangleSouthWest, spatialRectangleNorthEast, spatialUnit,
               timeStart, timeEnd, locationVar, timeVar, locationAndTimeVar, defaults, kb, this);

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
      public ICloseableIterator<IBindingSet> search(
            final GeoSpatialQuery query, final AbstractTripleStore kb) {

         final BOpContextBase context = new BOpContextBase(
            QueryEngineFactory.getInstance().getQueryController(kb.getIndexManager()));
         
         geoSpatialCounters.registerGeoSpatialSearchRequest();

         final GlobalAnnotations globals = new GlobalAnnotations(kb
               .getLexiconRelation().getNamespace(), kb.getSPORelation()
               .getTimestamp());
         
         final BigdataValueFactory vf = kb.getValueFactory();

         final BlockingBuffer<IBindingSet[]> buffer = 
            new BlockingBuffer<IBindingSet[]>(globalBufferChunkOfChunksCapacity);

         final FutureTask<Void> ft = 
            new FutureTask<Void>(new GeoSpatialServiceCallTask(
               buffer, query.normalize(), kb, vars, context, globals, vf, geoSpatialCounters, 
               executor, numTasks, minDatapointsPerTask, threadLocalBufferCapacity, stats));
         
         buffer.setFuture(ft); // set the future on the buffer
         kb.getIndexManager().getExecutorService().submit(ft);
         
         return new ChunkConsumerIterator<IBindingSet>(buffer.iterator());

      }
      
      private static Var<?> varFromIVar(IVariable<?> iVar) {
         
         return iVar==null ? null : Var.var(iVar.getName());
         
      }
      
      /**
       * A single serice call request, which may handle a list of 
       * {@link GeoSpatialQuery}s.
       * 
       * @author msc
       */
      private static class GeoSpatialServiceCallTask 
         extends Haltable<Void> implements Callable<Void> {

         final BlockingBuffer<IBindingSet[]> buffer;
         
         final private Executor executor;
         
         final private List<IGeoSpatialQuery> queries;
         final private AbstractTripleStore kb;
         final private IVariable<?>[] vars;
         
         final private GeoSpatialCounters geoSpatialCounters;
         
         final private BOpContextBase context;
         final private GlobalAnnotations globals;
         final private BigdataValueFactory vf;
         
         final private  GeoSpatialLiteralExtension<BigdataValue> litExt;
         
         private final int numTasks;
         private final int minDatapointsPerTask;
         private final int threadLocalBufferCapacity;

         private final BaseJoinStats stats;
         
         /**
          * The list of tasks to execute. Execution of these tasks is carried out
          * in parallel if an executor with parallel execution configuration turned
          * on is provided. Otherwise, the tasks are processed one by one
          */
         final private List<GeoSpatialServiceCallSubRangeTask> tasks;
         
         /**
          * Constructor creating a {@link GeoSpatialServiceCallTask}. Expects
          * a list of normalized queries as input, see {@link IGeoSpatialQuery#isNormalized()}.
          */
         public GeoSpatialServiceCallTask(
            final BlockingBuffer<IBindingSet[]> buffer, 
            final List<IGeoSpatialQuery> queries,
            final AbstractTripleStore kb, final IVariable<?>[] vars,
            final BOpContextBase context,
            final GlobalAnnotations globals, final BigdataValueFactory vf,
            final GeoSpatialCounters geoSpatialCounters, final Executor executor,
            final int numTasks, final int minDatapointsPerTask, 
            final int threadLocalBufferCapacity, final BaseJoinStats stats) {
            
            this.buffer = buffer;
            this.queries = queries;
            this.kb = kb;
            this.vars = vars;
            this.context = context;
            this.globals = globals;
            this.vf = vf;
            
            this.executor = executor;
            this.geoSpatialCounters = geoSpatialCounters;

            // for use in this thread only
            this.litExt = new GeoSpatialLiteralExtension<BigdataValue>(kb.getLexiconRelation());

            this.numTasks = numTasks;
            this.minDatapointsPerTask = minDatapointsPerTask;
            this.threadLocalBufferCapacity = threadLocalBufferCapacity;
            
            this.stats = stats;
            
            tasks = getSubTasks();
            
            // set stats
            geoSpatialCounters.registerGeoSpatialServiceCallTask();
            geoSpatialCounters.registerGeoSpatialServiceCallSubRangeTasks(tasks.size());

         }
         
         
         /**
          * Decomposes the context path into subtasks according to the configuration.
          * Each subtasks is a range scan backed by the buffer.
          */
         @SuppressWarnings("rawtypes")
         protected List<GeoSpatialServiceCallSubRangeTask> getSubTasks() {
                          
            final List<GeoSpatialServiceCallSubRangeTask> subTasks =
               new LinkedList<GeoSpatialServiceCallSubRangeTask>();

            for (IGeoSpatialQuery query : queries) {

                // queries must be normalized at this point
                if (!query.isNormalized()) {
                    throw new IllegalArgumentException("Expected list of normalized query as input.");
                }
                
                /**
                 * Get the bounding boxes for the subsequent scan
                 */
                final PointLatLonTime southWestWithTime = 
                    query.getBoundingBoxSouthWestWithTime(); 
                final Object[] southWestComponents = 
                        PointLatLonTime.toComponentString(southWestWithTime);
                    
                final PointLatLonTime northEastWithTime =
                    query.getBoundingBoxNorthEastWithTime();
                final Object[] northEastComponents =
                    PointLatLonTime.toComponentString(northEastWithTime);
    
                /**
                 * We proceed as follows:
                 * 
                 * 1.) Estimate the number of total points in the search range
                 * 2.) Based on the config parameter telling us how many points to process per thread, we obtain the #subtasks
                 * 3.) Knowing the number of subtasks, we split the range into #subtasks equal-length subranges
                 * 4.) For each of these subranges, we set up a subtask to process the subrange
                 */
                final AccessPath<ISPO> accessPath = 
                    getAccessPath(southWestComponents, northEastComponents, query);
                
                if (accessPath==null) // known unsatisfiable, e.g. if predicate unknown
                    continue;
                
                final long totalPointsInRange = accessPath.rangeCount(false/* exact */);
                
                // TODO: rangeCount() returns quite strange results, e.g. for 3.3.1 we get >1M although the range
                // is quite small; need to check what's going on there...
                
                stats.accessPathRangeCount.add(totalPointsInRange);
    
                /**
                 * The number of subtasks is calculated based on two parameters. First, there is a
                 * minumum number of datapoints per task, which is considered a hard limit. This means,
                 * we generate *at most* (totalPointsInRange/minDatapointsPerTask) tasks.
                 * 
                 * The second parameter is the numTasks parameter, which tells us the "desired"
                 * number of tasks. 
                 * 
                 * The number of subranges is then defined as follows:
                 * - If the desired number of tasks is larger than the hard limit, we choose the hard limit
                 * - Otherwise, we choose the desired number of tasks
                 * 
                 * Effectively, this means choosing the smaller of the two values (MIN).
                 */
                final long maxTasksByDatapointRestriction = totalPointsInRange/minDatapointsPerTask;
                final long desiredNumTasks = Math.max(numTasks, 1); /* at least one subrange */
                
                long nrSubRanges = Math.min(maxTasksByDatapointRestriction, desiredNumTasks);
                
                LiteralExtensionIV lowerBorderIV = litExt.createIV(southWestComponents);
                LiteralExtensionIV upperBorderIV = litExt.createIV(northEastComponents);
                
                if (log.isDebugEnabled()) {
                   
                   log.debug("[OuterRange] Scanning from " + lowerBorderIV.getDelegate().integerValue() 
                         + " / " + litExt.toComponentString(0, 2, lowerBorderIV));
                   log.debug("[OuterRange]            to " + upperBorderIV.getDelegate().integerValue()
                         + " / " +  litExt.toComponentString(0, 2, upperBorderIV));
    
                }
                
                // split range into nrSubRanges, equal-length pieces
                final GeoSpatialSubRangePartitioner partitioner = 
                   new GeoSpatialSubRangePartitioner(southWestWithTime, northEastWithTime, nrSubRanges, litExt);
                
                // set up tasks for partitions
                final SPOKeyOrder keyOrder = (SPOKeyOrder)accessPath.getKeyOrder(); // will be the same for all partitions
                final int subjectPos = keyOrder.getPositionInIndex(SPOKeyOrder.S);
                final int objectPos = keyOrder.getPositionInIndex(SPOKeyOrder.O);
                for (GeoSpatialSubRangePartition partition : partitioner.getPartitions()) {
                   
                   // set up a subtask for the partition
                   final GeoSpatialServiceCallSubRangeTask subTask = 
                      getSubTask(southWestWithTime, northEastWithTime, 
                         partition.lowerBorder, partition.upperBorder, 
                         keyOrder, subjectPos, objectPos, stats, query);
                   
                   if (subTask!=null) { // if satisfiable
                      subTasks.add(subTask);
                   }
                   
                   if (log.isDebugEnabled()) {
    
                      final Object[] lowerBorderComponentsPart = 
                         PointLatLonTime.toComponentString(partition.lowerBorder);
                      final Object[] upperBorderComponentsPart = 
                         PointLatLonTime.toComponentString(partition.upperBorder);
                      
                      LiteralExtensionIV lowerBorderIVPart = litExt.createIV(lowerBorderComponentsPart);
                      LiteralExtensionIV upperBorderIVPart = litExt.createIV(upperBorderComponentsPart);
    
                      log.debug("[InnerRange] Scanning from " + lowerBorderIVPart.getDelegate().integerValue() 
                            + " / " + litExt.toComponentString(0, 2, lowerBorderIVPart) 
                            + " / " + BytesUtil.byteArrToBinaryStr(litExt.toZOrderByteArray(lowerBorderComponentsPart)));
                      log.debug("[InnerRange]            to " + upperBorderIVPart.getDelegate().integerValue() 
                            + " / " +  litExt.toComponentString(0, 2, upperBorderIVPart) 
                            + " / " + BytesUtil.byteArrToBinaryStr(litExt.toZOrderByteArray(upperBorderComponentsPart)));
                   }
    
                }
                
            }

            return subTasks;

         }


         /**
          * Sets up a subtask for the given configuration. The method may return null
          * if it can be statically shown that the subtask produces no result, i.e. if
          * it is trivially not satisfiable. It gets as input information about the outer
          * range (i.e., the search rectangle surrounding *all* subtasks) and the 
          * range for the given subtask, plus some additional information about key order,
          * subject, and object position.
          * 
          * @param outerRangeUpperLeftWithTime the upper left geospatial+time point of the outer range
          * @param outerRangeLowerRightWithTime the lower right geospatial+time point of the outer range 
          * @param subRangeUpperLeftWithTime the upper left geospatial+time point of sub range covered by the task
          * @param subRangeLowerRightWithTime the lower right geospatial+time point of sub range covered by the task
          * @param keyOrder the key order of the underlying access path
          * @param subjectPos the position of the subject in the key
          * @param objectPos the position of the object in the key
          * 
          * @return the subtask or null 
          */
         protected GeoSpatialServiceCallSubRangeTask getSubTask(
            final PointLatLonTime outerRangeUpperLeftWithTime,    /* upper left of outer (non subtask) range */
            final PointLatLonTime outerRangeLowerRightWithTime,   /* lower right of outer (non subtask) range */
            final PointLatLonTime subRangeUpperLeftWithTime,      /* upper left of the subtask */
            final PointLatLonTime subRangeLowerRightWithTime,     /* lower right of the subtask */
            final SPOKeyOrder keyOrder, final int subjectPos, 
            final int objectPos, final BaseJoinStats stats, 
            final IGeoSpatialQuery query) {
            
            /**
             * Compose the surrounding filter. The filter is based on the outer range.
             */
            final GeoSpatialFilterBase filter;
            switch (query.getSearchFunction()) {
            case IN_CIRCLE: 
               {
                  filter = new GeoSpatialInCircleFilter(
                     query.getSpatialCircleCenter(),  query.getSpatialCircleRadius(), query.getSpatialUnit(), 
                     outerRangeUpperLeftWithTime.getTimestamp(), outerRangeLowerRightWithTime.getTimestamp(), 
                     new GeoSpatialLiteralExtension<BigdataValue>(kb.getLexiconRelation()), geoSpatialCounters);
                  
               }
               break;

            case IN_RECTANGLE: 
               {
                  filter = new GeoSpatialInRectangleFilter(
                     outerRangeUpperLeftWithTime, outerRangeLowerRightWithTime,
                     new GeoSpatialLiteralExtension<BigdataValue>(kb.getLexiconRelation()), geoSpatialCounters);
                  
               }
               break;

            default:
               throw new RuntimeException("Unknown geospatial search function.");
            }

            filter.setObjectPos(objectPos); // position of the object in the index

            /**
             * If the context is provided, we would need a key order such as
             * PCOS or CPOS, but unfortunately these key order are not available.
             * As a "workaround", we do not pass in the context into the predicate,
             * but instead set an additional context check in the filter. 
             * 
             * This is, of course, not a good strategy when the context is very
             * selective and the index access is not. We could do more sophisticated
             * stuff here, but for now let's try with this solution.
             */
            final TermNode ctxTermNode = query.getContext(); 
            if (ctxTermNode!=null) {
               
               final BigdataValue ctx = 
                  ctxTermNode==null ? null : ctxTermNode.getValue();
               if (ctx!=null && !(ctx instanceof BigdataURI)) {
                  throw new IllegalArgumentException(
                     "Context in GeoSpatial search must be a URI");
               }  

               // register context check in the filter
               filter.addContextCheck(
                  keyOrder.getPositionInIndex(SPOKeyOrder.C), (BigdataURI)ctx);
               
            }

            // decompose sub range borders into components
            final Object[] subRangeUpperLeftComponents = 
               PointLatLonTime.toComponentString(subRangeUpperLeftWithTime);
            final Object[] subRangeLowerRightComponents = 
               PointLatLonTime.toComponentString(subRangeLowerRightWithTime);

            // get the access path for the sub range
            final AccessPath<ISPO> accessPath = 
               getAccessPath(subRangeUpperLeftComponents, subRangeLowerRightComponents, query);
            
            if (accessPath==null) {
               return null;
            }

            // set up a big min advancer for efficient extraction of relevant values from access path
            final byte[] lowerZOrderKey = litExt.toZOrderByteArray(subRangeUpperLeftComponents);
            final byte[] upperZOrderKey = litExt.toZOrderByteArray(subRangeLowerRightComponents);
            
            final Advancer<SPO> bigMinAdvancer = 
               new ZOrderIndexBigMinAdvancer(
                  lowerZOrderKey, upperZOrderKey, litExt, objectPos, geoSpatialCounters);

            
            // set up a value resolver
            final Var<?> locationVar = varFromIVar(query.getLocationVar());
            final Var<?> timeVar = varFromIVar(query.getTimeVar());
            final Var<?> locationAndTimeVar = varFromIVar(query.getLocationAndTimeVar());
            
            final Var<?> var = Var.var(vars[0].getName());
            final IBindingSet incomingBindingSet = query.getIncomingBindings();

            final GeoSpatialServiceCallResolver resolver = 
                  new GeoSpatialServiceCallResolver(var, incomingBindingSet, locationVar,
                        timeVar, locationAndTimeVar, subjectPos, objectPos, vf, 
                        new GeoSpatialLiteralExtension<BigdataValue>(kb.getLexiconRelation()));
            
            // and construct the sub range task
            return new GeoSpatialServiceCallSubRangeTask(
               buffer, accessPath, bigMinAdvancer, filter, resolver, threadLocalBufferCapacity, stats);
         }


         @SuppressWarnings({ "unchecked", "rawtypes" })
         protected AccessPath<ISPO> getAccessPath(
            final Object[] lowerBorderComponents, final Object[] upperBorderComponents,
            final IGeoSpatialQuery query) {

            // set up range scan
            final Var oVar = Var.var(); // object position variable
            final RangeNode range = new RangeNode(new VarNode(oVar),
                  new ConstantNode(litExt.createIV(lowerBorderComponents)),
                  new ConstantNode(litExt.createIV(upperBorderComponents)));

            final RangeBOp rangeBop = ASTRangeOptimizer.toRangeBOp(context, range, globals);
            
            // we support projection of fixed subjects into the SERVICE call
            IConstant<?> constSubject = query.getSubject();
            
            // set up the predicate
            final TermNode s = constSubject==null ? 
               new VarNode(vars[0].getName()) : new ConstantNode((IConstant<IV>)constSubject);
            final TermNode p = query.getPredicate();
            final VarNode o = new VarNode(oVar);

            /**
             * We call kb.getPredicate(), which has the nice feature that it
             * returns null if the predicate is unsatisfiable (i.e., if the
             * predicate does not appear in the data). This gives us an early
             * exit point for the service (see null check below).
             */
            IPredicate<ISPO> pred = (IPredicate<ISPO>) kb.getPredicate(
                  (URI) s.getValue(), /* subject */
                  p==null ? null : (URI)p.getValue(), /* predicate */
                  o.getValue(), /* object */
                  null, /* context */
                  null, /* filter */
                  rangeBop); /* rangeBop */
           
            if (pred == null) {
               return null;
            }

            pred = (IPredicate<ISPO>) pred.setProperty(
                  IPredicate.Annotations.TIMESTAMP, kb.getSPORelation().getTimestamp());

            final IRelation<ISPO> relation = context.getRelation(pred);

            final AccessPath<ISPO> accessPath = (AccessPath<ISPO>) context
                  .getAccessPath(relation, pred);

            return accessPath;
            
         }



         
         @Override
         public Void call() throws Exception {

            if (log.isDebugEnabled()) {
                log.debug("Number of service call tasks to execute: " + tasks.size());
            }
             
            // if there's no executor specified or only one subtask, we run the task in process
            if (executor == null || tasks.size()==1) {

               /*
                * No Executor, so run each task in the caller's thread.
                */

               for (GeoSpatialServiceCallSubRangeTask task : tasks) {

                  task.call();
                  
               }

               buffer.flush();
               buffer.close();
               return null;

            }
            

            /*
             * Build list of FutureTasks. This list is used to check all
             * tasks for errors and ensure that any running tasks are
             * cancelled.
             */
            final List<FutureTask<Void>> futureTasks = new LinkedList<FutureTask<Void>>();

            for (GeoSpatialServiceCallSubRangeTask task : tasks) {

               final FutureTask<Void> ft =  new FutureTask<Void>(task);

               futureTasks.add(ft);

            }

            try {

               /*
                * Execute all tasks.
                */
               for (FutureTask<Void> ft : futureTasks) {

                  halted();

                  // Queue for execution.
                  executor.execute(ft);

               } // next task.

               /*
                * Wait for each task. If any task throws an exception, then
                * [halt] will become true and any running tasks will error
                * out quickly. Once [halt := true], we do not wait for any
                * more tasks, but proceed to cancel all tasks in the
                * finally {} clause below.
                */
               for (FutureTask<Void> ft : futureTasks) {

                  // Wait for a task.
                  if (!isDone())
                     ft.get();

               }

            } finally {

               /*
                * Ensure that all tasks are cancelled, regardless of
                * whether they were started or have already finished.
                */
               for (FutureTask<Void> ft : futureTasks) {

                  ft.cancel(true/* mayInterruptIfRunning */);

               }

            }

            buffer.flush();
            buffer.close();
            return null;
         }
         
         
         private static class GeoSpatialServiceCallSubRangeTask implements Callable<Void> {

            final private UnsynchronizedArrayBuffer<IBindingSet> localBuffer;
            
            final private AccessPath<ISPO> accessPath;
            
            final private Advancer<SPO> bigMinAdvancer;
            
            final private GeoSpatialFilterBase filter;
            
            final private GeoSpatialServiceCallResolver resolver;
            
            final private BaseJoinStats stats;
            
            public GeoSpatialServiceCallSubRangeTask(
               final BlockingBuffer<IBindingSet[]> backingBuffer,
               final AccessPath<ISPO> accessPath, Advancer<SPO> bigMinAdvancer,
               final GeoSpatialFilterBase filter, final GeoSpatialServiceCallResolver resolver,
               final int threadLocalBufferCapacity, final BaseJoinStats stats) {

               // create a local buffer linked to the backing, thread safe blocking buffer
               localBuffer = 
                  new UnsynchronizedArrayBuffer<IBindingSet>(
                     backingBuffer, IBindingSet.class, threadLocalBufferCapacity);

               this.accessPath = accessPath;
               this.bigMinAdvancer = bigMinAdvancer;
               this.filter = filter;
               this.resolver = resolver;
               this.stats = stats;
            }

            @SuppressWarnings("unchecked")
            @Override
            public Void call() throws Exception {
               
               
               final Iterator<IBindingSet> itr = 
                     new Striterator(accessPath.getIndex().rangeIterator(
                        accessPath.getFromKey(), accessPath.getToKey(), 0/* capacity */, 
                        IRangeQuery.KEYS | IRangeQuery.CURSOR, bigMinAdvancer))
                           .addFilter(filter)
                           .addFilter(resolver);

               // consume and flush the buffer
               stats.accessPathCount.increment();

               while (itr.hasNext()) {
                  stats.accessPathUnitsIn.increment();
                  localBuffer.add(itr.next());
               }
               localBuffer.flush();
               
               return null;
            }
            
         }
         
         /**
          * Class providing functionality to partition a geospatial search range.
          * 
          * @author msc
          */
         public static class GeoSpatialSubRangePartitioner {
          
            private List<GeoSpatialSubRangePartition> partitions;
            
            public GeoSpatialSubRangePartitioner(
              final PointLatLonTime lowerBorder, final PointLatLonTime upperBorder,
              final long numPartitions, GeoSpatialLiteralExtension<BigdataValue> litExt) {

               
               
               partitions = new ArrayList<GeoSpatialSubRangePartition>();

               final long lowerTimestamp = lowerBorder.getTimestamp();
               final long upperTimestamp = upperBorder.getTimestamp();

               final long numPartitionsSafe = Math.max(1, numPartitions); // at least 1 partition
               
               final long diff = upperTimestamp - lowerTimestamp;
               final long dist = diff/numPartitionsSafe;

               final List<Long> breakPoints = new ArrayList<Long>();
                  

               long lastConsidered = -1;
               breakPoints.add(lowerTimestamp-1); // first point

               // points in-between (ignoring first and last)
               for (long i=1; i<numPartitionsSafe; i++) {
                  
                  long breakPoint = lowerTimestamp + i*dist;
                  if (lastConsidered==breakPoint) {
                     break;
                  }
                  
                  if (breakPoint>lowerTimestamp && breakPoint<upperTimestamp) {
                     breakPoints.add(breakPoint);
                  }
                  
                  lastConsidered = breakPoint;
               }
               
               breakPoints.add(upperTimestamp); // last point

               for (int i=0; i<breakPoints.size()-1; i++) {
                  partitions.add(
                     new GeoSpatialSubRangePartition(
                        new PointLatLonTime(lowerBorder.getLat(), lowerBorder.getLon(), breakPoints.get(i)+1),
                        new PointLatLonTime(upperBorder.getLat(), upperBorder.getLon(), breakPoints.get(i+1))));
               }
            }
            
            public List<GeoSpatialSubRangePartition> getPartitions() {
               return partitions;
            }

         }
         
         
         public static class GeoSpatialSubRangePartition {
            
            final PointLatLonTime lowerBorder;
            final PointLatLonTime upperBorder;
            
            public GeoSpatialSubRangePartition(
               final PointLatLonTime lowerBorder, final PointLatLonTime upperBorder) {
               
               this.lowerBorder = lowerBorder;
               this.upperBorder = upperBorder;
            }
            
            @Override
            public String toString() {
               
               StringBuffer buf = new StringBuffer();
               buf.append("lowerBorder=");
               buf.append(lowerBorder.toString());
               buf.append(", upperBorder=");
               buf.append(upperBorder.toString());
               
               return buf.toString();
            }
         }
         
         
      }
      
      private static class GeoSpatialServiceCallResolver extends Resolver {

         private static final long serialVersionUID = 1L;

         final private Var<?> var;
         final private IBindingSet incomingBindingSet;
         final private Var<?> locationVar;
         final private Var<?> timeVar;
         final private Var<?> locationAndTimeVar;
         final private int subjectPos;
         final private int objectPos;
         final private BigdataValueFactory vf;
         final private GeoSpatialLiteralExtension<BigdataValue> litExt;
         
         // true if the resolver reports any components from the object literal
         final boolean reportsObjectComponents;
         
         // the position up to which we need to extract IVs
         final private int extractToPosition;
         
         public GeoSpatialServiceCallResolver(final Var<?> var, final IBindingSet incomingBindingSet,
            final Var<?> locationVar, final Var<?> timeVar, final Var<?> locationAndTimeVar, 
            final int subjectPos, final int objectPos, final BigdataValueFactory vf,
            final GeoSpatialLiteralExtension<BigdataValue> litExt) {
            
            this.var = var;
            this.incomingBindingSet = incomingBindingSet;
            this.locationVar = locationVar;
            this.timeVar = timeVar;
            this.locationAndTimeVar = locationAndTimeVar;
            this.subjectPos = subjectPos;
            this.objectPos = objectPos;
            this.vf = vf;
            this.litExt = litExt;
            
            reportsObjectComponents =
               locationVar!=null || timeVar!=null || locationAndTimeVar!=null;
            
            extractToPosition =
               reportsObjectComponents ? 
               Math.max(objectPos, subjectPos) + 1: 
               subjectPos + 1;
            
         }
              
         /**
           * Resolve tuple to IV.
           */
         @SuppressWarnings("rawtypes")
         @Override
         protected IBindingSet resolve(final Object obj) {

            final byte[] key = ((ITuple<?>) obj).getKey();


            // if results are reported, we need to decode up to subject + object,
            // otherwise decoding up to the subject position is sufficient

            final IV[] ivs = IVUtility.decode(key, extractToPosition);

            final IBindingSet bs = incomingBindingSet.clone();
            bs.set(var, new Constant<IV>(ivs[subjectPos]));

            // handle request for return of index components
            if (reportsObjectComponents) {
               
               if (locationVar!=null) {
                  
                  // wrap positions 0 + 1 (lat + lon) into a literal
                  final BigdataLiteral locationLit = 
                     vf.createLiteral(
                        litExt.toComponentString(0,1,(LiteralExtensionIV)ivs[objectPos]));
                  
                  bs.set(locationVar, 
                     new Constant<IV>(DummyConstantNode.toDummyIV((BigdataValue) locationLit)));
               }

               if (timeVar!=null) {
                  
                  // wrap positions 2 of the index into a literal
                  final BigdataLiteral timeLit = 
                        vf.createLiteral(
                           Long.valueOf(litExt.toComponentString(2,2,(LiteralExtensionIV)ivs[objectPos])));
                  
                  bs.set(timeVar, 
                        new Constant<IV>(DummyConstantNode.toDummyIV((BigdataValue) timeLit)));
               }

               if (locationAndTimeVar!=null) {
                  bs.set(locationAndTimeVar, new Constant<IV>(ivs[objectPos]));
               }

            }
            
            return bs;

         }
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
      private final IVariable<?> searchVar;
      private final TermNode predicate;
      private final TermNode context;
      private final TermNode spatialCircleCenter;
      private final TermNode spatialCircleRadius;
      private final TermNode spatialRectangleUpperLeft;
      private final TermNode spatialRectangleLowerRight;
      private final TermNode spatialUnit;
      private final TermNode timeStart;
      private final TermNode timeEnd;
      private final IVariable<?> locationVar;
      private final IVariable<?> timeVar;
      private final IVariable<?> locationAndTimeVar;
      

      final GeoSpatialDefaults defaults;
      final AbstractTripleStore kb;
      final GeoSpatialServiceCall serviceCall;

      int nextBindingSetItr = 0;

      ICloseableIterator<IBindingSet> curDelegate;

      public GeoSpatialInputBindingsIterator(final IBindingSet[] bindingSet,
            final TermNode searchFunction, IVariable<?> searchVar,
            final TermNode predicate, final TermNode context, 
            final TermNode spatialCircleCenter, final TermNode spatialCircleRadius,
            final TermNode spatialRectangleUpperLeft,
            final TermNode spatialRectangleLowerRight,
            final TermNode spatialUnit, final TermNode timeStart,
            final TermNode timeEnd, final IVariable<?> locationVar,
            final IVariable<?> timeVar, final IVariable<?> locationAndTimeVar,
            final GeoSpatialDefaults defaults, final AbstractTripleStore kb,
            final GeoSpatialServiceCall serviceCall) {

         this.bindingSet = bindingSet;
         this.searchFunction = searchFunction;
         this.searchVar = searchVar;
         this.predicate = predicate;
         this.context = context;
         this.spatialCircleCenter = spatialCircleCenter;
         this.spatialCircleRadius = spatialCircleRadius;
         this.spatialRectangleUpperLeft = spatialRectangleUpperLeft;
         this.spatialRectangleLowerRight = spatialRectangleLowerRight;
         this.spatialUnit = spatialUnit;
         this.timeStart = timeStart;
         this.timeEnd = timeEnd;
         this.locationVar = locationVar;
         this.timeVar = timeVar;
         this.locationAndTimeVar = locationAndTimeVar;
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

         GeoSpatialQuery sq = new GeoSpatialQuery(searchFunction,
               bs.get(searchVar), predicate, context, spatialCircleCenter, 
               spatialCircleRadius, spatialRectangleUpperLeft, 
               spatialRectangleLowerRight, spatialUnit, timeStart, timeEnd, 
               locationVar, timeVar, locationAndTimeVar, bs);

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

      // position of the constrained context in the tuple (only used in
      // quads mode if geo:context predicate is used)
      Integer contextPos = null;
      
      // value of the constrained context (only used in quads mode if 
      // geo:context predicate is used)
      BigdataURI context = null;
      
      // counters objects
      GeoSpatialCounters geoSpatialCounters;
      
      public GeoSpatialFilterBase(
         final GeoSpatialLiteralExtension<BigdataValue> litExt,
         final GeoSpatialCounters geoSpatialCounters) {
         
         this.litExt = litExt;
         this.geoSpatialCounters = geoSpatialCounters;
      }      
      
      // sets member variables that imply an additional context check
      public void addContextCheck(
         final Integer contextPos, final BigdataURI context) {
         
         this.contextPos = contextPos;
         this.context = context;
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



      @Override
      final protected boolean isValid(final ITuple tuple) {

         final long filterStartTime = System.nanoTime();
         
         // check that the context constraint is satisfied, if any
         if (!contextIsValid(tuple)) {
            return false;
         }
         
         // delegate to subclass
         boolean isValid = isValidInternal(tuple);
         
         final long filterEndTime = System.nanoTime();
         geoSpatialCounters.addFilterCalculationTime(filterEndTime-filterStartTime);
         
         return isValid;
      }
      
      
      /**
       * Check if the context is valid. If no context constraint is
       * imposed (which means, if contextPos or context are null),
       * then the method just returns true.
       */
      private boolean contextIsValid(final ITuple tuple) {
         
         // do not filter if not both the contextPos and context are set
         if (contextPos==null || context==null) {
            return true;
         }
         
         final byte[] key = ((ITuple<?>) tuple).getKey();
         final IV[] ivs = IVUtility.decode(key, contextPos+1);

         final IV contextIV= ivs[contextPos];
         return contextIV.equals(context.getIV());
      }
      
      /**
       * Check if the iv array is valid. To be implemented by subclasses.
       */
      protected abstract boolean isValidInternal(final ITuple tuple);

   }

   /**
    * Filter asserting that a given point lies into a specified square, defined
    * by its lower and upper border, plus time frame.
    */
   public static class GeoSpatialInCircleFilter extends GeoSpatialFilterBase {

      private static final long serialVersionUID = -346928614528045113L;

      final private double spatialPointLat;
      final private double spatialPointLon;      
      final private Double distanceInMeters;

      final private Long timeMin;
      final private Long timeMax;

      public GeoSpatialInCircleFilter(
         final PointLatLon spatialPoint, Double distance,
         final UNITS unit, final Long timeMin, final Long timeMax, 
         final GeoSpatialLiteralExtension<BigdataValue> litExt,
         final GeoSpatialCounters geoSpatialCounters) {
         
         super(litExt, geoSpatialCounters);

         this.spatialPointLat = spatialPoint.getLat();
         this.spatialPointLon = spatialPoint.getLon();
         this.distanceInMeters = CoordinateUtility.unitsToMeters(distance, unit);
         this.timeMin = timeMin;
         this.timeMax = timeMax;         
      }

      @Override
      @SuppressWarnings("rawtypes")      
      protected boolean isValidInternal(final ITuple tuple) {

          // Note: this is possibly called over and over again, so we choose a
          //       low-level implementation to get best performance out of it

         try {
             
            final byte[] key = ((ITuple<?>) tuple).getKey();
            final IV[] ivs = IVUtility.decode(key, objectPos + 1);

            final IV oIV = ivs[objectPos];

            final double lat;
            final double lon;
            final long time;
            if (oIV instanceof LiteralExtensionIV) {
                
               final LiteralExtensionIV lit = (LiteralExtensionIV) oIV;

               long[] longArr = litExt.asLongArray(lit);
               final Object[] components = litExt.longArrAsComponentArr(longArr);

               lat = (double)components[0];
               lon = (double)components[1];
               time = (long)components[2];
               
            } else {
                
                throw new IllegalArgumentException("Invalid IV cannot be cast to LiteralExtensionIV");
                
            }

            return 
               CoordinateUtility.distanceInMeters(lat, spatialPointLat, lon, spatialPointLon) <= distanceInMeters &&
               timeMin <= time && time <= timeMax;
                    
         } catch (Exception e) {
         
             if (log.isInfoEnabled()) {
                log.info(
                   "Something went wrong extracting the object: " + e.getMessage() +
                   "Rejecting unprocessable value.");
             }
             
         }
         
         return false; // exception code path -> reject value

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
         final GeoSpatialLiteralExtension<BigdataValue> litExt,
         final GeoSpatialCounters geoSpatialCounters) {
         
         super(litExt, geoSpatialCounters);
         
         this.lowerBorder = lowerBorder;
         this.upperBorder = upperBorder;
      }
      
      @SuppressWarnings("rawtypes")
      @Override
      protected boolean isValidInternal(final ITuple tuple) {

         // the advancer by design already returns only those values that
         // are in the rectangle, so there's nothing we need to do here
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
               || predicate.equals(GeoSpatial.CONTEXT)
               || predicate.equals(GeoSpatial.SEARCH)
               || predicate.equals(GeoSpatial.SPATIAL_CIRCLE_CENTER)
               || predicate.equals(GeoSpatial.SPATIAL_CIRCLE_RADIUS)
               || predicate.equals(GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST) 
               || predicate.equals(GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST)
               || predicate.equals(GeoSpatial.SPATIAL_UNIT)
               || predicate.equals(GeoSpatial.TIME_START)
               || predicate.equals(GeoSpatial.TIME_END)
               || predicate.equals(GeoSpatial.TIME_VALUE)
               || predicate.equals(GeoSpatial.LOCATION_VALUE)
               || predicate.equals(GeoSpatial.LOCATION_AND_TIME_VALUE)) {
               
               requiredBound.add((IVariable<?>)object); // the subject var is what we return                  
            }
         }
      }

      return requiredBound;
   }

}
