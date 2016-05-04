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
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.join.PipelineJoin.Annotations;
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
import com.bigdata.service.geospatial.GeoSpatial;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;
import com.bigdata.service.geospatial.GeoSpatialDatatypeFieldConfiguration.ServiceMapping;
import com.bigdata.service.geospatial.GeoSpatialDatatypeFieldConfiguration.ValueType;
import com.bigdata.service.geospatial.GeoSpatialConfig;
import com.bigdata.service.geospatial.GeoSpatialCounters;
import com.bigdata.service.geospatial.GeoSpatialDatatypeConfiguration;
import com.bigdata.service.geospatial.GeoSpatialDatatypeFieldConfiguration;
import com.bigdata.service.geospatial.GeoSpatialSearchException;
import com.bigdata.service.geospatial.IGeoSpatialLiteralSerializer;
import com.bigdata.service.geospatial.IGeoSpatialQuery;
import com.bigdata.service.geospatial.ZOrderIndexBigMinAdvancer;
import com.bigdata.service.geospatial.impl.GeoSpatialQuery;
import com.bigdata.service.geospatial.impl.GeoSpatialUtility.PointLatLon;
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

   final protected static boolean INFO = log.isInfoEnabled();
   final protected static boolean DEBUG = log.isDebugEnabled();
   
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
  
      if (DEBUG) {
         log.debug("maxParallel=" + maxParallel);
         log.debug("numTasksPerThread=" + numTasksPerThread);
         log.debug("threadLocalBufferCapacity=" + threadLocalBufferCapacity);
         log.debug("globalBufferChunkOfChunksCapacity=" + globalBufferChunkOfChunksCapacity);
      }
      
      if (!store.getLexiconRelation().getLexiconConfiguration().isGeoSpatial()) {
          throw new GeoSpatialSearchException(
              "Geospatial is disabled. Please enable geospatial and reload your data.");
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
               throw new RuntimeException("Unknown geospatial magic predicate: " + uri);

            final TermNode s = sp.s();

            if (!s.isVariable())
               throw new RuntimeException(
                     "Subject of geospatial search pattern must not be a constant: " + sp);

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
                  || uri.equals(GeoSpatial.LAT_VALUE) || uri.equals(GeoSpatial.LON_VALUE)
                  || uri.equals(GeoSpatial.LITERAL_VALUE) || uri.equals(GeoSpatial.COORD_SYSTEM_VALUE)
                  || uri.equals(GeoSpatial.CUSTOM_FIELDS_VALUES)
                  || uri.equals(GeoSpatial.LOCATION_AND_TIME_VALUE)) {
            
            assertObjectIsVariable(sp);
            
         } else if (uri.equals(GeoSpatial.SEARCH_DATATYPE)) {

             assertObjectIsUriOrVariable(sp);

         } else {
            
             assertObjectIsLiteralOrVariable(sp);
            
         }
      }
   }

   private void assertObjectIsURI(final StatementPatternNode sp) {

      final TermNode o = sp.o();

      if (o instanceof URI) {

         throw new IllegalArgumentException(
               "Object is not a URI: " + sp);

      }

   }

   private void assertObjectIsUriOrVariable(final StatementPatternNode sp) {

       final TermNode o = sp.o();

       boolean isNotUri= !o.isConstant()
             || !(((ConstantNode) o).getValue() instanceof URI);
       boolean isNotVariable = !o.isVariable();

       if (isNotUri && isNotVariable) {

          throw new IllegalArgumentException(
                "Object is not uri or variable: " + sp);

       }

   }
   
   private void assertObjectIsLiteralOrVariable(final StatementPatternNode sp) {

      final TermNode o = sp.o();

      boolean isNotLiteral = !o.isConstant()
            || !(((ConstantNode) o).getValue() instanceof Literal);
      boolean isNotVariable = !o.isVariable();

      if (isNotLiteral && isNotVariable) {

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
      
      final GeoSpatialServiceCallConfiguration gssConfig;
      
      private IVariable<?>[] vars;
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
            final Map<URI, StatementPatternNode> sps,
            final IServiceOptions serviceOptions,
            final GeoSpatialDefaults dflts, final AbstractTripleStore kb,
            final int maxParallel, final int numTasks,
            final int minDatapointsPerTask,
            final int threadLocalBufferCapacity, 
            final int globalBufferChunkOfChunksCapacity,
            final BaseJoinStats stats) {

         if (searchVar == null)
            throw new IllegalArgumentException();

         if (sps == null)
            throw new IllegalArgumentException();

         if (serviceOptions == null)
            throw new IllegalArgumentException();

         if (kb == null)
            throw new IllegalArgumentException();         

         this.serviceOptions = serviceOptions;

         this.gssConfig = 
             new GeoSpatialServiceCallConfiguration(
                 dflts, kb.getLexiconRelation().getLexiconConfiguration().getGeoSpatialConfig(), 
                 searchVar, sps);

         // for now: a single variable containing the result
         this.vars = new IVariable[] { searchVar };
         this.kb = kb;
         
         geoSpatialCounters =
            QueryEngineFactory.getInstance().getQueryController(
               kb.getIndexManager()).getGeoSpatialCounters();
            
         this.numTasks = numTasks;
         this.minDatapointsPerTask = minDatapointsPerTask;         
         this.threadLocalBufferCapacity = threadLocalBufferCapacity;
         this.globalBufferChunkOfChunksCapacity = globalBufferChunkOfChunksCapacity;
         
         if (DEBUG) {
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
         return new GeoSpatialInputBindingsIterator(incomingBs, gssConfig, kb, this);

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
         final private GeoSpatialConfig geoSpatialConfig;
         
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

            this.numTasks = numTasks;
            this.minDatapointsPerTask = minDatapointsPerTask;
            this.threadLocalBufferCapacity = threadLocalBufferCapacity;
            
            this.stats = stats;
            this.geoSpatialConfig = 
                kb.getLexiconRelation().getLexiconConfiguration().getGeoSpatialConfig();
            
            tasks = getSubTasks();
            
            // set stats
            geoSpatialCounters.registerGeoSpatialServiceCallTask();
            geoSpatialCounters.registerGeoSpatialServiceCallSubRangeTasks(tasks.size());

         }
         
         
         /**
          * Decomposes the context path into subtasks according to the configuration.
          * Each subtasks is a range scan backed by the buffer.
          */
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
                
                final Object[] southWestComponents = query.getLowerAndUpperBound().getLowerBound();
                final Object[] northEastComponents = query.getLowerAndUpperBound().getUpperBound();


                final AccessPath<ISPO> accessPath = 
                    getAccessPath(southWestComponents, northEastComponents, query);
                
                if (accessPath==null) // known unsatisfiable, e.g. if predicate unknown
                    continue;
                
                // estimate the total number of points in the search range
                final long totalPointsInRange = accessPath.rangeCount(false/* exact */);
                
                stats.accessPathRangeCount.add(totalPointsInRange);

                
                // set up datatype configuration for the datatype URI
                final GeoSpatialDatatypeConfiguration datatypeConfig =
                    geoSpatialConfig.getConfigurationForDatatype(query.getSearchDatatype());
                if (datatypeConfig==null) {
                    throw new GeoSpatialSearchException(
                        GeoSpatialSearchException.INVALID_PARAMETER_EXCEPTION + ": search datatype unknown.");
                }
                
                final GeoSpatialLiteralExtension<BigdataValue> litExt = 
                    new GeoSpatialLiteralExtension<BigdataValue>(kb.getLexiconRelation(), datatypeConfig);
                
                
                // this debug code (currently broken) might be re-enabled once needed
//                if (log.isDebugEnabled()) {
//                   
//                    final LiteralExtensionIV lowerBorderIV = litExt.createIV(southWestComponents);
//                    final LiteralExtensionIV upperBorderIV = litExt.createIV(northEastComponents);
//                   log.debug("[OuterRange] Scanning from " + lowerBorderIV.getDelegate().integerValue() 
//                         + " / " + litExt.toComponentString(0, 2, lowerBorderIV));
//                   log.debug("[OuterRange]            to " + upperBorderIV.getDelegate().integerValue()
//                         + " / " +  litExt.toComponentString(0, 2, upperBorderIV));
//    
//                }
                


                
                // set up tasks for partitions
                final SPOKeyOrder keyOrder = (SPOKeyOrder)accessPath.getKeyOrder(); // will be the same for all partitions
                final int subjectPos = keyOrder.getPositionInIndex(SPOKeyOrder.S);
                final int objectPos = keyOrder.getPositionInIndex(SPOKeyOrder.O);
                
                // set up the search range and a partitioner
                final GeoSpatialSearchRange searchRange =
                    new GeoSpatialSearchRange(datatypeConfig, litExt, southWestComponents, northEastComponents);
                final GeoSpatialSearchRangePartitioner partitioner = new GeoSpatialSearchRangePartitioner(searchRange);
                for (GeoSpatialSearchRange partition : partitioner.partition(numTasks, totalPointsInRange, minDatapointsPerTask)) {
                   
                   final Object[] lowerBorder = partition.getLowerBorderComponents();
                   final Object[] upperBorder = partition.getUpperBorderComponents();
                   

                   // set up a subtask for the partition
                   final GeoSpatialServiceCallSubRangeTask subTask = 
                      getSubTask(query, lowerBorder, upperBorder,  keyOrder, subjectPos, objectPos, stats);
                   
                   if (subTask!=null) { // if satisfiable
                      subTasks.add(subTask);
                   }
                   
                   // note: this is old debugging code, which is broken
                   //       -> might be fixed once we need debugging here...
//                   if (log.isDebugEnabled()) {
//    
//                      LiteralExtensionIV lowerBorderIVPart = litExt.createIV(lowerBorder);
//                      LiteralExtensionIV upperBorderIVPart = litExt.createIV(upperBorder);
//    
//                      log.debug("[InnerRange] Scanning from " + lowerBorderIVPart.getDelegate().integerValue() 
//                            + " / " + litExt.toComponentString(0, 2, lowerBorderIVPart) 
//                            + " / " + BytesUtil.byteArrToBinaryStr(litExt.toZOrderByteArray(lowerBorder)));
//                      log.debug("[InnerRange]            to " + upperBorderIVPart.getDelegate().integerValue() 
//                            + " / " +  litExt.toComponentString(0, 2, upperBorderIVPart) 
//                            + " / " + BytesUtil.byteArrToBinaryStr(litExt.toZOrderByteArray(upperBorder)));
//                   }
    
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
            final IGeoSpatialQuery query,
            final Object[] lowerBorder, final Object[] upperBorder,
            final SPOKeyOrder keyOrder, final int subjectPos, 
            final int objectPos, final BaseJoinStats stats) {
            
            /**
             * Compose the surrounding filter. The filter is based on the outer range.
             */
            final GeoSpatialFilterBase filter;

            // set up datatype configuration for the datatype URI
            final GeoSpatialDatatypeConfiguration datatypeConfig = query.getDatatypeConfig();
            final GeoSpatialLiteralExtension<BigdataValue> litExt = 
                new GeoSpatialLiteralExtension<BigdataValue>(kb.getLexiconRelation(), datatypeConfig);
            
            switch (query.getSearchFunction()) {
            case IN_CIRCLE: 
               {
                  // for circle queries, the filter retains those values that are indeed in the
                  // circle (the z-order borders we're using for scanning report values that are
                  // not inside this circle)
                  filter = new GeoSpatialInCircleFilter(
                     query.getSpatialCircleCenter(),  query.getSpatialCircleRadius(), 
                     query.getSpatialUnit(), query.getTimeStart(), query.getTimeEnd(), 
                     new GeoSpatialLiteralExtension<BigdataValue>(kb.getLexiconRelation(), datatypeConfig), geoSpatialCounters);
                  
               }
               break;

               
            case IN_RECTANGLE: 
            case UNDEFINED:
               {
                  // for a rectangle query, the z-order lower and upper border exactly coincide
                  // with what we're looking for, so we set up a dummy filter for that case;
                  // we also use such a dummy filter if the geospatial search function is undefined
                  // (which might be the case if we query an index without latitude and longitude)
                  filter = new AcceptAllSolutionsFilter(
                     new GeoSpatialLiteralExtension<BigdataValue>(kb.getLexiconRelation(), datatypeConfig), geoSpatialCounters);
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

            // get the access path for the sub range
            final AccessPath<ISPO> accessPath = getAccessPath(lowerBorder, upperBorder, query);
            
            if (accessPath==null) {
               return null;
            }

            // set up a big min advancer for efficient extraction of relevant values from access path
            final byte[] lowerZOrderKey = litExt.toZOrderByteArray(lowerBorder);
            final byte[] upperZOrderKey = litExt.toZOrderByteArray(upperBorder);
            
            final Advancer<SPO> bigMinAdvancer = 
               new ZOrderIndexBigMinAdvancer(
                  lowerZOrderKey, upperZOrderKey, litExt, objectPos, geoSpatialCounters);

            
            // set up a value resolver
            final Var<?> locationVar = varFromIVar(query.getLocationVar());
            final Var<?> timeVar = varFromIVar(query.getTimeVar());
            final Var<?> latVar = varFromIVar(query.getLatVar());
            final Var<?> lonVar = varFromIVar(query.getLonVar());
            final Var<?> coordSystemVar = varFromIVar(query.getCoordSystemVar());
            final Var<?> customFieldsVar = varFromIVar(query.getCustomFieldsVar());
            final Var<?> locationAndTimeVar = varFromIVar(query.getLocationAndTimeVar());
            final Var<?> literalVar = varFromIVar(query.getLiteralVar());
            final Var<?> distanceVar = varFromIVar(query.getDistanceVar());
            
            final Var<?> var = Var.var(vars[0].getName());
            final IBindingSet incomingBindingSet = query.getIncomingBindings();
            
            // calculate center point, if any
            final PointLatLon centerPoint = query.getSpatialCircleCenter();
            final CoordinateDD centerPointDD = 
                centerPoint==null ? null : new CoordinateDD(centerPoint.getLat(), centerPoint.getLon());
                   

            final GeoSpatialServiceCallResolver resolver = 
                new GeoSpatialServiceCallResolver(var, incomingBindingSet, locationVar,
                    timeVar, locationAndTimeVar, latVar, lonVar, coordSystemVar, 
                    customFieldsVar, literalVar, distanceVar, subjectPos, objectPos, vf, 
                    new GeoSpatialLiteralExtension<BigdataValue>(kb.getLexiconRelation(), datatypeConfig),
                    query.getCustomFieldsConstraints().keySet(), centerPointDD, query.getSpatialUnit());
            
            // and construct the sub range task
            return new GeoSpatialServiceCallSubRangeTask(
               buffer, accessPath, bigMinAdvancer, filter, resolver, threadLocalBufferCapacity, stats);
         }


         @SuppressWarnings({ "unchecked", "rawtypes" })
         protected AccessPath<ISPO> getAccessPath(
            final Object[] lowerBorderComponents, final Object[] upperBorderComponents,
            final IGeoSpatialQuery query) {

             // set up datatype configuration and literal extension object for the datatype URI
             final GeoSpatialDatatypeConfiguration datatypeConfig = query.getDatatypeConfig();
             final GeoSpatialLiteralExtension litExt = 
                 new GeoSpatialLiteralExtension<BigdataValue>(kb.getLexiconRelation(), datatypeConfig);
             
            // set up range scan
            final Var oVar = Var.var(); // object position variable
            final RangeNode range = new RangeNode(new VarNode(oVar),
                  new ConstantNode(litExt.createIV(lowerBorderComponents)),
                  new ConstantNode(litExt.createIV(upperBorderComponents)));

            final RangeBOp rangeBop = ASTRangeOptimizer.toRangeBOp(context, range, globals);
            
            // we support projection of fixed subjects into the SERVICE call
            final IConstant<?> constSubject = query.getSubject();
            
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

            if (DEBUG) {
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
          * Builds a number of partitions over a given geospatial search range, effectively 
          * splitting up the search range into fully covering smaller search ranges.
          */
         public static class GeoSpatialSearchRangePartitioner {
             
             private final GeoSpatialSearchRange geoSpatialSearchRange;
             
             public GeoSpatialSearchRangePartitioner(final GeoSpatialSearchRange geoSpatialSearchRange) {
                 this.geoSpatialSearchRange = geoSpatialSearchRange;
             }
             
                          
             /**
              * Computes the partitions based on the configuration. For now, we always partition along the
              * last dimension, e.g. for a ternary datatype such as LAT+LON+TIME we would partition on TIME.
              * 
              * The number of partitions is computed by computeNumberOfPartitions() with the given parameters,
              * see the documentation of the latter method for a detailed explanation.
              * 
              * @param numTasks desired number of access path tasks generated per thread in case the range 
              *                 is large enough to be split, see {@link PipelineJoin.Annotations.NUM_TASKS_PER_THREAD}
              * @param minDatapointsPerTask the minimum number of data points assigned to a a given task, which
              *                             essentially should be the threshold on which parallelization starts 
              *                             to pay out, see {@link PipelineJoin.Annotations.MIN_DATAPOINTS_PER_TASK}
              * @param totalPointsInRange the estimated number of total points in the range.
              * 
              * @return the partitions, fully and exactly covering this search range
              * 
              * TODO: what's the rational for choosing the last component? wouldn't the one with the most significant
              *       bit make more sense? would need some experimental validation to figure that out...
              * TODO: the bit shift/precision logics might be encapsulated in methods in the geospatial literal; it
              *       unnecessarily blows up this method here and doesn't really belong here...
              */
             public List<GeoSpatialSearchRange> partition(int numTasks, long totalPointsInRange, int minDatapointsPerTask) {

                 final List<GeoSpatialSearchRange> partitions =  new ArrayList<GeoSpatialSearchRange>();

                 final long numPartitions = computeNumberOfPartitions(numTasks, totalPointsInRange, minDatapointsPerTask);
                 
                 final GeoSpatialDatatypeConfiguration datatypeConfig = geoSpatialSearchRange.getDatatypeConfig();
                 
                 final int numDimensions = datatypeConfig.getNumDimensions();
                 final Object[] lowerBorderComponents = geoSpatialSearchRange.getLowerBorderComponents();
                 final Object[] upperBorderComponents = geoSpatialSearchRange.getUpperBorderComponents();
                 
                 assert (numDimensions == lowerBorderComponents.length && 
                         numDimensions == upperBorderComponents.length);


                 // the component we partition on is the last component in multi-dimensional index
                 final GeoSpatialDatatypeFieldConfiguration fieldToPartitionOn = 
                     datatypeConfig.getFields().get(numDimensions-1);
                 
                 
                 final ValueType vt = fieldToPartitionOn.getValueType();
                 
                 // initialize the value value representing the component on which we split
                 // -> note that we convert to long, applying the precision adjustment
                 final long lowerComponentLongValue;
                 final long upperComponentLongValue;
                 switch (vt) {
                 case LONG:
                 {
                     lowerComponentLongValue = (Long)lowerBorderComponents[numDimensions-1];
                     upperComponentLongValue = (Long)upperBorderComponents[numDimensions-1];
                     
                     break;
                 } 
                 case DOUBLE:
                 {
                     lowerComponentLongValue = 
                         (Double.valueOf((Double)lowerBorderComponents[numDimensions-1]*fieldToPartitionOn.getMultiplier())).longValue();
                     
                     upperComponentLongValue = 
                         (Double.valueOf((Double)upperBorderComponents[numDimensions-1]*fieldToPartitionOn.getMultiplier())).longValue();
                     
                     break;
                 }
                 default:
                     throw new RuntimeException("Unsupported value type: " + vt);
                 }

                     
                 final long diff = upperComponentLongValue - lowerComponentLongValue;

                 final long dist = diff / numPartitions;

                 final List<Long> breakPoints = new ArrayList<Long>();

                 /**
                  * Compute the break points
                  */
                 long lastConsidered = -1;
                 breakPoints.add(lowerComponentLongValue -1); // one value on the left -> will add +1 below
                 
                 // points in-between (ignoring first and last)
                 for (long i = 1; i < numPartitions; i++) {

                     long breakPoint = lowerComponentLongValue + i * dist;
                     if (lastConsidered == breakPoint) {
                         break;
                     }

                     if (breakPoint > lowerComponentLongValue && breakPoint < upperComponentLongValue) {
                         breakPoints.add(breakPoint);
                     }

                     lastConsidered = breakPoint;
                 }
                 breakPoints.add(upperComponentLongValue); // last point


                 /**
                  * Setup partitions for the given breakpoints
                  */
                 final int finalPosition = lowerBorderComponents.length-1;
                 for (int i = 0; i < breakPoints.size() - 1; i++) {
                     
                     final Object[] breakpointLowerBorder = new Object[lowerBorderComponents.length];
                     final Object[] breakpointUpperBorder = new Object[upperBorderComponents.length];
                     
                     for (int j=0; j<lowerBorderComponents.length-1; j++) {
                         breakpointLowerBorder[j] = lowerBorderComponents[j];
                         breakpointUpperBorder[j] = upperBorderComponents[j];
                     }

                     final long startLongVal = breakPoints.get(i) + 1;
                     final long endLongVal = breakPoints.get(i + 1);             
                     
                     
                     switch (vt) {
                     case LONG:
                     {
                         // just copy over value specified by breakpoint
                         breakpointLowerBorder[finalPosition] = startLongVal;
                         breakpointUpperBorder[finalPosition] = endLongVal;
                         
                         break;
                     }
                     case DOUBLE:
                     {
                         // apply precision adjustment
                         breakpointLowerBorder[finalPosition] = 
                             (double)startLongVal/(double)fieldToPartitionOn.getMultiplier();
                         
                         breakpointUpperBorder[finalPosition] = 
                             (double)endLongVal/(double)fieldToPartitionOn.getMultiplier();
                         
                         break;
                     }
                     default:
                         throw new RuntimeException("Unsupported value type: " + vt);
                     }
                     
                     final GeoSpatialSearchRange searchRange = 
                         new GeoSpatialSearchRange(
                             geoSpatialSearchRange.getDatatypeConfig(), geoSpatialSearchRange.getLitExt(), 
                             breakpointLowerBorder, breakpointUpperBorder);
                     
                     partitions.add(searchRange);
                 }
                 
                 return partitions;
             }

                 
             /**
              * The number of partitions is calculated based on two parameters. First, there is a
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
              * Effectively, this means choosing the smaller of the two values (MIN), while
              * making sure that the value returned is >= 1.
              * 
              * @param numTasks desired number of access path tasks generated per thread in case the range 
              *                 is large enough to be split, see {@link PipelineJoin.Annotations.NUM_TASKS_PER_THREAD}
              * @param minDatapointsPerTask the minimum number of data points assigned to a a given task, which
              *                             essentially should be the threshold on which parallelization starts 
              *                             to pay out, see {@link PipelineJoin.Annotations.MIN_DATAPOINTS_PER_TASK}
              * @param totalPointsInRange the estimated number of total points in the range.
              *
              * @return the resulting number of partitions
              **/
             private long computeNumberOfPartitions(int numTasks, long totalPointsInRange, int minDatapointsPerTask) {
                 

                 final long maxTasksByDatapointRestriction = totalPointsInRange/minDatapointsPerTask;
                 final long desiredNumTasks = Math.max(numTasks, 1); /* at least one subrange */
                 
                 long nrSubRanges = Math.min(maxTasksByDatapointRestriction, desiredNumTasks);
                 
                 return Math.max(1, nrSubRanges); // at least one
             }
             
         }
         
         
         public static class GeoSpatialSearchRange {
             
             final GeoSpatialDatatypeConfiguration datatypeConfig;
             final GeoSpatialLiteralExtension<BigdataValue> litExt;
             
             private final Object[] lowerBorderComponents;
             private final Object[] upperBorderComponents;
             
             public GeoSpatialSearchRange(
                 final GeoSpatialDatatypeConfiguration datatypeConfig,
                 final GeoSpatialLiteralExtension<BigdataValue> litExt,
                 final Object[] lowerBorderComponents, final Object[] upperBorderComponents) {
                 
                 this.datatypeConfig = datatypeConfig;
                 this.litExt = litExt;
                 
                 this.lowerBorderComponents = lowerBorderComponents;
                 this.upperBorderComponents = upperBorderComponents;
                 
             }
             
             public GeoSpatialLiteralExtension<BigdataValue> getLitExt() {
                 return litExt;
             }
             
             public GeoSpatialDatatypeConfiguration getDatatypeConfig() {
                 return datatypeConfig;
             }
             
             public Object[] getLowerBorderComponents() {
                 return lowerBorderComponents;
             }

             public Object[] getUpperBorderComponents() {
                 return upperBorderComponents;
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
         final private Var<?> latVar;
         final private Var<?> lonVar;
         final private Var<?> coordSystemVar;
         final private Var<?> customFieldsVar;
         final private Var<?> literalVar;
         final private Var<?> distanceVar;
         
         final private int subjectPos;
         final private int objectPos;
         
         final private int latIdx;
         final private int lonIdx;
         final private int timeIdx;
         final private int coordSystemIdx;
         final private int[] idxsOfCustomFields;
         
         final private CoordinateDD centerPoint;
         final private UNITS unit;
         
         
         final private BigdataValueFactory vf;
         final private GeoSpatialLiteralExtension<BigdataValue> litExt;
         final private IGeoSpatialLiteralSerializer literalSerializer;
         
         // true if the resolver needs to dereference the object, e.g. because
         // the object's value or the distance to the object is reported back
         final boolean requiresObjectDereferencing;
         
         // the position up to which we need to extract IVs
         final private int extractToPosition;
         
         public GeoSpatialServiceCallResolver(final Var<?> var, final IBindingSet incomingBindingSet,
            final Var<?> locationVar, final Var<?> timeVar, final Var<?> locationAndTimeVar, 
            final Var<?> latVar, final Var<?> lonVar, final Var<?> coordSystemVar, 
            final Var<?> customFieldsVar, final Var<?> literalVar, final Var<?> distanceVar,
            final int subjectPos, final int objectPos, final BigdataValueFactory vf, 
            final GeoSpatialLiteralExtension<BigdataValue> litExt, final Set<String> customFieldStrings, 
            final CoordinateDD centerPoint, final UNITS unit) {
            
            this.var = var;
            this.incomingBindingSet = incomingBindingSet;
            this.locationVar = locationVar;
            this.timeVar = timeVar;
            this.locationAndTimeVar = locationAndTimeVar;
            this.latVar = latVar;
            this.lonVar = lonVar;
            this.coordSystemVar = coordSystemVar;
            this.customFieldsVar = customFieldsVar;
            this.literalVar = literalVar;
            this.distanceVar = distanceVar;
            this.subjectPos = subjectPos;
            this.objectPos = objectPos;
            this.vf = vf;
            this.litExt = litExt;
            this.centerPoint = centerPoint;
            this.unit = unit;

            literalSerializer = litExt.getDatatypeConfig().getLiteralSerializer();

            requiresObjectDereferencing =
               locationVar!=null || timeVar!=null || locationAndTimeVar!=null ||coordSystemVar!=null
               ||customFieldsVar!=null || latVar!=null || lonVar!=null || literalVar!=null || distanceVar!=null;
            
            extractToPosition =
               requiresObjectDereferencing ? 
               Math.max(objectPos, subjectPos) + 1: 
               subjectPos + 1;

            final GeoSpatialDatatypeConfiguration datatypeConfig = litExt.getDatatypeConfig();
            latIdx = datatypeConfig.idxOfField(ServiceMapping.LATITUDE);
            lonIdx = datatypeConfig.idxOfField(ServiceMapping.LONGITUDE);
            timeIdx = datatypeConfig.idxOfField(ServiceMapping.TIME);
            coordSystemIdx = datatypeConfig.idxOfField(ServiceMapping.COORD_SYSTEM);
            idxsOfCustomFields = datatypeConfig.idxsOfCustomFields(customFieldStrings);
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
            
            // handle request for binding index components
            if (requiresObjectDereferencing) {
                
                final Object[] componentArr = 
                    litExt.toComponentArray((LiteralExtensionIV)ivs[objectPos]);
               
                if (locationVar!=null) {
                    
                    bs.set(locationVar, 
                       new Constant<IV>(
                           literalSerializer.serializeLocation(
                               vf, componentArr[latIdx], componentArr[lonIdx])));
                }
                
                if (locationAndTimeVar!=null) {
                    
                    bs.set(locationAndTimeVar, 
                       new Constant<IV>(
                           literalSerializer.serializeLocationAndTime(
                               vf, 
                               componentArr[latIdx], 
                               componentArr[lonIdx], 
                               componentArr[timeIdx])));
                }
                
                if (timeVar!=null) {
                    
                    bs.set(timeVar, 
                       new Constant<IV>(
                           literalSerializer.serializeTime(vf, componentArr[timeIdx])));
                    
                }

                if (latVar!=null) {
                    
                    bs.set(latVar, 
                       new Constant<IV>(
                           literalSerializer.serializeLatitude(vf, componentArr[latIdx])));
                    
                }
                
                if (lonVar!=null) {
                    
                    bs.set(lonVar, 
                        new Constant<IV>(
                            literalSerializer.serializeLongitude(vf, componentArr[lonIdx])));
                    
                }
                
                if (coordSystemVar!=null) {
                    
                    bs.set(coordSystemVar, 
                       new Constant<IV>(literalSerializer.serializeCoordSystem(vf, componentArr[coordSystemIdx])));
                    
                }
                
                if (customFieldsVar!=null) {
                    
                    final Object[] customFieldsValues = new Object[idxsOfCustomFields.length];
                    
                    for (int i=0; i<idxsOfCustomFields.length; i++) {
                        customFieldsValues[i] = componentArr[idxsOfCustomFields[i]];
                    }
                        
                    bs.set(customFieldsVar, 
                       new Constant<IV>(literalSerializer.serializeCustomFields(vf, customFieldsValues)));
                    
                }
                
                if (literalVar!=null) {
                    
                    bs.set(literalVar,
                       new Constant<IV>(
                           DummyConstantNode.toDummyIV(
                               vf.createLiteral(literalSerializer.fromComponents(componentArr),
                               litExt.getDatatypeConfig().getUri()))));
                    
                }
                
                if (distanceVar!=null) {
                    
                    // set up coordinate for the given point and calculate distance
                    final Double curLatValue = componentArr[latIdx] instanceof Double ? 
                        (Double)componentArr[latIdx] : ((Long)componentArr[latIdx]).doubleValue();
                    final Double curLonValue = componentArr[lonIdx] instanceof Double ? 
                        (Double)componentArr[lonIdx] : ((Long)componentArr[lonIdx]).doubleValue();
                    
                    final CoordinateDD cur = new CoordinateDD(curLatValue, curLonValue);
                    
                    bs.set(distanceVar, 
                        new Constant<IV>(
                            literalSerializer.serializeDistance(vf, centerPoint.distance(cur, unit), unit)));
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
      private final GeoSpatialServiceCallConfiguration gssConfig;
      final AbstractTripleStore kb;
      final GeoSpatialServiceCall serviceCall;

      int nextBindingSetItr = 0;

      ICloseableIterator<IBindingSet> curDelegate;

      public GeoSpatialInputBindingsIterator(final IBindingSet[] bindingSet,
            final GeoSpatialServiceCallConfiguration gssConfig,
            final AbstractTripleStore kb, final GeoSpatialServiceCall serviceCall) {

         this.bindingSet = bindingSet;
         this.gssConfig = gssConfig;
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
         
         final GeoSpatialQuery sq = gssConfig.toGeoSpatialQuery(bs);

         curDelegate = serviceCall.search(sq, kb);

         return true;
      }

      private void init() {

         nextDelegate();

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

      protected final GeoSpatialDatatypeConfiguration datatypeConfig;

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
         this.datatypeConfig = litExt.getDatatypeConfig();
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
      
      final private int idxOfLat;
      final private int idxOfLon;
      
      final boolean latLonIndicesValid;

      public GeoSpatialInCircleFilter(
         final PointLatLon spatialPoint, Double distance,
         final UNITS unit, final Long timeMin, final Long timeMax, 
         final GeoSpatialLiteralExtension<BigdataValue> litExt,
         final GeoSpatialCounters geoSpatialCounters) {
         
         super(litExt, geoSpatialCounters);

         this.spatialPointLat = spatialPoint.getLat();
         this.spatialPointLon = spatialPoint.getLon();
         this.distanceInMeters = CoordinateUtility.unitsToMeters(distance, unit);

         this.idxOfLat = datatypeConfig.idxOfField(ServiceMapping.LATITUDE);
         this.idxOfLon = datatypeConfig.idxOfField(ServiceMapping.LONGITUDE);

         latLonIndicesValid = idxOfLat>=0 && idxOfLon>=0;
      }

      @Override
      @SuppressWarnings("rawtypes")      
      protected boolean isValidInternal(final ITuple tuple) {
          
          if (!latLonIndicesValid)
              return false; // something's wrong here, reject all tuples

          // Note: this is possibly called over and over again, so we choose a
          //       low-level implementation to get best performance out of it

         try {
             
            final byte[] key = ((ITuple<?>) tuple).getKey();
            final IV[] ivs = IVUtility.decode(key, objectPos + 1);

            final IV oIV = ivs[objectPos];

            final double lat;
            final double lon;
            if (oIV instanceof LiteralExtensionIV) {
                
               final LiteralExtensionIV lit = (LiteralExtensionIV) oIV;

               long[] longArr = litExt.asLongArray(lit);
               final Object[] components = litExt.longArrAsComponentArr(longArr);

               lat = (double)components[idxOfLat];
               lon = (double)components[idxOfLon];
               
            } else {
                
                throw new IllegalArgumentException("Invalid IV cannot be cast to LiteralExtensionIV");
                
            }

            return 
               CoordinateUtility.distanceInMeters(lat, spatialPointLat, lon, spatialPointLon) <= distanceInMeters;
                    
         } catch (Exception e) {
         
             if (INFO) {
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
   public static class AcceptAllSolutionsFilter extends GeoSpatialFilterBase {

      private static final long serialVersionUID = -314581671447912352L;
      
      public AcceptAllSolutionsFilter(
         final GeoSpatialLiteralExtension<BigdataValue> litExt,
         final GeoSpatialCounters geoSpatialCounters) {
         
         super(litExt, geoSpatialCounters);
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
            throw new GeoSpatialSearchException("Nested groups are not allowed.");            
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
               || predicate.equals(GeoSpatial.SEARCH)
               || predicate.equals(GeoSpatial.SEARCH_DATATYPE)
               || predicate.equals(GeoSpatial.CONTEXT)
               || predicate.equals(GeoSpatial.SPATIAL_CIRCLE_CENTER)
               || predicate.equals(GeoSpatial.SPATIAL_CIRCLE_RADIUS)
               || predicate.equals(GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST) 
               || predicate.equals(GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST)
               || predicate.equals(GeoSpatial.SPATIAL_UNIT)
               || predicate.equals(GeoSpatial.TIME_START)
               || predicate.equals(GeoSpatial.TIME_END)
               || predicate.equals(GeoSpatial.COORD_SYSTEM)
               || predicate.equals(GeoSpatial.CUSTOM_FIELDS)
               || predicate.equals(GeoSpatial.CUSTOM_FIELDS_LOWER_BOUNDS)
               || predicate.equals(GeoSpatial.CUSTOM_FIELDS_UPPER_BOUNDS)) {
                
               requiredBound.add((IVariable<?>)object); // the subject var is what we return                  
            }
         }
      }

      return requiredBound;
   }
   
   /**
    * Wrapper class representing a geospatial service call configuration.
    * 
    * @author msc
    */
   public static class GeoSpatialServiceCallConfiguration {
       
       private GeoSpatialDefaults defaults = null;
       private GeoSpatialConfig geoSpatialConfig = null;
       
       private TermNode searchFunction = null;
       private TermNode predicate = null;
       private TermNode searchDatatype = null;
       private TermNode context = null;
       private TermNode spatialCircleCenter = null;
       private TermNode spatialCircleRadius = null;
       private TermNode spatialRectangleSouthWest = null;
       private TermNode spatialRectangleNorthEast = null;
       private TermNode spatialUnit = null;
       private TermNode timeStart = null;
       private TermNode timeEnd = null;
       private TermNode coordSystem = null;
       private TermNode customFields = null;
       private TermNode customFieldsLowerBounds = null;
       private TermNode customFieldsUpperBounds = null;

       private IVariable<?> searchVar = null;
       private IVariable<?> locationVar = null;
       private IVariable<?> locationAndTimeVar = null;
       private IVariable<?> timeVar = null;
       private IVariable<?> latVar = null;
       private IVariable<?> lonVar = null;
       private IVariable<?> coordSystemVar = null;
       private IVariable<?> customFieldsVar = null;
       private IVariable<?> literalVar = null;
       private IVariable<?> distanceVar = null;
       
       public GeoSpatialServiceCallConfiguration(
           final GeoSpatialDefaults defaults, final GeoSpatialConfig geoSpatialConfig,
           final IVariable<?> searchVar, final Map<URI, StatementPatternNode> sps) {
           
           this.geoSpatialConfig = geoSpatialConfig;
           this.defaults = defaults;
           this.searchVar = searchVar;

           if (sps.containsKey(GeoSpatial.SEARCH)) {
               this.searchFunction = sps.get(GeoSpatial.SEARCH).o();
           }
               
           if (sps.containsKey(GeoSpatial.PREDICATE)) {
               this.predicate = sps.get(GeoSpatial.PREDICATE).o();
           }

           if (sps.containsKey(GeoSpatial.SEARCH_DATATYPE)) {
               this.searchDatatype = sps.get(GeoSpatial.SEARCH_DATATYPE).o();
           }

           if (sps.containsKey(GeoSpatial.CONTEXT)) {
               this.context = sps.get(GeoSpatial.CONTEXT).o();
           }

           if (sps.containsKey(GeoSpatial.SPATIAL_CIRCLE_CENTER)) {
               this.spatialCircleCenter = sps.get(GeoSpatial.SPATIAL_CIRCLE_CENTER).o();
           }

           if (sps.containsKey(GeoSpatial.SPATIAL_CIRCLE_RADIUS)) {
               this.spatialCircleRadius = sps.get(GeoSpatial.SPATIAL_CIRCLE_RADIUS).o();
           }

           if (sps.containsKey(GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST)) {
               this.spatialRectangleSouthWest = 
                   sps.get(GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST).o();
           }

           if (sps.containsKey(GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST)) {
               this.spatialRectangleNorthEast = 
                   sps.get(GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST).o();
           }

           if (sps.containsKey(GeoSpatial.SPATIAL_UNIT)) {
               this.spatialUnit = sps.get(GeoSpatial.SPATIAL_UNIT).o();
           }

           if (sps.containsKey(GeoSpatial.TIME_START)) {
               this.timeStart = sps.get(GeoSpatial.TIME_START).o();
           }

           if (sps.containsKey(GeoSpatial.TIME_END)) {
               this.timeEnd = sps.get(GeoSpatial.TIME_END).o();
           }

           if (sps.containsKey(GeoSpatial.COORD_SYSTEM)) {
               this.coordSystem = sps.get(GeoSpatial.COORD_SYSTEM).o();
           }

           if (sps.containsKey(GeoSpatial.CUSTOM_FIELDS)) {
               this.customFields = sps.get(GeoSpatial.CUSTOM_FIELDS).o();
           }

           if (sps.containsKey(GeoSpatial.CUSTOM_FIELDS_LOWER_BOUNDS)) {
               this.customFieldsLowerBounds = 
                   sps.get(GeoSpatial.CUSTOM_FIELDS_LOWER_BOUNDS).o();
           }

           if (sps.containsKey(GeoSpatial.CUSTOM_FIELDS_UPPER_BOUNDS)) {
               this.customFieldsUpperBounds = 
                   sps.get(GeoSpatial.CUSTOM_FIELDS_UPPER_BOUNDS).o();
           }

           if (sps.containsKey(GeoSpatial.LOCATION_VALUE)) {
               
               final StatementPatternNode sp = sps.get(GeoSpatial.LOCATION_VALUE);
               if (!sp.o().isVariable()) {
                   throw new GeoSpatialSearchException(
                       "locationValue property must point to a variable");
               }

               this.locationVar = (IVariable<?>) sp.o().getValueExpression();
           }
           
           if (sps.containsKey(GeoSpatial.TIME_VALUE)) {
               
               final StatementPatternNode sp = sps.get(GeoSpatial.TIME_VALUE);
               if (!sp.o().isVariable()) {
                   throw new GeoSpatialSearchException(
                       "timeValue property must point to a variable");
               }

               this.timeVar = (IVariable<?>) sp.o().getValueExpression();
           }
           
           
           if (sps.containsKey(GeoSpatial.LAT_VALUE)) {
               
               final StatementPatternNode sp = sps.get(GeoSpatial.LAT_VALUE);
               if (!sp.o().isVariable()) {
                   throw new GeoSpatialSearchException(
                       "latValue property must point to a variable");
               }

               this.latVar = (IVariable<?>) sp.o().getValueExpression();
           }

           if (sps.containsKey(GeoSpatial.LON_VALUE)) {
               
               final StatementPatternNode sp = sps.get(GeoSpatial.LON_VALUE);
               if (!sp.o().isVariable()) {
                   throw new GeoSpatialSearchException(
                      "lonValue property must point to a variable");
               }

               this.lonVar = (IVariable<?>) sp.o().getValueExpression();
           }

           if (sps.containsKey(GeoSpatial.COORD_SYSTEM_VALUE)) {
               
               final StatementPatternNode sp = sps.get(GeoSpatial.COORD_SYSTEM_VALUE);
               if (!sp.o().isVariable()) {
                   throw new GeoSpatialSearchException(
                       "coordSystemValue property must point to a variable");
               }

               this.coordSystemVar = (IVariable<?>) sp.o().getValueExpression();
           }
           
           if (sps.containsKey(GeoSpatial.CUSTOM_FIELDS_VALUES)) {
               
               final StatementPatternNode sp = sps.get(GeoSpatial.CUSTOM_FIELDS_VALUES);
               if (!sp.o().isVariable()) {
                   throw new GeoSpatialSearchException(
                       "customFieldsValues property must point to a variable");
               }

               this.customFieldsVar = (IVariable<?>) sp.o().getValueExpression();
           }
           
           if (sps.containsKey(GeoSpatial.LOCATION_AND_TIME_VALUE)) {
               
               final StatementPatternNode sp = sps.get(GeoSpatial.LOCATION_AND_TIME_VALUE);
               if (!sp.o().isVariable()) {
                   throw new GeoSpatialSearchException(
                       "locationAndTimeValue property must point to a variable");
               }

               this.locationAndTimeVar = (IVariable<?>) sp.o().getValueExpression();
           }
           
           if (sps.containsKey(GeoSpatial.LITERAL_VALUE)) {

               final StatementPatternNode sp = sps.get(GeoSpatial.LITERAL_VALUE);
               if (!sp.o().isVariable()) {
                   throw new GeoSpatialSearchException(
                       "locationAndTimeValue property must point to a variable");
               }

               this.literalVar = (IVariable<?>) sp.o().getValueExpression();

           }
           
           if (sps.containsKey(GeoSpatial.DISTANCE_VALUE)) {

               final StatementPatternNode sp = sps.get(GeoSpatial.DISTANCE_VALUE);
               if (!sp.o().isVariable()) {
                   throw new GeoSpatialSearchException(
                       "distanceValue property must point to a variable");
               }

               this.distanceVar = (IVariable<?>) sp.o().getValueExpression();

           }
       }

       /**
        * Converts the configuration into a query over the given binding set
        * @param bs
        */
       public GeoSpatialQuery toGeoSpatialQuery(final IBindingSet bs) {
           
             final URI searchDatatypeUri = resolveSearchDatatype(
                   this.searchDatatype, bs);
             final GeoFunction searchFunction = resolveAsGeoFunction(
                   this.searchFunction, bs);
             final PointLatLon spatialCircleCenter = resolveAsPoint(
                   this.spatialCircleCenter, bs);
             final Double spatialCircleRadius = resolveAsDouble(
                   this.spatialCircleRadius, bs);
             final PointLatLon spatialRectangleUpperLeft = resolveAsPoint(
                   this.spatialRectangleSouthWest, bs);
             final PointLatLon spatialRectangleLowerRight = resolveAsPoint(
                   this.spatialRectangleNorthEast, bs);
             final UNITS spatialUnit = resolveAsSpatialDistanceUnit(
                   this.spatialUnit, bs);
             final Long coordSystem = resolveAsLong(this.coordSystem, bs);
             final Long timeStart = resolveAsLong(this.timeStart, bs);
             final Long timeEnd = resolveAsLong(this.timeEnd, bs);
             
             final String[] customFields = resolveAsStringArr(this.customFields, bs);
             
             // gather value types for custom fields
             final GeoSpatialDatatypeConfiguration datatypeConfig = 
                 geoSpatialConfig.getConfigurationForDatatype(searchDatatypeUri);
             
             if (datatypeConfig==null) {
                 throw new GeoSpatialSearchException(
                     "Datatype " + searchDatatypeUri + " is not a registered geospatial "
                     + "datatype. Query cannot be executed.");
             }
             
             final ValueType[] customFieldsVTs = new ValueType[customFields.length];
             for (int i=0; i<customFields.length; i++) {
                 final ValueType vt = datatypeConfig.getValueTypeOfCustomField(customFields[i]);
                 if (vt!=null) {
                     customFieldsVTs[i] = vt;
                 } else {
                     throw new GeoSpatialSearchException(
                         "Undefined custom field used in query: " + customFields[i]);
                 }
             }
             
             final Object[] customFieldsLowerBounds = 
                 resolveAsLongDoubleArr(this.customFieldsLowerBounds, customFieldsVTs, bs);
             final Object[] customFieldsUpperBounds = 
                 resolveAsLongDoubleArr(this.customFieldsUpperBounds, customFieldsVTs, bs);

             final GeoSpatialQuery sq = 
                 new GeoSpatialQuery(geoSpatialConfig, searchFunction, searchDatatypeUri,
                     bs.get(searchVar), predicate, context, spatialCircleCenter, 
                     spatialCircleRadius, spatialRectangleUpperLeft, 
                     spatialRectangleLowerRight, spatialUnit, timeStart, timeEnd, coordSystem, 
                     GeoSpatialQuery.toValidatedCustomFieldsConstraints(
                         customFields, customFieldsLowerBounds, customFieldsUpperBounds), 
                     locationVar, timeVar, locationAndTimeVar, latVar, lonVar,
                     coordSystemVar, customFieldsVar, literalVar, distanceVar, bs);

             return sq;
       }
       
       GeoFunction resolveAsGeoFunction(final TermNode termNode, final IBindingSet bs) {

           if (termNode==null) {
               return GeoFunction.UNDEFINED; // no geo function defined
           }
           
           String geoFunctionStr = resolveAsString(termNode, bs);

           // try override with system default, if not set
           if (geoFunctionStr == null) {
              geoFunctionStr = defaults.getDefaultFunction();
           }

           if (geoFunctionStr != null && !geoFunctionStr.isEmpty()) {

               final GeoFunction gf = GeoFunction.forName(geoFunctionStr);

               if (gf==null) {
                   throw new GeoSpatialSearchException("Geo function '" + geoFunctionStr + "' not known.");
               }
               
               return gf;
           }

           return GeoFunction.UNDEFINED; // fallback

        }

        UNITS resolveAsSpatialDistanceUnit(final TermNode termNode, final IBindingSet bs) {

           String spatialUnitStr = resolveAsString(termNode, bs);

           // try override with system default, if not set
           if (spatialUnitStr == null) {
              spatialUnitStr = defaults.getDefaultSpatialDistanceUnit();
           }

           if (spatialUnitStr != null && !spatialUnitStr.isEmpty()) {

               final UNITS u = UNITS.valueOf(spatialUnitStr);

               if (u==null) {
                   throw new GeoSpatialSearchException("Input could not be parsed as unit: '" + spatialUnitStr + "'.");
               }
               
               return u;
           }

           return GeoSpatial.Options.DEFAULT_GEO_SPATIAL_UNIT; // fallback

        }

        Double resolveAsDouble(final TermNode termNode, final IBindingSet bs) {

           String s = resolveAsString(termNode, bs);
           if (s == null || s.isEmpty()) {
              return null;
           }

           try {
               
              return Double.valueOf(s);
              
           } catch (NumberFormatException e) {

               throw new GeoSpatialSearchException("Input could not be resolved as double value: '" + s + "'.");

           }
           
        }

        Long resolveAsLong(final TermNode termNode, final IBindingSet bs) {

           String s = resolveAsString(termNode, bs);
           if (s == null || s.isEmpty()) {
              return null;
           }

           try {
               
              return Long.valueOf(s);
              
           } catch (NumberFormatException e) {

               throw new GeoSpatialSearchException("Input could not be resolved as long value: '" + s + "'.");

           }
        }

        Literal resolveAsLiteral(final TermNode termNode, final IBindingSet bs) {

            if (termNode == null) { // term node not set explicitly
               return null;
            }

            if (termNode.isConstant()) {

               return (Literal) termNode.getValue();

            } else {

               if (bs == null) {
                  return null; // shouldn't happen, but just in case...
               }

               final IVariable<?> var = (IVariable<?>) termNode .getValueExpression();

               if (bs.isBound(var)) {
                  final IConstant<?> c = bs.get(var);
                  if (c == null || c.get() == null) {
                     return null;
                  }


                  final Object obj = c.get();
                  if (obj instanceof TermId<?>) {

                      return ((TermId<?>) obj);

                  } else if (obj instanceof IV) {

                     @SuppressWarnings("rawtypes")
                     final BigdataValue bdVal = ((IV)obj).getValue();

                      if (bdVal!=null) {
                          return (Literal)bdVal;
                      }
                  }

                  // should never end up here
                  throw new GeoSpatialSearchException("Value for literal could not be retrieved.");

               } else {
                  throw new GeoSpatialSearchException(
                      GeoSpatialSearchException.SERVICE_VARIABLE_UNBOUND + ":" + var);
               }
            }
         }

        PointLatLon resolveAsPoint(final TermNode termNode, final IBindingSet bs) {

            final Literal lit = resolveAsLiteral(termNode, bs);
            if (lit == null || lit.stringValue().isEmpty()) {
                return null;
            }

            final String pointAsStr = lit.stringValue();
            IGeoSpatialLiteralSerializer serializer = null;
            GeoSpatialDatatypeConfiguration pconfig = null;

            if (lit.getDatatype() != null) {
                // If we have datatype that can extract coordinates, use it to exteract
                pconfig = geoSpatialConfig.getConfigurationForDatatype(lit.getDatatype());
                if (pconfig.hasLat() && pconfig.hasLon()) {
                    serializer = pconfig.getLiteralSerializer();
                }
            }

            try {
                
                if (serializer == null) {
                    
                  return new PointLatLon(pointAsStr);

                } else {
                    
                    final String[] comps = serializer.toComponents(pointAsStr);
                    final Double lat = Double.parseDouble(comps[pconfig.idxOfField(ServiceMapping.LATITUDE)]);
                    final Double lon = Double.parseDouble(comps[pconfig.idxOfField(ServiceMapping.LONGITUDE)]);
                    return new PointLatLon(lat, lon);
                
                }
                
            } catch (NumberFormatException e) {

                throw new GeoSpatialSearchException("Input could not be resolved as point: '" + pointAsStr + "'.");
            }
        }

        String resolveAsString(final TermNode termNode, final IBindingSet bs) {

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

              final IVariable<?> var = (IVariable<?>) termNode .getValueExpression();
              
              if (bs.isBound(var)) {
                 IConstant<?> c = bs.get(var);
                 if (c == null || c.get() == null) {
                    return null;
                 }

                 
                 final Object obj = c.get();
                 if (obj instanceof TermId<?>) {
                     
                     return ((TermId<?>) obj).stringValue();
                     
                 } else if (obj instanceof IV) {
                     
                    @SuppressWarnings("rawtypes")
                    final BigdataValue bdVal = ((IV)obj).getValue();

                     if (bdVal!=null) {
                         return bdVal.stringValue();
                     }
                 } 

                 // should never end up here
                 throw new GeoSpatialSearchException("Value for literal could not be retrieved.");

              } else {
                 throw new GeoSpatialSearchException(
                     GeoSpatialSearchException.SERVICE_VARIABLE_UNBOUND + ":" + var);
              }
           }
        }
        
        String[] resolveAsStringArr(final TermNode termNode, final IBindingSet bs) {
            
            final String toSplit = resolveAsString(termNode, bs);
            
            if (toSplit==null)
                return new String[0];
            
            return toSplit.split(GeoSpatial.CUSTOM_FIELDS_SEPARATOR);
        }
        
        Object[] resolveAsLongDoubleArr(final TermNode termNode, final ValueType[] valueTypes, final IBindingSet bs) {

            final String[] stringArr = resolveAsStringArr(termNode, bs);
            
            if (stringArr.length!=valueTypes.length) {
                throw new GeoSpatialSearchException(
                    "Magic predicates geo:customFields, geo:customFieldsLowerBounds, and "
                    + "geo:customFieldsUpperBounds must all be given and have same length.");
            }
            
            final Object[] objArr = new Object[stringArr.length];
            for (int i=0; i<stringArr.length; i++) {
                switch (valueTypes[i]) {
                case DOUBLE:
                {
                    final Double val = Double.valueOf(stringArr[i]);
                    objArr[i] = val;
                    break;
                }
                case LONG:
                {
                    final Long val = Long.valueOf(stringArr[i]);
                    objArr[i] = val;
                    break;
                }
                default:
                    throw new GeoSpatialSearchException("Unhandled value type: " + valueTypes[i]);
                }
            }
            
            return objArr;
        }
        
        URI resolveSearchDatatype(final TermNode searchDatatype, final IBindingSet bs) {
            
            if (searchDatatype==null) {
                final URI datatype = geoSpatialConfig.getDefaultDatatype();
                
                if (datatype==null) {
                    throw new GeoSpatialSearchException(
                        "No default datatype set in configuration. Please specify the datatype "
                        + "that you want to query using magic predicate " + GeoSpatial.SEARCH_DATATYPE + ".");
                }
                
                return datatype;
            }
            
            if (searchDatatype.isConstant()) {
                
                URI uri = (URI) searchDatatype.getValue();
                
                if (uri==null) {
                    
                    uri = geoSpatialConfig.getDefaultDatatype();
                    if (uri==null) {
                        throw new GeoSpatialSearchException(
                            "No default datatype set in configuration. Please specify the datatype "
                            + "that you want to query using magic predicate " + GeoSpatial.SEARCH_DATATYPE + ".");
                    }
                }
                
                return uri;
                
            } else {
                
                if (bs==null) {
                    return null; // shouldn't happen, but just in case...
                }
                
                final IVariable<?> var = (IVariable<?>) searchDatatype
                        .getValueExpression();
                if (bs.isBound(var)) {
                    IConstant<?> c = bs.get(var);
                    if (c == null || c.get() == null
                          || !(c.get() instanceof TermId<?>)) {
                       return null;
                    }

                    TermId<?> cAsTerm = (TermId<?>) c.get();
                    return (URI)cAsTerm.getValue();

                 } else {
                    throw new GeoSpatialSearchException(
                        GeoSpatialSearchException.SERVICE_VARIABLE_UNBOUND + ":" + var);
                 }
            }
        }
   }

}
