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
package com.bigdata.rdf.graph.impl.bd;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.graph.IBinder;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGASStats;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.IPredecessor;
import com.bigdata.rdf.graph.IReducer;
import com.bigdata.rdf.graph.TraversalDirectionEnum;
import com.bigdata.rdf.graph.analytics.CC;
import com.bigdata.rdf.graph.analytics.PR;
import com.bigdata.rdf.graph.impl.GASEngine;
import com.bigdata.rdf.graph.impl.GASState;
import com.bigdata.rdf.graph.impl.bd.BigdataGASEngine.BigdataGraphAccessor;
import com.bigdata.rdf.graph.impl.scheduler.CHMScheduler;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueImpl;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.CustomServiceFactoryBase;
import com.bigdata.rdf.sparql.ast.service.BigdataNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.ChunkedArrayIterator;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A SERVICE that exposes {@link IGASProgram}s for SPARQL execution.
 * <p>
 * For example, the following would run a depth-limited BFS traversal:
 * 
 * <pre>
 * PREFIX gas: <http://www.bigdata.com/rdf/gas#>
 * #...
 * SERVICE &lt;gas#service&gt; {
 *    gas:program gas:gasClass "com.bigdata.rdf.graph.analytics.BFS" .
 *    gas:program gas:in &lt;IRI&gt; . # one or more times, specifies the initial frontier.
 *    gas:program gas:out ?out . # exactly once - will be bound to the visited vertices.
 *    gas:program gas:maxIterations 4 . # optional limit on breadth first expansion.
 *    gas:program gas:maxVisited 2000 . # optional limit on the #of visited vertices.
 *    gas:program gas:nthreads 4 . # specify the #of threads to use (optional)
 * }
 * </pre>
 * 
 * Or the following would run the FuzzySSSP algorithm.
 * 
 * <pre>
 * PREFIX gas: <http://www.bigdata.com/rdf/gas#>
 * #...
 * SERVICE &lt;gas:service&gt; {
 *    gas:program gas:gasClass "com.bigdata.rdf.graph.analytics.FuzzySSSP" .
 *    gas:program gas:in &lt;IRI&gt; . # one or more times, specifies the initial frontier.
 *    gas:program gas:target &lt;IRI&gt; . # one or more times, identifies the target vertices and hence the paths of interest.
 *    gas:program gas:out ?out . # exactly once - will be bound to the visited vertices laying within N-hops of the shortest paths.
 *    gas:program gas:maxIterations 4 . # optional limit on breadth first expansion.
 *    gas:program gas:maxVisited 2000 . # optional limit on the #of visited vertices.
 * }
 * </pre>
 * 
 * FIXME Also allow the execution of gas workflows, such as FuzzySSSP. A
 * workflow would be more along the lines of a Callable, but one where the
 * initial source and/or target vertices could be identified. Or have an
 * interface that wraps the analytics (including things like FuzzySSSP) so they
 * can declare their own arguments for invocation as a SERVICE.
 * 
 * TODO The input frontier could be a variable, in which case we would pull out
 * the column for that variable rather than running the algorithm once per
 * source binding set, right? Or maybe not.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://wiki.blazegraph.com/wiki/index.php/RDF_GAS_API">RDF GAS
 *      API</a>
 */
public class GASService extends CustomServiceFactoryBase {

    public interface Options {
        
        /**
         * The namespace used for bigdata GAS API.
         */
        String NAMESPACE = "http://www.bigdata.com/rdf/gas#";

        /**
         * The URL at which the {@link GASService} will respond.
         */
        URI SERVICE_KEY = new URIImpl(NAMESPACE + "service");
        
        /**
         * Used as the subject in the GAS SERVICE invocation pattern.
         */
        URI PROGRAM = new URIImpl(NAMESPACE + "program");

        /**
         * Magic predicate identifies the fully qualified class name of the
         * {@link IGASProgram} to be executed.
         */
        URI GAS_CLASS = new URIImpl(NAMESPACE + "gasClass");

        /**
         * The #of threads that will be used to expand the frontier in each
         * iteration of the algorithm (optional, default
         * {@value #DEFAULT_NTHREADS}).
         * 
         * @see #DEFAULT_NTHREADS
         */
        URI NTHREADS = new URIImpl(NAMESPACE + "nthreads");
        
        int DEFAULT_NTHREADS = 4;

        /**
         * This option determines the traversal direction semantics for the
         * {@link IGASProgram} against the graph, including whether the the
         * edges of the graph will be interpreted as directed (
         * {@link TraversalDirectionEnum#Forward} (which is the default),
         * {@link TraversalDirectionEnum#Reverse}), or
         * {@link TraversalDirectionEnum#Undirected}.
         * 
         * @see TraversalDirectionEnum
         * @see IGASContext#setTraversalDirection(TraversalDirectionEnum)
         */
        URI TRAVERSAL_DIRECTION = new URIImpl(NAMESPACE + "traversalDirection");
        
        TraversalDirectionEnum DEFAULT_DIRECTED_TRAVERSAL = TraversalDirectionEnum.Forward;
        
        /**
         * The maximum #of iterations for the GAS program (optional, default
         * {@value #DEFAULT_MAX_ITERATIONS}).
         * 
         * @see #DEFAULT_MAX_ITERATIONS
         * @see IGASContext#setMaxIterations(int)
         */
        URI MAX_ITERATIONS = new URIImpl(NAMESPACE + "maxIterations");
        
        int DEFAULT_MAX_ITERATIONS = Integer.MAX_VALUE;

        /**
         * The maximum #of iterations for the GAS program after the targets
         * have been reached (optional, default
         * {@value #DEFAULT_MAX_ITERATIONS_AFTER_TARGETS}).  Default behavior
         * is to not stop once the targets are reached.
         * 
         * @see #DEFAULT_MAX_ITERATIONS_AFTER_TARGETS
         * @see IGASContext#setMaxIterationsAfterTargets(int)
         */
        URI MAX_ITERATIONS_AFTER_TARGETS = new URIImpl(NAMESPACE + "maxIterationsAfterTargets");
        
        int DEFAULT_MAX_ITERATIONS_AFTER_TARGETS = Integer.MAX_VALUE;

        /**
         * The maximum #of vertices in the visited set for the GAS program
         * (optional, default {@value #DEFAULT_MAX_VISITED}).
         * 
         * @see #DEFAULT_MAX_VISITED
         * @see IGASContext#setMaxVisited(int)
         */
        URI MAX_VISITED = new URIImpl(NAMESPACE + "maxVisited");
        
        int DEFAULT_MAX_VISITED = Integer.MAX_VALUE;

        /**
         * An optional constraint on the types of links that will be visited by
         * the algorithm.
         * <p>
         * Note: When this option is used, the scatter and gather will not visit
         * the property set for the vertex. Instead, the graph is treated as if
         * it were an unattributed graph and only mined for the connectivity
         * data (which may include a link weight).
         * 
         * @see IGASContext#setLinkType(URI)
         */
        URI LINK_TYPE = new URIImpl(NAMESPACE + "linkType");

        /**
         * An optional constraint on the types of the link attributes that will
         * be visited by the algorithm - the use of this option is required if
         * you want to process some specific link weight rather than the simple
         * topology of the graph.
         * 
         * @see IGASContext#setLinkAttributeType(URI)
         */
        URI LINK_ATTR_TYPE = new URIImpl(NAMESPACE + "linkAttrType");

        /**
         * The {@link IGASScheduler} (default is {@link #DEFAULT_SCHEDULER}).
         * Class must implement {@link IGASSchedulerImpl}.
         */
        URI SCHEDULER_CLASS = new URIImpl(NAMESPACE + "schedulerClass");
 
        Class<? extends IGASSchedulerImpl> DEFAULT_SCHEDULER = CHMScheduler.class;

        /**
         * Magic predicate used to specify one (or more) vertices in the initial
         * frontier.
         * <p>
         * Note: Algorithms such as {@link CC} and {@link PR} automatically
         * place all vertices into the initial frontier. For such algorithms,
         * you do not need to specify {@link #IN}.
         */
        URI IN = new URIImpl(NAMESPACE + "in");

        /**
         * Magic predicate used to specify one (or more) target vertices. This
         * may be used in combination with algorithms that compute paths in a
         * graph to filter the visited vertices after the traversal in order to
         * remove any vertex that is not part of a path to one or more of the
         * specified target vertices.
         * <p>
         * In order to support this, the algorithm has to have a concept of a
         * <code>predecessor</code>. For each <code>target</code>, the set of
         * visited vertices is checked to see if the target was reachable. If it
         * was reachable, then the predecessors are walked backwards until a
         * starting vertex is reached (predecessor:=null). Each such predecessor
         * is added to a list of vertices to be retained. This is repeated for
         * each target. Once we have identified the combined list of vertices to
         * be reained, all vertices NOT in that list are removed from the
         * visited vertex state. This causes the algorithm to only report on
         * those paths that lead to at least one of the specified target
         * vertices.
         * <p>
         * Note: If you do not care about the distance between two vertices, but
         * only whether they are reachable from one another, you can put both
         * vertices into the initial frontier. The algorithm will then work from
         * both points which can accelerate convergence.
         */
        URI TARGET = new URIImpl(NAMESPACE + "target");
        
        /**
         * Magic predicate used to specify a variable that will become bound to
         * each vertex in the visited set for the analytic. {@link #OUT} is
         * always bound to the visited vertices. The other "out" variables are
         * bound to state associated with the visited vertices in an algorithm
         * dependent manner.
         * 
         * @see IGASProgram#getBinderList()
         */
        URI OUT = new URIImpl(NAMESPACE + "out");

        URI OUT1 = new URIImpl(NAMESPACE + "out1");

        URI OUT2 = new URIImpl(NAMESPACE + "out2");

        URI OUT3 = new URIImpl(NAMESPACE + "out3");

        URI OUT4 = new URIImpl(NAMESPACE + "out4");

        URI OUT5 = new URIImpl(NAMESPACE + "out5");

        URI OUT6 = new URIImpl(NAMESPACE + "out6");

        URI OUT7 = new URIImpl(NAMESPACE + "out7");

        URI OUT8 = new URIImpl(NAMESPACE + "out8");

        URI OUT9 = new URIImpl(NAMESPACE + "out9");
        
    }

    static private transient final Logger log = Logger
            .getLogger(GASService.class);

    /**
     * The list of all out variables.
     */
    static private List<URI> OUT_VARS = Collections.unmodifiableList(Arrays
            .asList(new URI[] { Options.OUT, Options.OUT1, Options.OUT2,
                    Options.OUT3, Options.OUT4, Options.OUT5, Options.OUT6,
                    Options.OUT7, Options.OUT8, Options.OUT9 }));

    private final BigdataNativeServiceOptions serviceOptions;

    public GASService() {

        serviceOptions = new BigdataNativeServiceOptions();
        
        /*
         * TODO Review decision to make this a runFirst service. The rational is
         * that this service can only apply a very limited set of restrictions
         * during query, therefore it will often make sense to run it first.
         * However, the fromTime and toTime could be bound by the query and the
         * service can filter some things more efficiently internally than if we
         * generated a bunch of intermediate solutions for those things.
         */
        serviceOptions.setRunFirst(true);
        
    }

    /**
     * The known URIs.
     * <p>
     * Note: We can recognize anything in {@link Options#NAMESPACE}, but the
     * predicate still has to be something that we know how to interpret.
     */
    static final Set<URI> gasUris;
    
    static {
        
        final Set<URI> set = new LinkedHashSet<URI>();
    
        set.add(Options.PROGRAM);
        
        gasUris = Collections.unmodifiableSet(set);
        
    }

    @Override
    public IServiceOptions getServiceOptions() {

        return serviceOptions;
        
    }

    /**
     * NOP
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void startConnection(BigdataSailConnection conn) {
        // NOP
    }

    @Override
    public ServiceCall<?> create(final ServiceCallCreateParams params) {

        if (params == null)
            throw new IllegalArgumentException();

        final AbstractTripleStore store = params.getTripleStore();

        if (store == null)
            throw new IllegalArgumentException();

        /*
         * Create and return the ServiceCall object which will execute this
         * query.
         */

        return new GASServiceCall(store, params.getServiceNode(),
                getServiceOptions());

    }

    /**
     * Execute the service call (run the GAS program).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     *         TODO Validate the service call parameters, including whether they
     *         are understood by the specific algorithm.
     */
    private static class GASServiceCall<VS, ES, ST> implements BigdataServiceCall {

        private final AbstractTripleStore store;
        private final GraphPatternGroup<IGroupMemberNode> graphPattern;
        private final IServiceOptions serviceOptions;
        
        // options extracted from the SERVICE's graph pattern.
        private final int nthreads;
        private final TraversalDirectionEnum traversalDirection;
        private final int maxIterations;
        private final int maxIterationsAfterTargets;
        private final int maxVisited;
        private final URI linkType, linkAttrType;
        private final Class<IGASProgram<VS, ES, ST>> gasClass;
        private final Class<IGASSchedulerImpl> schedulerClass;
        private final Value[] initialFrontier;
        private final Value[] targetVertices;
        private final IVariable<?>[] outVars;

        public GASServiceCall(final AbstractTripleStore store,
                final ServiceNode serviceNode,
                final IServiceOptions serviceOptions) {

            if (store == null)
                throw new IllegalArgumentException();

            if (serviceNode == null)
                throw new IllegalArgumentException();

            if (serviceOptions == null)
                throw new IllegalArgumentException();

            this.store = store;

            this.graphPattern = serviceNode.getGraphPattern();

            this.serviceOptions = serviceOptions;

            this.nthreads = ((Literal) getOnlyArg(
                    Options.PROGRAM,
                    Options.NTHREADS,
                    store.getValueFactory().createLiteral(
                            Options.DEFAULT_NTHREADS))).intValue();

            this.traversalDirection = TraversalDirectionEnum
                    .valueOf(((Literal) getOnlyArg(
                            Options.PROGRAM,
                            Options.TRAVERSAL_DIRECTION,
                            store.getValueFactory().createLiteral(
                                    Options.DEFAULT_DIRECTED_TRAVERSAL.name())))
                            .stringValue());

            this.maxIterations = ((Literal) getOnlyArg(Options.PROGRAM,
                    Options.MAX_ITERATIONS, store.getValueFactory()
                            .createLiteral(Options.DEFAULT_MAX_ITERATIONS)))
                    .intValue();

            this.maxIterationsAfterTargets = ((Literal) getOnlyArg(Options.PROGRAM,
                    Options.MAX_ITERATIONS_AFTER_TARGETS, store.getValueFactory()
                            .createLiteral(Options.DEFAULT_MAX_ITERATIONS_AFTER_TARGETS)))
                    .intValue();

            this.maxVisited = ((Literal) getOnlyArg(
                    Options.PROGRAM,
                    Options.MAX_VISITED,
                    store.getValueFactory().createLiteral(
                            Options.DEFAULT_MAX_VISITED))).intValue();

            this.linkType = (URI) getOnlyArg(Options.PROGRAM,
                    Options.LINK_TYPE, null/* default */);

            this.linkAttrType = (URI) getOnlyArg(Options.PROGRAM,
                    Options.LINK_ATTR_TYPE, null/* default */);

            // GASProgram (required)
            {
                
                final Literal tmp = (Literal) getOnlyArg(Options.PROGRAM,
                        Options.GAS_CLASS);

                if (tmp == null)
                    throw new IllegalArgumentException(
                            "Required predicate not specified: "
                                    + Options.GAS_CLASS);

                final String className = tmp.stringValue();

                final Class<?> cls;
                try {
                    cls = Class.forName(className);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("No such class: "
                            + className);
                }

                if (!IGASProgram.class.isAssignableFrom(cls))
                    throw new IllegalArgumentException(Options.GAS_CLASS
                            + " must extend " + IGASProgram.class.getName());

                this.gasClass = (Class<IGASProgram<VS, ES, ST>>) cls;

            }

            // Scheduler (optional).
            {
                
                final Literal tmp = (Literal) getOnlyArg(Options.PROGRAM,
                        Options.SCHEDULER_CLASS);

                if (tmp == null) {

                    this.schedulerClass = null;

                } else {
                    
                    final String className = tmp.stringValue();

                    final Class<?> cls;
                    try {
                        cls = Class.forName(className);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("No such class: "
                                + className);
                    }

                    if (!IGASSchedulerImpl.class.isAssignableFrom(cls))
                        throw new IllegalArgumentException(
                                Options.SCHEDULER_CLASS + " must extend "
                                        + IGASSchedulerImpl.class.getName());

                    this.schedulerClass = (Class<IGASSchedulerImpl>) cls;

                }

            }
            
            // Initial frontier.
            this.initialFrontier = getArg(Options.PROGRAM, Options.IN);

            // Target vertices
            this.targetVertices = getArg(Options.PROGRAM, Options.TARGET);

            /*
             * The output variable (bound to the visited set).
             * 
             * TODO This does too much work. It searches the group graph pattern
             * 10 times, when we could do just one pass and find everything.
             */
            {

                this.outVars = new IVariable[10];

                int i = 0;

                for (URI uri : OUT_VARS) {

                    this.outVars[i++] = getVar(Options.PROGRAM, uri);

                }

            }

        }

        /**
         * Return the variable associated with the first instandce of the
         * specified subject and predicate in the service's graph pattern. Only
         * the simple {@link StatementPatternNode}s are visited.
         * 
         * @param s
         *            The subject.
         * @param p
         *            The predicate.
         * 
         * @return The variable -or- <code>null</code> if the specified subject
         *         and predicate do not appear.
         */
        private IVariable<?> getVar(final URI s, final URI p) {

            if (s == null)
                throw new IllegalArgumentException();
            if (p == null)
                throw new IllegalArgumentException();

            List<Value> tmp = null;

            final Iterator<IGroupMemberNode> itr = graphPattern.getChildren()
                    .iterator();

            while (itr.hasNext()) {

                final IGroupMemberNode child = itr.next();

                if (!(child instanceof StatementPatternNode))
                    continue;

                final StatementPatternNode sp = (StatementPatternNode) child;

                // s and p are constants.
                if (!sp.s().isConstant())
                    continue;
                if (!sp.p().isConstant())
                    continue;

                // constants match.
                if (!s.equals(sp.s().getValue()))
                    continue;
                if (!p.equals(sp.p().getValue()))
                    continue;

                if (tmp == null)
                    tmp = new LinkedList<Value>();

                // found an o.
                return ((VarNode)sp.o()).getValueExpression();

            }

            return null; // not found.

        }
        
        /**
         * Return the object bindings from the service's graph pattern for the
         * specified subject and predicate. Only the simple
         * {@link StatementPatternNode}s are visited.
         * 
         * @param s
         *            The subject.
         * @param p
         *            The predicate.
         * 
         * @return An array containing one or more bindings -or-
         *         <code>null</code> if the specified subject and predicate do
         *         not appear.
         */
        private Value[] getArg(final URI s, final URI p) {

            if (s == null)
                throw new IllegalArgumentException();
            if (p == null)
                throw new IllegalArgumentException();

            List<Value> tmp = null;

            final Iterator<IGroupMemberNode> itr = graphPattern.getChildren()
                    .iterator();

            while (itr.hasNext()) {

                final IGroupMemberNode child = itr.next();

                if (!(child instanceof StatementPatternNode))
                    continue;

                final StatementPatternNode sp = (StatementPatternNode) child;

                // s and p are constants.
                if (!sp.s().isConstant())
                    continue;
                if (!sp.p().isConstant())
                    continue;

                // constants match.
                if (!s.equals(sp.s().getValue()))
                    continue;
                if (!p.equals(sp.p().getValue()))
                    continue;

                if (tmp == null)
                    tmp = new LinkedList<Value>();

                // found an o.
                tmp.add(sp.o().getValue());

            }

            if (tmp == null)
                return null;

            return tmp.toArray(new Value[tmp.size()]);

        }

        /**
         * Return the sole {@link Value} for the given s and p.
         * 
         * @param s
         *            The subject.
         * @param p
         *            The predicate.
         *            
         * @return The sole {@link Value} for that s and p -or-
         *         <code>null</code> if no value was given.
         *         
         * @throws RuntimeException
         *             if there are multiple values.
         */
        private Value getOnlyArg(final URI s, final URI p) {

            final Value[] tmp = getArg(s, p);

            if (tmp == null)
                return null;

            if (tmp.length > 1)
                throw new IllegalArgumentException("Multiple values: s=" + s
                        + ", p=" + p);

            return tmp[0];
            
        }

        /**
         * Return the sole {@link Value} for the given s and p and the default
         * value if no value was explicitly provided.
         * 
         * @param s
         *            The subject.
         * @param p
         *            The predicate.
         * @param def
         *            The default value.
         * 
         * @return The sole {@link Value} for that s and p -or- the default
         *         value if no value was given.
         * 
         * @throws RuntimeException
         *             if there are multiple values.
         */
        private Value getOnlyArg(final URI s, final URI p, final Value def) {

            final Value tmp = getOnlyArg(s, p);

            if (tmp == null)
                return def;

            return tmp;
            
        }
        
        @Override
        public IServiceOptions getServiceOptions() {

            return serviceOptions;
            
        }

        /**
         * Execute the GAS program.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public ICloseableIterator<IBindingSet> call(
                final IBindingSet[] bindingSets) throws Exception {

            /*
             * Try/finally pattern to setup the BigdataGASEngine, execute the
             * algorithm, and return the results.
             */
            IGASEngine gasEngine = null;

            try {

                gasEngine = newGasEngine(store.getIndexManager(), nthreads);

                if (schedulerClass != null) {

                    ((GASEngine) gasEngine).setSchedulerClass(schedulerClass);

                }

                final IGraphAccessor graphAccessor = newGraphAccessor(store);

                final IGASProgram<VS, ES, ST> gasProgram = newGASProgram(gasClass);

                final IGASContext<VS, ES, ST> gasContext = gasEngine.newGASContext(
                        graphAccessor, gasProgram);

                gasContext.setTraversalDirection(traversalDirection);
                
                gasContext.setMaxIterations(maxIterations);

                gasContext.setMaxIterationsAfterTargets(maxIterationsAfterTargets);

                gasContext.setMaxVisited(maxVisited);
                
                if (targetVertices != null) {

                	gasContext.setTargetVertices(toIV(targetVertices));
                	
                }
                
                // Optional link type constraint.
                if (linkType != null)
                    gasContext.setLinkType(linkType);

                // Optional link attribute constraint.
                if (linkAttrType != null)
                    gasContext.setLinkAttributeType(linkAttrType);

                final IGASState<VS, ES, ST> gasState = gasContext.getGASState();

                if (initialFrontier != null) {

                    /*
                     * FIXME Why can't we pass in the Value (with a defined IV)
                     * and not the IV? This should work. Passing in the IV is
                     * against the grain of the API and the generalized
                     * abstraction as Values. Of course, having the IV is
                     * necessary since this is an internal, high performance,
                     * and close to the indices operation.
                     */
                    @SuppressWarnings("rawtypes")
                    final IV[] tmp = toIV(initialFrontier);
                    
                    // set the frontier.
                    gasState.setFrontier(gasContext, tmp);

                }
                
                // Run the analytic.
                final IGASStats stats = (IGASStats) gasContext.call();

                if (targetVertices != null
                        && gasProgram instanceof IPredecessor) {

                    /*
                     * Remove vertices from the visited set that are not on a
                     * path leading to at least one of the specified target
                     * vertices.
                     * 
                     * FIXME Why can't we pass in the Value (with a defined IV)
                     * and not the IV? This should work. Passing in the IV is
                     * against the grain of the API and the generalized
                     * abstraction as Values. Of course, having the IV is
                     * necessary since this is an internal, high performance,
                     * and close to the indices operation.
                     */

                    @SuppressWarnings("rawtypes")
                    final IV[] tmp = toIV(targetVertices);

                    @SuppressWarnings("unchecked")
                    final IPredecessor<VS, ES, ST> t = (IPredecessor<VS, ES, ST>) gasProgram;
                    
                    t.prunePaths(gasContext, tmp);

                }
                
                if (log.isInfoEnabled()) {
                    final StringBuilder sb = new StringBuilder();
                    sb.append("GAS");
                    sb.append(": analytic=" + gasProgram.getClass().getSimpleName());
                    sb.append(", nthreads=" + nthreads);
                    sb.append(", scheduler=" + ((GASState<VS, ES, ST>)gasState).getScheduler().getClass().getSimpleName());
                    sb.append(", gasEngine=" + gasEngine.getClass().getSimpleName());
                    sb.append(", stats=" + stats);
                    log.info(sb.toString());
                }

                /*
                 * Bind output variables (if any).
                 */

                final IBindingSet[] out = gasState
                        .reduce(new BindingSetReducer<VS, ES, ST>(outVars,
                                store, gasProgram, gasContext));

                return new ChunkedArrayIterator<IBindingSet>(out);

            } finally {

                if (gasEngine != null) {

                    gasEngine.shutdownNow();

                    gasEngine = null;

                }

            }

        }

        /**
         * Convert a {@link Value}[] of {@link BigdataValue} instances into an
         * {@link IV}[].
         */
        private static IV[] toIV(final Value[] values) {

            @SuppressWarnings("rawtypes")
            final IV[] tmp = new IV[values.length];

            // Setup the initial frontier.
            int i = 0;
            for (Value v : values) {

                tmp[i++] = ((BigdataValue) v).getIV();

            }

            return tmp;

        }
        
        /**
         * Class used to report {@link IBindingSet}s to the {@link GASService}.
         * {@link IGASProgram}s can customize the way in which they interpret
         * the declared variables by subclassing this class.
         * 
         * @param <VS>
         * @param <ES>
         * @param <ST>
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * 
         *         TODO This should use the TLBFactory when we change to use
         *         stream solutions out of the SERVICE (#507), but the TLB class
         *         is not necessary until the reduce itself runs in concurrent
         *         threads (it is single threaded right now based on the backing
         *         CHM iterator).
         */
        public static class BindingSetReducer<VS, ES, ST> implements
                IReducer<VS, ES, ST, IBindingSet[]> {

            /**
             * The declared output variables (the ones that the caller wants to
             * extract). Any position that will not be extracted is a
             * <code>null</code>.
             */
            private final IVariable<?>[] outVars;

            /**
             * The KB instance.
             */
            private final AbstractTripleStore store;
            
            private final LexiconRelation lex;
            
            /**
             * The object used to create the variable bindings.
             */
            private final ValueFactory vf;
            
            /**
             * The list of objects used to extract the variable bindings.
             */
            private final List<IBinder<VS, ES, ST>> binderList;
            
            /**
             * The collected solutions.
             */
            private final List<IBindingSet> tmp = new LinkedList<IBindingSet>();
            
            /**
             * 
             * @param outVars
             *            The declared output variables (the ones that the
             *            caller wants to extract). Any position that will not
             *            be extracted is a <code>null</code>.
             */
            public BindingSetReducer(//
                    final IVariable<?>[] outVars,
                    final AbstractTripleStore store,
                    final IGASProgram<VS, ES, ST> gasProgram,
                    final IGASContext<VS, ES, ST> ctx) {

                this.outVars = outVars;

                this.store = store;

                this.lex = store.getLexiconRelation();
                
                this.vf = store.getValueFactory();
                
                this.binderList = gasProgram.getBinderList();

            }
            
            @Override
            public void visit(final IGASState<VS, ES, ST> state, final Value u) {

                final List<IBindingSet> bSets = new LinkedList<IBindingSet>();
                
                bSets.add(new ListBindingSet());
                
                for (IBinder<VS, ES, ST> b : binderList) {

                    // The variable for this binder.
                    final IVariable<?> var = outVars[b.getIndex()];

                    if(var == null)
                        continue;

                	final Iterator<IBindingSet> it = bSets.iterator();
                	
                	final List<IBindingSet> bSets2 = new LinkedList<IBindingSet>();
                	
                	while (it.hasNext()) {
                		
                		final IBindingSet parent = it.next();
                		
						if (log.isTraceEnabled())
							log.trace("parent: " + parent);
                		
                    	final List<Value> vals = 
                    			b.bind(vf, state, u, outVars, parent);
                    	
                    	if (vals.size() == 0) {
                    		
                    		// do nothing, leave the parent in the bSets
                    		
                    	} else if (vals.size() == 1) {
                    		
                    		/*
                    		 * Bind the single value, leave the parent in the 
                    		 * bSets.
                    		 */
                    		
                    		final Value val = vals.get(0);
                    		
                    		bind(var, val, parent);
                    		
							if (log.isTraceEnabled())
								log.trace("parent (after bind): " + parent);
                    		
                    	} else {
                    	
                    		/* 
                    		 * Remove the parent from the bSets, for each new
                    		 * value, clone the parent, bind the value, and add
                    		 * the new solution to the bSets
                    		 */
                    		
                    		for (Value val : vals) {
                    			
                    			final IBindingSet child = parent.clone();
                    			
                    			bind(var, val, child);
                    			
    							if (log.isTraceEnabled())
    								log.trace("child: " + child);
                        		
                    			bSets2.add(child);
                    			
                    		}
                    		
                    		it.remove();
                    		
                    	}
                		
                	}

                	bSets.addAll(bSets2);
                	
                }

                // Add to the set of generated solutions.
                tmp.addAll(bSets);

            }

            @SuppressWarnings({ "unchecked", "rawtypes" })
			protected void bind(final IVariable<?> var, final Value val, final IBindingSet bs) {
            	
                if (val == null)
                    return;

                if (val instanceof IV) {

                    // The value is already an IV.
                    bs.set(var, new Constant((IV) val));

                } else {

                    /*
                     * The Value is a BigdataValueImpl (if the bind() method
                     * used the supplied ValueFactory). We need to convert
                     * it to an IV and this code ASSUMES that we can do this
                     * using an inline IV with the as configured KB. (This
                     * will work for anything numeric, but not for strings.)
                     */
                    final IV<BigdataValueImpl, ?> iv = lex
                            .getLexiconConfiguration().createInlineIV(val);

                    if (iv != null) {

                    	iv.setValue((BigdataValueImpl) val);

                    	bs.set(var, new Constant(iv));
                    	
                    } else if (val instanceof BigdataValue) {
                    	
                    	bs.set(var, new Constant(DummyConstantNode.toDummyIV((BigdataValue) val)));
                    	
                    } else {
                    	
                    	throw new RuntimeException("FIXME");
                    	
                    }

                }

            }
            
            @Override
            public IBindingSet[] get() {

                return tmp.toArray(new IBindingSet[tmp.size()]);

            }

        }
        
        /**
         * Factory for the {@link IGASEngine}.
         */
        private IGASEngine newGasEngine(final IIndexManager indexManager,
                final int nthreads) {

            return new BigdataGASEngine(indexManager, nthreads);

        }

        /**
         * Return an instance of the {@link IGASProgram} to be evaluated.
         */
        private IGASProgram<VS, ES, ST> newGASProgram(
                final Class<IGASProgram<VS, ES, ST>> cls) {

            if (cls == null)
                throw new IllegalArgumentException();

            try {

                final Constructor<IGASProgram<VS, ES, ST>> ctor = cls
                        .getConstructor(new Class[] {});

                final IGASProgram<VS, ES, ST> gasProgram = ctor
                        .newInstance(new Object[] {});

                return gasProgram;

            } catch (Exception e) {
                
                throw new RuntimeException(e);
                
            }
            
        }

        /**
         * Return the object used to access the as-configured graph.
         */
        private IGraphAccessor newGraphAccessor(final AbstractTripleStore kb) {

            /*
             * Use a read-only view (sampling depends on access to the BTree rather
             * than the ReadCommittedIndex).
             */
            final BigdataGraphAccessor graphAccessor = new BigdataGraphAccessor(
                    kb.getIndexManager(), kb.getNamespace(), kb
                            .getIndexManager().getLastCommitTime());

            return graphAccessor;
            
        }

    }

}
