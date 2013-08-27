package com.bigdata.rdf.graph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;

import cutthecrap.utils.striterators.IStriterator;

/**
 * Interface for options that are understood by the {@link IGASEngine} and which
 * may be declared by the {@link IGASProgram}.
 * 
 * TODO Add option to order the vertices to provide a serializable execution
 * plan (like GraphChi). I believe that this reduces to computing a DAG over the
 * frontier before executing the GATHER and then executing the frontier such
 * that the parallel execution is constrained by arcs in the DAG that do not
 * have mutual dependencies. This is really an option that would be implemented
 * by the {@link IGASContext}, which would have to place a partial ordering over
 * the vertices in the frontier and then process the frontier with limited
 * parallelism based on that partial ordering.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGASOptions<VS, ES> {

    /**
     * Return the set of edges to which the GATHER is applied -or-
     * {@link EdgesEnum#NoEdges} to skip the GATHER phase.
     */
    EdgesEnum getGatherEdges();

    /**
     * Return the set of edges to which the SCATTER is applied -or-
     * {@link EdgesEnum#NoEdges} to skip the SCATTER phase.
     */
    EdgesEnum getScatterEdges();

    /**
     * Return a factory for vertex state objects.
     * <p>
     * Note: A <code>null</code> value may not be allowed in the visited vertex
     * map, so if the algorithm does not use vertex state, then the factory
     * should return a singleton instance each time it is invoked.
     */
    @SuppressWarnings("rawtypes")
    Factory<IV, VS> getVertexStateFactory();

    /**
     * Return a factory for edge state objects -or- <code>null</code> if the
     * {@link IGASProgram} does not use edge state (in which case the edge state
     * will not be allocated or maintained).
     */
    Factory<ISPO, ES> getEdgeStateFactory();

    /**
     * Return non-<code>null</code> iff there is a single link type to be
     * visited. This corresponds to a view of the graph as a sparse connectivity
     * matrix. The {@link IGASEngine} can optimize traversal patterns using the
     * <code>POS</code> index.
     * <p>
     * Note: When this option is used, the scatter and gather will not visit the
     * property set for the vertex. The graph is treated as if it were an
     * unattributed graph and only mined for the connectivity data.
     * 
     * @return The {@link IV} for the predicate that identifies the desired link
     *         type (there can be many types of links - the return value
     *         specifies which attribute is of interest).
     * 
     * @see #getLinkAttribType()
     */
    @SuppressWarnings("rawtypes")
    IV getLinkType();

//    /**
//     * Return non-<code>null</code> iff there is a single link type to be
//     * visited. This corresponds to a view of the graph as a sparse matrix where
//     * the data in the matrix provides the link weights. The type of the visited
//     * link weights is specified by the return value for this method. The
//     * {@link IGASEngine} can optimize traversal patterns using the
//     * <code>POS</code> index.
//     * <p>
//     * Note: When this option is used, the scatter and gather will not visit the
//     * property set for the vertex. The graph is treated as if it were an
//     * unattributed graph and only mined for the connectivity data.
//     * 
//     * @return The {@link IV} for the predicate that identifies the desired link
//     *         attribute type (a link can have many attributes - the return
//     *         value specifies which attribute is of interest).
//     * 
//     * @see #getLinkType()
//     */
//    IV getLinkAttribType();
//    
//    /**
//     * When non-<code>null</code>, the specified {@link Filter} will be used to
//     * restrict the visited edges. For example, you can restrict the visitation
//     * to a subset of the predicates that are of interest, to only visit edges
//     * that have link edges, to visit only select property values, etc. Some
//     * useful filters are defined in an abstract implementation of this
//     * interface.
//     * 
//     * @see #visitPropertySet()
//     */
//    IFilterTest getEdgeFilter();

    /**
     * Hook to impose a constraint on the visited edges and/or property values.
     * 
     * @param itr
     *            The iterator visiting those edges and/or property values.
     *            
     * @return Either the same iterator or a constrained iterator.
     */
    IStriterator constrainFilter(IStriterator eitr);
    
}