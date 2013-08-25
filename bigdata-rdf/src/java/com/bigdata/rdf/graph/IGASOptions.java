package com.bigdata.rdf.graph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;

/**
 * Interface for options that are understood by the {@link IGASEngine} and which
 * may be declared by the {@link IGASProgram}.
 * 
 * TODO Add option to order the vertices to provide a serializable execution
 * plan (like GraphChi). I believe that this reduces to computing a DAG over the
 * frontier before executing the GATHER and then executing the frontier such
 * that the parallel execution is constrained by arcs in the DAG that do not
 * have mutual dependencies.
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

}