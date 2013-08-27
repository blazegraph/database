package com.bigdata.rdf.graph;

import java.util.Iterator;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;

/**
 * Interface abstracts access to a backend graph implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGraphAccessor {

    /**
     * Return the edges for the vertex.
     * 
     * @param p The {@link IGASProgram}
     * @param u
     *            The vertex.
     * @param edges
     *            Typesafe enumeration indicating which edges should be visited.
     * @return An iterator that will visit the edges for that vertex.
     */
    @SuppressWarnings("rawtypes")
    Iterator<ISPO> getEdges(IGASProgram<?, ?, ?> p, IV u, EdgesEnum edges);

    /**
     * Hook to advance the view of the graph. This is invoked at the end of each
     * GAS computation round for a given {@link IGASProgram}.
     */
    void advanceView();

}
