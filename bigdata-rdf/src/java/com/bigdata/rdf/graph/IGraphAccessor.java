package com.bigdata.rdf.graph;

import java.util.Iterator;
import java.util.Random;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

/**
 * Interface abstracts access to a backend graph implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGraphAccessor {

    /**
     * Return the edges for the vertex.
     * 
     * @param p
     *            The {@link IGASContext}.
     * @param u
     *            The vertex.
     * @param edges
     *            Typesafe enumeration indicating which edges should be visited.
     *            
     * @return An iterator that will visit the edges for that vertex.
     */
    Iterator<Statement> getEdges(IGASContext<?, ?, ?> p, Value u,
            EdgesEnum edges);

    /**
     * Hook to advance the view of the graph. This is invoked at the end of each
     * GAS computation round for a given {@link IGASProgram}.
     */
    void advanceView();

    /**
     * Return a sample (without duplicates) of vertices from the graph.
     * 
     * @param desiredSampleSize
     *            The desired sample size.
     * 
     * @return The distinct samples that were found.
     * 
     *         FIXME Specify whether the sample must be uniform over the
     *         vertices, proportional to the #of (in/out)edges for those
     *         vertices, etc. Without a clear specification, we will not
     *         have the same behavior across different backends.
     */
    Value[] getRandomSample(final Random r, final int desiredSampleSize);
    
}
