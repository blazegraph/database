package com.bigdata.rdf.graph.impl;

import java.util.concurrent.Callable;

import com.bigdata.rdf.internal.IV;

/**
 * A factory for tasks that are applied to each vertex in the frontier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public interface VertexTaskFactory<T> {

    /**
     * Return a new task that will evaluate the vertex.
     * 
     * @param u
     *            The vertex to be evaluated.
     * 
     * @return The task.
     */
    Callable<T> newVertexTask(@SuppressWarnings("rawtypes") IV u);

}
