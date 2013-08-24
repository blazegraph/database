package com.bigdata.rdf.graph;

import com.bigdata.rdf.internal.IV;

/**
 * Interface schedules a vertex for execution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IScheduler {

    /**
     * Add the vertex to the schedule.
     * 
     * @param v
     *            The vertex.
     */
    void schedule(@SuppressWarnings("rawtypes") IV v);

}
