package com.bigdata.rdf.graph;

import org.openrdf.model.Value;

/**
 * Interface schedules a vertex for execution. This interface is exposed to the
 * {@link IGASProgram}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGASScheduler {

    /**
     * Add the vertex to the schedule.
     * 
     * @param v
     *            The vertex.
     */
    void schedule(Value v);

}
