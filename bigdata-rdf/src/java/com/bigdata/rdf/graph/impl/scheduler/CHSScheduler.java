package com.bigdata.rdf.graph.impl.scheduler;

import org.eclipse.jetty.util.ConcurrentHashSet;
import org.openrdf.model.Value;

import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.GASEngine;

/**
 * A simple scheduler based on a concurrent hash collection
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * 
 *         FIXME SCHEDULER: This is a Jetty class. Unbundle it! Use CHM
 *         instead. See {@link CHMScheduler}.
 */
public class CHSScheduler implements IGASSchedulerImpl {

    private final ConcurrentHashSet<Value> vertices;

    public CHSScheduler(final GASEngine gasEngine) {

        vertices = new ConcurrentHashSet<Value>();

    }

    @Override
    public void schedule(final Value v) {

        vertices.add(v);

    }

    @Override
    public void clear() {
        
        vertices.clear();
        
    }

    @Override
    public void compactFrontier(final IStaticFrontier frontier) {

        frontier.resetFrontier(vertices.size()/* minCapacity */,
                false/* ordered */, vertices.iterator());
        
    }

} // CHMScheduler