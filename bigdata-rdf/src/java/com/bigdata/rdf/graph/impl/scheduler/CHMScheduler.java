package com.bigdata.rdf.graph.impl.scheduler;

import java.util.concurrent.ConcurrentHashMap;

import org.openrdf.model.Value;

import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.GASEngine;

/**
 * A simple scheduler based on a {@link ConcurrentHashMap}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public class CHMScheduler implements IGASSchedulerImpl {

    private final ConcurrentHashMap<Value,Value> vertices;

    public CHMScheduler(final GASEngine gasEngine) {

        vertices = new ConcurrentHashMap<Value, Value>(gasEngine.getNThreads());

    }

    @Override
    public void schedule(final Value v) {

        vertices.putIfAbsent(v,v);

    }

    @Override
    public void clear() {
        
        vertices.clear();
        
    }

    @Override
    public void compactFrontier(final IStaticFrontier frontier) {

        frontier.resetFrontier(vertices.size()/* minCapacity */,
                false/* ordered */, vertices.keySet().iterator());

    }

} // CHMScheduler