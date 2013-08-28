package com.bigdata.rdf.graph.impl.scheduler;

import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.GASEngine;
import com.bigdata.rdf.internal.IV;

/**
 * A simple scheduler based on a {@link ConcurrentHashMap}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
@SuppressWarnings("rawtypes")
public class CHMScheduler implements IGASSchedulerImpl {

    private final ConcurrentHashMap<IV,IV> vertices;

    public CHMScheduler(final GASEngine gasEngine) {

        vertices = new ConcurrentHashMap<IV,IV>(gasEngine.getNThreads());

    }

    @Override
    public void schedule(final IV v) {

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