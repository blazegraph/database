package com.bigdata.rdf.graph.impl.scheduler;

import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.model.Value;

import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.GASEngine;

/**
 * A scheduler suitable for a single thread.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class STScheduler implements IGASSchedulerImpl {

    final Set<Value> vertices;
    
    public STScheduler(final GASEngine gasEngine) {

        this.vertices = new LinkedHashSet<Value>();
    
    }
    
    @Override
    public void schedule(final Value v) {
    
        vertices.add(v);
        
    }

    @Override
    public void compactFrontier(final IStaticFrontier frontier) {

        frontier.resetFrontier(vertices.size()/* minCapacity */,
                false/* ordered */, vertices.iterator());

    }

    @Override
    public void clear() {
        
        vertices.clear();
        
    }
    
}