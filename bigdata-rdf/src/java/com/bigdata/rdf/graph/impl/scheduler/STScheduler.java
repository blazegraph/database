package com.bigdata.rdf.graph.impl.scheduler;

import java.util.LinkedHashSet;
import java.util.Set;

import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.GASEngine;
import com.bigdata.rdf.graph.impl.util.GASImplUtil;
import com.bigdata.rdf.internal.IV;

import cutthecrap.utils.striterators.ArrayIterator;

/**
 * A scheduler suitable for a single thread.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@SuppressWarnings("rawtypes")
public class STScheduler implements IGASSchedulerImpl {

    final Set<IV> vertices;
    
    public STScheduler(final GASEngine gasEngine) {

        this.vertices = new LinkedHashSet<IV>();
    
    }
    
    @Override
    public void schedule(final IV v) {
    
        vertices.add(v);
        
    }

    @Override
    public void compactFrontier(final IStaticFrontier frontier) {

        final IV[] a = GASImplUtil.compactAndSort(vertices);

        frontier.resetFrontier(a.length, new ArrayIterator<IV>(a));

    }

    @Override
    public void clear() {
        
        vertices.clear();
        
    }
    
}