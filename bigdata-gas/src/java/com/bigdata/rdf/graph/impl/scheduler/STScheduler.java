/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
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

    /**
     * The scheduled vertices.
     */
    private final Set<Value> vertices;
    private final boolean sortFrontier;
    
    public STScheduler(final GASEngine gasEngine) {

        this.vertices = new LinkedHashSet<Value>();
        this.sortFrontier = gasEngine.getSortFrontier();
    
    }
    
    /**
     * The #of vertices in the frontier.
     */
    public int size() {

        return vertices.size();
        
    }

    /**
     * The backing collection.
     */
    public Set<Value> getVertices() {
        
        return vertices;
        
    }
    
    @Override
    public void schedule(final Value v) {
    
        vertices.add(v);
        
    }

    @Override
    public void compactFrontier(final IStaticFrontier frontier) {

        frontier.resetFrontier(vertices.size()/* minCapacity */, sortFrontier,
                vertices.iterator());

    }

    @Override
    public void clear() {
        
        vertices.clear();
        
    }
    
}