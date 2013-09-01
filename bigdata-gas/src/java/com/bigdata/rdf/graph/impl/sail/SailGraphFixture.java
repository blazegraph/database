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
package com.bigdata.rdf.graph.impl.sail;

import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.impl.sail.SAILGASEngine.SAILGraphAccessor;
import com.bigdata.rdf.graph.util.AbstractGraphFixture;

public class SailGraphFixture extends AbstractGraphFixture {
    
    private Sail sail;
    
    public SailGraphFixture() throws SailException {
        sail = new MemoryStore();
        sail.initialize();
    }
    
    /**
     * Return the provisioned {@link Sail}.
     */
    public Sail getSail() {
        return sail;
    }

    @Override
    public void destroy() throws SailException {
        sail.shutDown();
    }

    @Override
    public IGASEngine newGASEngine(int nthreads) {

        return new SAILGASEngine(nthreads);
        
    }

    @Override
    public IGraphAccessor newGraphAccessor(SailConnection cxn) {
        
        return new SAILGraphAccessor(cxn);
        
    }
    
}