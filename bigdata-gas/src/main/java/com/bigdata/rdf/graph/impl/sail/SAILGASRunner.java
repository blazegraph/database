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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.impl.sail.SAILGASEngine.SAILGraphAccessor;
import com.bigdata.rdf.graph.impl.util.GASRunnerBase;
import com.bigdata.rdf.graph.util.SailGraphLoader;

/**
 * Class for running GAS performance tests against the SAIL.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class SAILGASRunner<VS, ES, ST> extends GASRunnerBase<VS, ES, ST> {

    private static final Logger log = Logger.getLogger(SAILGASRunner.class);

    public SAILGASRunner(String[] args) throws ClassNotFoundException {
        super(args);
    }

    protected class SAILOptionData extends GASRunnerBase<VS, ES, ST>.OptionData {

        private Sail sail = null;
        
        private SailConnection cxn = null;
        
        @Override
        public void init() throws Exception {

            super.init();

            sail = new MemoryStore();

            sail.initialize();
            
            cxn = sail.getConnection();
            
        }

        @Override
        public void shutdown() {

            if (cxn != null) {

                try {

                    cxn.close();

                } catch (SailException e) {

                    log.error(e, e);

                } finally {

                    cxn = null;

                }

            }

            if (sail != null) {

                try {

                    sail.shutDown();

                } catch (SailException e) {
                    
                    log.error(e,e);

                } finally {
                
                    sail = null;
                    
                }
                
            }
            
        }
        @Override
        public boolean handleArg(final AtomicInteger i, final String[] args) {
            if (super.handleArg(i, args)) {
                return true;
            }
//            final String arg = args[i.get()];
//            if (arg.equals("-bufferMode")) {
//                final String s = args[i.incrementAndGet()];
//                bufferModeOverride = BufferMode.valueOf(s);
//            } else if (arg.equals("-namespace")) {
//                final String s = args[i.incrementAndGet()];
//                namespaceOverride = s;
//            } else {
//                return false;
//            }
            return false;
        }
        
        @Override
        public void report(final StringBuilder sb) {
            // NOP
        }
        
    } // class SAILOptionData
    
    @Override
    protected SAILOptionData newOptionData() {

        return new SAILOptionData();
        
    }

    @Override
    protected IGASEngine newGASEngine() {

        return new SAILGASEngine(getOptionData().nthreads);

    }

    @Override
    protected void loadFiles() throws Exception {

        final SAILOptionData opt = getOptionData();
        final String[] resources = opt.loadSet.toArray(new String[0]);
        
        boolean ok = false;
        SailConnection cxn = null;
        try {
            cxn = opt.cxn;
            new SailGraphLoader(cxn).loadGraph(null/* fallback */, resources);
            cxn.commit();
            ok = true;
        } finally {
            if (cxn != null) {
                if (!ok)
                    cxn.rollback();
                // Note: using the same connection, so don't close here.
//                cxn.close();
            }
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    protected SAILOptionData getOptionData() {

        return (SAILOptionData) super.getOptionData();
        
    }
    
    @Override
    protected IGraphAccessor newGraphAccessor() {

        return new SAILGraphAccessor(getOptionData().cxn,
                false/* includeInferred */, new Resource[0]/* defaultContext */);

    }

    /**
     * Performance testing harness.
     * 
     * @see #GASRunner(String[])
     */
    @SuppressWarnings("rawtypes")
    public static void main(final String[] args) throws Exception {

        new SAILGASRunner(args).call();

    }

}
