/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.rdf.sail.webapp;

import org.eclipse.jetty.webapp.WebAppContext;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Unit tests for the {@link NanoSparqlServer} with a focus on the ability to
 * override the init parameters, the default http port, etc. This test suite is
 * written without the proxy mechanisms to make this easier to debug.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestNanoSparqlServer extends AbstractTestNanoSparqlServer {

    

//    protected AbstractTripleStore createTripleStore(
//            final IIndexManager indexManager, final String namespace,
//            final Properties properties) {
//        
//        if(log.isInfoEnabled())
//            log.info("KB namespace=" + namespace);
//    
//        // Locate the resource declaration (aka "open"). This tells us if it
//        // exists already.
//        AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
//                .getResourceLocator().locate(namespace, ITx.UNISOLATED);
//    
//        if (tripleStore != null) {
//    
//            fail("exists: " + namespace);
//            
//        }
//    
//        /*
//         * Create the KB instance.
//         */
//    
//        if (log.isInfoEnabled()) {
//            log.info("Creating KB instance: namespace="+namespace);
//            log.info("Properties=" + properties.toString());
//        }
//    
//        if (indexManager instanceof Journal) {
//    
//            // Create the kb instance.
//            tripleStore = new LocalTripleStore(indexManager, namespace,
//                    ITx.UNISOLATED, properties);
//    
//        } else {
//    
//            tripleStore = new ScaleOutTripleStore(indexManager, namespace,
//                    ITx.UNISOLATED, properties);
//        }
//    
//        // create the triple store.
//        tripleStore.create();
//    
//        if(log.isInfoEnabled())
//            log.info("Created tripleStore: " + namespace);
//    
//        // New KB instance was created.
//        return tripleStore;
//    
//    }

//    protected void dropTripleStore(final IIndexManager indexManager,
//            final String namespace) {
//
//        if (log.isInfoEnabled())
//            log.info("KB namespace=" + namespace);
//    
//        // Locate the resource declaration (aka "open"). This tells us if it
//        // exists already.
//        final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
//                .getResourceLocator().locate(namespace, ITx.UNISOLATED);
//    
//        if (tripleStore != null) {
//    
//            if (log.isInfoEnabled())
//                log.info("Destroying: " + namespace);
//    
//            tripleStore.destroy();
//            
//        }
//    
//    }

    /**
     * Simple start/kill in which we verify that the default KB was NOT created
     * and that the explicitly create KB instance can still be resolved. This is
     * basically a test of the ability to override the init parameters in
     * <code>web.xml</code> to specify the {@link ConfigParams#NAMESPACE} and
     * {@link ConfigParams#CREATE} properties. If those overrides are not
     * applied then the default KB will be created and this test will fail. If
     * the test fails, the place to look is {@link NanoSparqlServer} where it is
     * overriding the init parameters for the {@link WebAppContext}.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/730" >
     *      Allow configuration of embedded NSS jetty server using jetty-web.xml
     *      </a>
     */
    public void test_start_stop() {

        final AbstractTripleStore tripleStore = (AbstractTripleStore) m_indexManager
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        assertNotNull("Explicitly create KB not found: namespace=" + namespace,
                tripleStore);

        final AbstractTripleStore tripleStore2 = (AbstractTripleStore) m_indexManager
                .getResourceLocator().locate(
                        BigdataSail.Options.DEFAULT_NAMESPACE, ITx.UNISOLATED);

        /*
         * Note: A failure here means that our override of
         * ConfigParams.NAMESPACE was ignored.
         */
        assertNull("Default KB should not exist.", tripleStore2);

    }

}
