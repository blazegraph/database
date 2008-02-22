/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
/*
 * Created on Feb 22, 2008
 */

package com.bigdata.journal;

import java.util.Properties;

import com.bigdata.repo.BigdataRepository.Options;
import com.bigdata.util.MillisecondTimestampFactory;

/**
 * Base class for {@link ResourceManager} test suites that can use normal
 * startup and shutdown.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractResourceManagerTestCase extends
        AbstractResourceManagerBootstrapTestCase {

    /**
     * 
     */
    public AbstractResourceManagerTestCase() {
        super();
    }

    /**
     * @param arg0
     */
    public AbstractResourceManagerTestCase(String arg0) {
        super(arg0);
    }

    /**
     * Forces the use of persistent journals so that we can do overflow
     * operations and the like.
     */
    public Properties getProperties() {
        
        Properties properties = new Properties( super.getProperties() );
        
        // Note: test requires data on disk.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                .toString());
        
        return properties;
        
    }
    
    protected ResourceManager resourceManager;
    protected ConcurrencyManager concurrencyManager;
    protected AbstractLocalTransactionManager localTransactionManager;
    protected static final MillisecondTimestampFactory timestampFactory = new MillisecondTimestampFactory();

    public void setUp() throws Exception {
        
        super.setUp();

        Properties properties = getProperties();
        
        resourceManager = new ResourceManager(properties);
        
        localTransactionManager = new AbstractLocalTransactionManager(resourceManager) {

            public long nextTimestamp() {

                return timestampFactory.nextMillis();
                
            }
            
        };
        
        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, resourceManager);

        localTransactionManager.setConcurrencyManager(concurrencyManager);

        resourceManager.setConcurrencyManager(concurrencyManager);
        
        resourceManager.start();
        
    }

    public void tearDown() throws Exception {

        shutdownNow();
        
        if (resourceManager != null)
            resourceManager.destroyAllResources();

    }

    /**
     * Polite shutdown does not accept new requests and will shutdown once the
     * existing requests have been processed.
     * <p>
     * Note: The {@link IConcurrencyManager} is shutdown first, then the
     * {@link ITransactionManager} and finally the {@link IResourceManager}.
     */
    public void shutdown() {

        concurrencyManager.shutdown();

        localTransactionManager.shutdown();

        resourceManager.shutdown();

    }

    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon as
     * possible.
     * <p>
     * Note: The {@link IConcurrencyManager} is shutdown first, then the
     * {@link ITransactionManager} and finally the {@link IResourceManager}.
     */
    public void shutdownNow() {

        if (concurrencyManager != null)
            concurrencyManager.shutdownNow();

        if (localTransactionManager != null)
            localTransactionManager.shutdownNow();

        if (resourceManager != null)
            resourceManager.shutdownNow();

    }

}
