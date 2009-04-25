/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jul 25, 2007
 */

package com.bigdata.rdf.store;

import junit.framework.TestCase2;

import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.util.JiniServicesHelper;

/**
 * An abstract test harness that sets up (and tears down) the metadata and data
 * services required for a bigdata federation using JINI to handle service
 * discovery.
 * <p>
 * Note: The configuration options for the (meta)data services are set in their
 * respective <code>properties</code> files NOT by the System properties!
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractDistributedBigdataFederationTestCase extends TestCase2 {

    public AbstractDistributedBigdataFederationTestCase() {
        super();
    }
    
    public AbstractDistributedBigdataFederationTestCase(String name) {
        super(name);
    }

    protected JiniServicesHelper services = new JiniServicesHelper(
            "src/resources/config/standalone/");
    
    protected JiniClient client;
    
    protected void setUp() throws Exception {
        
        // start services.
        services.start();
        
        // expose to subclasses.
        client = services.client;
        
    }

    protected void tearDown() throws Exception {
        
        services.destroy();
        
    }
    
}
