/*
 * Copyright SYSTAP, LLC 2006-2008.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Apr 15, 2008
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.sail.BigdataSail.Options;

/**
 * Abstract test harness.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBigdataSailTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractBigdataSailTestCase() {
    }

    /**
     * @param name
     */
    public AbstractBigdataSailTestCase(String name) {
        super(name);
    }

    /*
     * test fixtures
     */
    
    BigdataSail sail;
    
    protected void setUp() throws Exception {
     
        Properties properties = new Properties();

        // transient means that there is nothing to delete after the test.
        properties.setProperty(Options.BUFFER_MODE,BufferMode.Transient.toString());
        
        // the other way to handle cleanup is to use a temp file and mark it for delete on close.
//        properties.setProperty(Options.CREATE_TEMP_FILE,"true");
//
//        properties.setProperty(Options.DELETE_ON_CLOSE,"true");
//
//        properties.setProperty(Options.DELETE_ON_EXIT,"true");

        // option to turn off closure.
//        properties.setProperty(Options.CLOSURE,ClosureEnum.None.toString());
        
        sail = new BigdataSail(properties);
        
    }
    
    protected void tearDown() throws Exception {
        
        if (sail != null) {

            sail.shutDown();
            
        }
                
    }
    
}
