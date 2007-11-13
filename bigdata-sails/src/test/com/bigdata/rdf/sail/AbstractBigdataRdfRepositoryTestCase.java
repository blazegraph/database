/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
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
 * Created on Nov 13, 2007
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import junit.framework.TestCase2;

import org.openrdf.sesame.sail.SailInitializationException;

import com.bigdata.rdf.sail.BigdataRdfRepository.Options;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * Abstract base class initializes an instance of the
 * {@link BigdataRdfRepository} using a {@link LocalTripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractBigdataRdfRepositoryTestCase extends TestCase2 {

    public AbstractBigdataRdfRepositoryTestCase() {
        
    }

    public AbstractBigdataRdfRepositoryTestCase(String name) {
        
        super(name);
        
    }

    /**
     * Configures the test repository.
     */
    protected void setUp() throws Exception
    {
        
        super.setUp();
        
        Properties params = new Properties();

        params.setProperty(Options.CREATE_TEMP_FILE, "true");
        
//        params.setProperty(Options.ISOLATABLE_INDICES, "true");
        
        repo = new BigdataRdfSchemaRepository();

        try {

            repo.initialize(params);

        }

        catch (SailInitializationException ex) {

            /*
             * Note: Sesame is not provide the stack trace, so we do it
             * ourselves.
             */
            ex.printStackTrace();

            throw ex;

        }
        
    }

    protected void tearDown() throws Exception {

        // @todo close out the store.
//        if (repo != null) {
//
////            closeAndDelete();
//            
//        }
        
        super.tearDown();
        
    }
    
    protected BigdataRdfSchemaRepository repo;
    
}
