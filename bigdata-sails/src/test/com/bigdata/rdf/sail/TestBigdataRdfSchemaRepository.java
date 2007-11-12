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
 * Created on Oct 25, 2007
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.openrdf.sesame.sail.RdfSchemaRepository;
import org.openrdf.sesame.sail.SailInitializationException;

/**
 * Runs the Sesame 1.x test suite for the {@link RdfSchemaRepository}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class TestBigdataRdfSchemaRepository extends
        org.openrdf.sesame.sail.RdfSchemaRepositoryTest {

    // test fixture.
    protected BigdataRdfSchemaRepository repo;

    public TestBigdataRdfSchemaRepository(String name) {
        super(name);
    }
    
    /*
     * @todo any custom unit tests for this SAIL would go here.
     */
    
    /**
     * Return the properties used to initialize the {@link BigdataRdfRepository}.
     */
    abstract public Properties getProperties();

    /**
     * Configures the test repository.
     */
    protected RdfSchemaRepository _getTestRepository()
       throws SailInitializationException
    {
        
        Properties params = getProperties();

        /*
         * Make sure that only the RDFS axioms and rules are used since that is
         * all that the Sesame test suite is expecting.
         */
        
        params.put(com.bigdata.rdf.inf.InferenceEngine.Options.RDFS_ONLY, "true");

        RdfSchemaRepository repo = new BigdataRdfSchemaRepository();

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

        return repo;
        
    }

}
