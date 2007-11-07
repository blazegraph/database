/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 25, 2007
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.openrdf.sesame.sail.RdfSchemaRepository;
import org.openrdf.sesame.sail.SailInitializationException;

import com.bigdata.rdf.store.TestLocalTripleStore;

/**
 * Runs the Sesame 1.x test suite for the {@link RdfSchemaRepository}.
 * 
 * FIXME run for each of the triple store variants - only in
 * {@link TestLocalTripleStore} at this time.
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
