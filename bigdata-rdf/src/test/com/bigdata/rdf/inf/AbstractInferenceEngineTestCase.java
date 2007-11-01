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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf.inf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import org.openrdf.sesame.admin.UpdateException;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.RdfRepository;
import org.openrdf.sesame.sail.SailInitializationException;
import org.openrdf.sesame.sailimpl.memory.RdfSchemaRepository;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;

/**
 * Base class for test suites for inference engine and the magic sets
 * implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractInferenceEngineTestCase extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public AbstractInferenceEngineTestCase() {
    }

    /**
     * @param name
     */
    public AbstractInferenceEngineTestCase(String name) {
        super(name);
    }
    
    /**
     * Read the resource into an {@link RdfSchemaRepository}. This
     * automatically computes the closure of the told triples.
     * <p>
     * Note: We treat the closure as computed by the {@link RdfSchemaRepository}
     * as if it were ground truth.
     * 
     * @return The {@link RdfSchemaRepository} with the loaded resource.
     * 
     * @throws SailInitializationException
     * @throws IOException
     * @throws UpdateException
     */
    protected RdfRepository getGroundTruth(String resource, String baseURL,
            RDFFormat format) throws SailInitializationException, IOException,
            UpdateException {

        return getGroundTruth(new String[] { resource },
                new String[] { baseURL }, new RDFFormat[] { format });
        
    }

    /**
     * Read the resource(s) into an {@link RdfSchemaRepository}. This
     * automatically computes the closure of the told triples.
     * <p>
     * Note: We treat the closure as computed by the {@link RdfSchemaRepository}
     * as if it were ground truth.
     * 
     * @return The {@link RdfSchemaRepository} with the loaded resource.
     * 
     * @throws SailInitializationException
     * @throws IOException
     * @throws UpdateException
     */
    protected RdfRepository getGroundTruth(String[] resource, String[] baseURL,
            RDFFormat[] format) throws SailInitializationException, IOException,
            UpdateException {

        assert resource.length == baseURL.length;
        
        RdfRepository repo = new org.openrdf.sesame.sailimpl.memory.RdfSchemaRepository();

        repo.initialize(new HashMap());
        
        for(int i=0; i<resource.length; i++) {
            
            upload(repo, resource[i], baseURL[i], format[i]);
            
        }

        return repo;
        
    }

}
