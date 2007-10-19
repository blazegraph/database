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
 * Created on Apr 12, 2007
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sesame.sail.RdfRepository;
import org.openrdf.sesame.sail.SailInitializationException;
import org.openrdf.sesame.sail.SailUpdateException;
import org.openrdf.vocabulary.XmlSchema;

/**
 * Test suite for {@link BigdataRdfRepository} implementations that verifies
 * the {@link RdfRepository} implementation (no entailments).
 * <p>
 * Note: This class has a dependency on the openrdf test suite since it extends
 * a test class defined by that project. If you are not trying to run the test
 * suites, then can simply ignore compile errors in this class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class TestBigdataRdfRepository extends org.openrdf.sesame.sail.RdfRepositoryTest {

    protected BigdataRdfRepository repo;
    
    /**
     * @param arg0
     */
    public TestBigdataRdfRepository(String arg0) {
        super(arg0);
    }

    /**
     * Test using a literal for the value.
     */
    public void test_addStatement_001() throws SailUpdateException {

        repo.startTransaction();

        Resource s = new URIImpl("http://www.somewhere.org");

        URI p = new URIImpl("http://www.outthere.com");

        Value o = new LiteralImpl("in the sky");

        assertFalse(repo.hasStatement(s, p, o));

        repo.addStatement(s, p, o);

        assertTrue(repo.hasStatement(s, p, o));

        repo.commitTransaction();

    }

    /**
     * Test using a literal with a language tag.
     */
    public void test_addStatement_002() throws SailUpdateException {

        repo.startTransaction();

        Resource s = new URIImpl("http://www.somewhere.org");

        URI p = new URIImpl("http://www.outthere.com");

        Value o = new LiteralImpl("in the sky", "en");

        assertFalse(repo.hasStatement(s, p, o));

        repo.addStatement(s, p, o);

        assertTrue(repo.hasStatement(s, p, o));
        
        repo.commitTransaction();
        
    }

    /** Test using a literal with a datatype URI. */    
    public void test_addStatement_003()
        throws SailUpdateException
    {
    
        repo.startTransaction();
        
        Resource s = new URIImpl("http://www.somewhere.org");

        URI p = new URIImpl("http://www.outthere.com");

        Value o = new LiteralImpl("in the sky", new URIImpl(XmlSchema.STRING));

        assertFalse(repo.hasStatement(s, p, o));

        repo.addStatement(s, p, o);

        assertTrue(repo.hasStatement(s, p, o));
        
        repo.commitTransaction();
        
    }

    /**
     * Return the properties used to initialize the {@link BigdataRdfRepository}.
     */
    abstract public Properties getProperties();
    
    /**
     * Configures the test repository.
     * <p>
     * Note: This test suite requires that the RDFS closure is NOT maintained
     * since some of the tests require RDF-only semantics and will trip up on
     * the entailments inserted into the RDF model by closure, e.g., the axoims.
     * For example, an empty repository is not empty after closure.
     */    
    protected RdfRepository _getTestRepository()
       throws SailInitializationException
    {

        repo = new BigdataRdfRepository();
        
        Properties params = getProperties();
        
        /*
         * Note: This test suite requires that the RDFS closure is NOT
         * maintained since some of the tests require RDF-only semantics and
         * will trip up on the entailments inserted into the RDF model by
         * closure, e.g., the axoims. For example, an empty repository is not
         * empty after closure.
         */

        params.put(Options.RDFS_CLOSURE, "false");
            
        try {
            
            repo.initialize( params );
        
        }
        
        catch( SailInitializationException ex ) {
        
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
