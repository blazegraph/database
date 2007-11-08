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

import com.bigdata.rdf.sail.BigdataRdfRepository.Options;

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

        params.put(Options.TRUTH_MAINTENANCE, "false");

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
