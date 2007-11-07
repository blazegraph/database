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
 * Created on Nov 7, 2007
 */

package com.bigdata.rdf.sail;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import junit.framework.TestCase;

import org.openrdf.model.Statement;
import org.openrdf.sesame.admin.RdfAdmin;
import org.openrdf.sesame.admin.StdOutAdminListener;
import org.openrdf.sesame.admin.UpdateException;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.RdfRepository;
import org.openrdf.sesame.sail.SailInitializationException;
import org.openrdf.sesame.sail.SailUtil;
import org.openrdf.sesame.sail.StatementIterator;
import org.openrdf.sesame.sailimpl.memory.RdfSchemaRepository;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Options;
import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractInferenceEngineTestCase extends TestCase {

    /**
     * 
     */
    public AbstractInferenceEngineTestCase() {
        super();
    }

    /**
     * @param arg0
     */
    public AbstractInferenceEngineTestCase(String arg0) {
        super(arg0);
    }

    public Properties getProperties() {
        
        Properties properties = new Properties();
        
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE, "true");

        properties.setProperty(Options.DELETE_ON_EXIT, "true");

        properties
                .setProperty(
                        com.bigdata.rdf.store.LocalTripleStore.Options.ISOLATABLE_INDICES,
                        "false");

        properties.setProperty(
                com.bigdata.rdf.sail.BigdataRdfRepository.Options.STORE_CLASS,
                LocalTripleStore.class.getName());

        return properties;
        
    }
    
    public AbstractTripleStore getStore() {
    
        return new LocalTripleStore(getProperties());
        
    }
    
    /*
     * compares two RDF models for equality.
     */

    /**
     * Wraps up the {@link AbstractTripleStore} as an {@link RdfRepository} to
     * facilitate using {@link #modelsEqual(RdfRepository, RdfRepository)} for
     * ground truth testing.
     * 
     * @see SailUtil#modelsEqual(org.openrdf.sesame.sail.RdfSource, org.openrdf.sesame.sail.RdfSource)
     * which might be used instead.  However, it does not provide any details on
     * how the models differ.
     */
    public boolean modelsEqual(RdfRepository expected, InferenceEngine inf)
            throws SailInitializationException {

        BigdataRdfRepository repo = new BigdataRdfRepository(inf);
        
        Properties properties = new Properties(getProperties());
        
        properties.setProperty(BigdataRdfRepository.Options.TRUTH_MAINTENANCE,
                "" + true);
        
        repo.initialize( properties );
        
        return modelsEqual(expected,repo);
        
    }
    
    /**
     * Compares two RDF graphs for equality (same statements) - does NOT handle
     * bnodes, which much be treated as variables for RDF semantics.
     * 
     * @param expected
     * 
     * @param actual
     * 
     * @return true if all statements in the expected graph are in the actual
     *         graph and if the actual graph does not contain any statements
     *         that are not also in the expected graph.
     * 
     * @todo Sesame probably bundles this logic in
     *       {@link SailUtil#modelsEqual(org.openrdf.sesame.sail.RdfSource, org.openrdf.sesame.sail.RdfSource)}
     *       in a manner that handles bnodes.
     */
    public static boolean modelsEqual(RdfRepository expected,
            BigdataRdfRepository actual) {

        Collection<Statement> expectedRepo = getStatements(expected);

        int actualSize = 0; 
        boolean sameStatements1 = true;
        {

            StatementIterator it = actual.getStatements(null, null, null);

            try {

                for (; it.hasNext();) {

                    Statement stmt = it.next();

                    if (!expected.hasStatement(stmt.getSubject(), stmt
                            .getPredicate(), stmt.getObject())) {

                        sameStatements1 = false;

                        log("Not expecting: " + stmt);

                    }

                    actualSize++; // count #of statements actually visited.
                    
                }

            } finally {

                it.close();

            }
            
            log("all the statements in actual in expected? " + sameStatements1);

        }

        int expectedSize = 0;
        boolean sameStatements2 = true;
        {

            for (Iterator<Statement> it = expectedRepo.iterator(); it.hasNext();) {

                Statement stmt = it.next();

                if (!actual.hasStatement(stmt.getSubject(),
                        stmt.getPredicate(), stmt.getObject())) {

                    sameStatements2 = false;

                    log("    Expecting: " + stmt);

                }
                
                expectedSize++; // counts statements actually visited.

            }

            log("all the statements in expected in actual? " + sameStatements2);

        }

        final boolean sameSize = expectedSize == actualSize;
        
        log("size of 'expected' repository: " + expectedSize);

        log("size of 'actual'   repository: " + actualSize);

        return sameSize && sameStatements1 && sameStatements2;

    }

    private static Collection<Statement> getStatements(RdfRepository repo) {

        /*
         * Note: do NOT use a hash set here since it will hide conceal the
         * presence of duplicate statements in either graph.
         */

        Collection<Statement> c = new LinkedList<Statement>();

        StatementIterator statIter = repo.getStatements(null, null, null);

        while (statIter.hasNext()) {

            Statement stmt = statIter.next();

            c.add(stmt);

        }

        statIter.close();

        return c;

    }

    private static void log(String s) {

        System.err.println(s);

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

    /**
     * Uploads an file into an {@link RdfRepository}.
     *
     * @see RdfAdmin
     */
    protected void upload(RdfRepository repo, String resource, String baseURL,
            RDFFormat format)
            throws IOException, UpdateException {

        InputStream rdfStream = getClass().getResourceAsStream(resource);

        if (rdfStream == null) {

            // If we do not find as a Resource then try the file system.
            rdfStream = new BufferedInputStream(new FileInputStream(resource));

        }

        try {

            RdfAdmin admin = new RdfAdmin(repo);

            final boolean validate = true;

            admin.addRdfModel(rdfStream, baseURL, new StdOutAdminListener(),
                    format, validate);

        } finally {

            rdfStream.close();

        }

    }

}
