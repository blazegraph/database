package com.bigdata.rdf.sail.webapp;

import java.net.URISyntaxException;

import junit.framework.TestCase;

import org.openrdf.model.Resource;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;

/**
 * Helper class to debug the NSS by issuing commands that we can not issue
 * from the index.html page (HTTP DELETEs, etc).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHelper extends TestCase {

    static public void main(final String[] args) throws URISyntaxException,
            RepositoryException {

        if (args.length != 1) {

            System.err.println("usage: SPARQL-Endpoint-URL");

            System.exit(1);

        }

        final String sparqlEndpointURL = args[0];

        final BigdataSailRemoteRepository repo = new BigdataSailRemoteRepository(
                sparqlEndpointURL);

        RepositoryConnection cxn = null;
        try {
            
            cxn = (RepositoryConnection) repo.getConnection();

            cxn.remove(null/* s */, RDF.TYPE, FOAF.PERSON, (Resource[]) null/* c */);

        } finally {

            if (cxn != null) {
                cxn.close();
                cxn = null;
            }

            repo.shutDown();

        }

    }

}
