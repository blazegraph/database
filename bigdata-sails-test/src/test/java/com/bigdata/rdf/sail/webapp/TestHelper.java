package com.bigdata.rdf.sail.webapp;

import junit.framework.TestCase;

import org.openrdf.model.Resource;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.repository.RepositoryConnection;

import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.util.httpd.Config;

/**
 * Helper class to debug the NSS by issuing commands that we can not issue
 * from the index.html page (HTTP DELETEs, etc).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHelper extends TestCase {

   /**
    * 
    * @param args
    * @throws Exception
    */
   static public void main(final String[] args) throws Exception {

      if (args.length != 1) {

         System.err.println("usage: SPARQL-Endpoint-URL");

         System.exit(1);

      }

      final String sparqlEndpointURL = args[0];

      final RemoteRepositoryManager mgr = new RemoteRepositoryManager(
            "localhost:" + Config.BLAZEGRAPH_HTTP_PORT /* serviceURLIsIngored */);

      try {

         final BigdataSailRemoteRepository repo = mgr.getRepositoryForURL(
               sparqlEndpointURL).getBigdataSailRemoteRepository();

         RepositoryConnection cxn = null;
         try {

            cxn = repo.getConnection();

            cxn.remove(null/* s */, RDF.TYPE, FOAF.PERSON, (Resource[]) null/* c */);

         } finally {

            if (cxn != null) {
               cxn.close();
               cxn = null;
            }

            repo.shutDown();

         }

      } finally {

         mgr.close();

      }

   }

}
