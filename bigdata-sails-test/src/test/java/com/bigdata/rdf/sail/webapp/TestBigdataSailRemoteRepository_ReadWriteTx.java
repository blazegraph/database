package com.bigdata.rdf.sail.webapp;

import java.util.Properties;

import org.openrdf.model.URI;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepositoryConnection;

import junit.framework.Test;

/**
 * An *extension* of the test suite that uses a namespace that is configured to
 * support read/write transactions.
 * <p>
 * Note: This does not change whether or not a transaction may be created, just
 * whether or not the namespace will allow an operation that is isolated by a
 * read/write transaction.
 */
public class TestBigdataSailRemoteRepository_ReadWriteTx<S extends IIndexManager>
        extends TestBigdataSailRemoteRepository<S> {

    public TestBigdataSailRemoteRepository_ReadWriteTx() {

    }

    public TestBigdataSailRemoteRepository_ReadWriteTx(final String name) {

        super(name);

    }

    public static Test suite() {

      return ProxySuiteHelper.suiteWhenStandalone(TestBigdataSailRemoteRepository_ReadWriteTx.class,
            "test.*"
//            "test_DELETE_accessPath_delete_all" 
            , TestMode.quads
//            , TestMode.sids
//            , TestMode.triples
            );
       
   }

  /**
   * Enable isolatable indices for so we can have concurrent read/write
   * transactions in the {@link RepositoryConnection}.
   */
  @Override
  public Properties getProperties() {

     final Properties p = new Properties(super.getProperties());

     p.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");

     return p;

  }

  /**
   * Basic test creates a read/write connection, issues begin(), and then
   * issues commit() on the connection.
   * 
   * TODO Test where we abort the connection. Verify write set is discarded.
   * 
   * @throws RepositoryException
   * @throws MalformedQueryException
   * @throws UpdateExecutionException
   */
      public void test_tx_begin_addStatement_commit() throws RepositoryException,
            MalformedQueryException, UpdateExecutionException {
         
if(true) return; // FIXME (***) TX TEST DISABLED

     assertFalse(cxn.isActive());

     cxn.begin();

     assertTrue(cxn.isActive());

     final URI a = cxn.getValueFactory().createURI(DEFAULT_PREFIX + "a");
     final URI b = cxn.getValueFactory().createURI(DEFAULT_PREFIX + "b");
     final URI c = cxn.getValueFactory().createURI(DEFAULT_PREFIX + "c");
     
     assertFalse(cxn.hasStatement(a, b, c, true/* includeInferred */));
     
     // Add to default graph.
     cxn.add(cxn.getValueFactory().createStatement(a,b,c));
     
     // visible inside of the connection.
     assertTrue(cxn.hasStatement(a, b, c, true/* includeInferred */));

     // not visible from a new connection.
     {
        final BigdataSailRemoteRepositoryConnection cxn2 = repo
              .getConnection();
        try {
           assertTrue(cxn2 != cxn);
           // cxn2.begin();
           assertFalse(cxn2.hasStatement(a, b, c, true/* includeInferred */));
        } finally {
           cxn2.close();
        }
     }
   
     cxn.commit();

     assertFalse(cxn.isActive());

  }

  /**
   * Basic test creates a read/write connection, issues begin(), and then
   * issues commit() on the connection.
   * 
   * TODO Test where we abort the connection. Verify write set is discarded.
   * 
   * @throws RepositoryException
   * @throws MalformedQueryException
   * @throws UpdateExecutionException
   */
      public void test_tx_begin_UPDATE_commit() throws RepositoryException,
            MalformedQueryException, UpdateExecutionException {

if(true) return; // FIXME (***) TX TEST DISABLED

     assertFalse(cxn.isActive());

     cxn.begin();

     assertTrue(cxn.isActive());

     final URI a = cxn.getValueFactory().createURI(DEFAULT_PREFIX + "a");
     final URI b = cxn.getValueFactory().createURI(DEFAULT_PREFIX + "b");
     final URI c = cxn.getValueFactory().createURI(DEFAULT_PREFIX + "c");
     
     assertFalse(cxn.hasStatement(a, b, c, true/* includeInferred */));
     
     cxn.prepareUpdate(QueryLanguage.SPARQL,
           getNamespaceDeclarations() + "INSERT DATA { :a :b :c }").execute();

     // visible inside of the connection.
     assertTrue(cxn.hasStatement(a, b, c, true/* includeInferred */));

     // not visible from a new connection.
     {
        final BigdataSailRemoteRepositoryConnection cxn2 = repo
              .getConnection();
        try {
           assertTrue(cxn2 != cxn);
           // cxn2.begin();
           assertFalse(cxn2.hasStatement(a, b, c, true/* includeInferred */));
            } finally {
               cxn2.close();
            }
         }
       
         cxn.commit();

         assertFalse(cxn.isActive());

      }

}