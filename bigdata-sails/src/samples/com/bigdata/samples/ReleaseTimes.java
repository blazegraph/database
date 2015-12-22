package com.bigdata.samples;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.sail.SailException;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.store.BD;
import com.bigdata.service.AbstractTransactionService;

/**
 * Example of History retention usage with Sail interface classes.
 * 
 * A sequence of commits updates the value of a statement 
 * and retains the commit time of each.
 * 
 * We can confirm that the correct data is associated with the relevant
 * commit time
 * 
 * On closing and re-opening the store, the historical data remains
 * accessible, however, the release time is recalculated when connections
 * are closed.  The code clarifies several of the issues involved.
 *
 * @see http://sourceforge.net/apps/mediawiki/bigdata/index.php?title=RetentionHistory
 * 
 * @author Martyn Cutcher
 */
public class ReleaseTimes {

		public static void main(String[] args) throws IOException, InterruptedException, SailException, RepositoryException {
			/**
			 * First create a new repository with 5000ms (5 second) retention
			 */
			BigdataSail sail = new BigdataSail(getProperties("5000"));
			sail.initialize();
			BigdataSailRepository repo = new BigdataSailRepository(sail);
	    	final BigdataSailRepositoryConnection cxn = repo.getConnection();
	        cxn.setAutoCommit(false);
				        
            final ValueFactory vf = sail.getValueFactory();

            /*
             * Create some terms.
             */
            final URI devuri = vf.createURI(BD.NAMESPACE + "developer");
            final Literal mp = vf.createLiteral("mike personick");
            final Literal bt = vf.createLiteral("bryan thompson");
            final Literal mc = vf.createLiteral("martyn cutcher");
            
            /*
             * Associate statements with recorded state
             */
            System.out.println("Creating historical states...");
            final long state1 = assertStatement(cxn, devuri, RDFS.LABEL, mp);
            final long state2 = assertStatement(cxn, devuri, RDFS.LABEL, bt);
            final long state3 = assertStatement(cxn, devuri, RDFS.LABEL, mc);      
            
            System.out.println("State1: " + state1 + ", state2: " + state2 + ", state3: " + state3);
            
            // Check historical state
            System.out.println("Checking historical states...");
            check(checkState(repo, state2, devuri, RDFS.LABEL, bt));
            check(checkState(repo, state1, devuri, RDFS.LABEL, mp));
            check(checkState(repo, state3, devuri, RDFS.LABEL, mc));
	        
            System.out.println("Shutting down...");
	        // Shutdown sail
	        cxn.close();
	        sail.shutDown();
	        	        
	        // Reopen with sufficient retention time to cover previous states
			sail = new BigdataSail(getProperties("5000"));
			sail.initialize();
			repo = new BigdataSailRepository(sail);
	    	final BigdataSailRepositoryConnection cxn2 = repo.getConnection();
	        cxn2.setAutoCommit(false);
            System.out.println("Reopened");
	        
	        // wait for data to "age"
			Thread.sleep(5000);

	        // ... confirm earliest historical access after open
	        check(checkState(repo, state2, devuri, RDFS.LABEL, bt));
            System.out.println("History retained after re-open");
	        
            // closing the connection will reset the release time so
	        // that subsequent accesses will fail
	        check(!checkState(repo, state2, devuri, RDFS.LABEL, bt));
	        check(!checkState(repo, state1, devuri, RDFS.LABEL, mp));
            System.out.println("History released after closing read-only connections");
	        
	        // this is last committed state, so will succeed
	        check(checkState(repo, state3, devuri, RDFS.LABEL, mc));
            System.out.println("History of last commit point accessible");
	        
	        // .. and will also succeed directly against READ_COMMITTED
	        check(checkState(repo, ITx.READ_COMMITTED, devuri, RDFS.LABEL, mc));
	        
	        // Updating the statement...
            assertStatement(cxn2, devuri, RDFS.LABEL, mp);
            System.out.println("Statement udated");

            // the previous historical access now fails
            check(!checkState(repo, state3, devuri, RDFS.LABEL, mc));
            System.out.println("History of last commit point of re-opened journal no longer accessible after update");

            // ..and the READ_COMMITTED is updated
            check(checkState(repo, ITx.READ_COMMITTED, devuri, RDFS.LABEL, mp));
            System.out.println("Committed state confirmed");
	        	        
	        cxn2.close();
	        sail.shutDown();

	        System.out.println("DONE");
		}
		
		private static void check(boolean checkState) {
			if (!checkState) {
				throw new AssertionError("Unexpected results");
			}
		}

		/**
		 * Define statement with a URI identifier, type and literal label
		 * 
		 * First ensure that any previous valued statement is removed.
		 * 
		 * Commit, and return the store state.
		 */
		private static long assertStatement(BigdataSailRepositoryConnection cxn, URI id,
				URI pred, Literal label) throws RepositoryException
		{
			cxn.remove(id, pred, null);
	        
			check(!cxn.getStatements(id, pred, null, false).hasNext());

			cxn.add(id, pred, label);
			
			check(cxn.getStatements(id, pred, label, false).hasNext());
	        
            // Return the time of the commit
	        return cxn.commit2();
		}

		/**
		 * To check a specific state we retrieve a readOnlyConnection from the
		 * repository for the time/state specified.
		 * 
		 * An IllegalStateException is thrown if no valid connection can be
		 * established.
		 * 
		 * We then retrieve statements from the connection, checking that only
		 * the requested statement is present.
		 */
		static boolean checkState(final BigdataSailRepository repo, final long state,  
				final URI id, final URI pred, final Literal label )
		{
			try {
				final BigdataSailRepositoryConnection cxn;
				try {
					cxn = repo.getReadOnlyConnection(state);
				} catch (IllegalStateException e) {
					return false; // invalid state!
				}
				try {
					RepositoryResult<Statement> results = cxn.getStatements(id, pred, null, false);
			        
					// nothing found
			        if (!results.hasNext()) {
			        	return false;
			        }
			        
			        // Check that result is the one expected
			        final String labelstr = label.toString();
			        final String value = results.next().getObject().toString();
			        
					if (!labelstr.equals(value)) {
						return false;
					}
					
			        // ..and that no others are returned
					return !results.hasNext();
				} finally {
					cxn.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new AssertionError("Unable to load index");
			}
		}
		
		static String filename = null;
	    static public Properties getProperties(String releaseAge) throws IOException {
	        
	        Properties properties = new Properties();
	        
	        // create temporary file for this application run
	        if (filename == null)
	        	filename = File.createTempFile("BIGDATA", "jnl").getAbsolutePath();

	        properties.setProperty(Options.FILE, filename);
	        properties.setProperty(Options.DELETE_ON_EXIT, "true");
	            
	        // Set RWStore
	        properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
	        
	        // Set minimum commit history
	        properties.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE, releaseAge);
	       
	        return properties;
	        
	    }
	}
