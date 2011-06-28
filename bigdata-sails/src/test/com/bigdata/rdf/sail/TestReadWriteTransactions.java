/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestReadWriteTransactions extends ProxyBigdataSailTestCase {

    /**
     * 
     */
    public TestReadWriteTransactions() {
    }

    /**
     * @param arg0
     */
    public TestReadWriteTransactions(String arg0) {
        super(arg0);
    }

    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();
        
        props.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }
    
    private URI uri(String s) {
        return new URIImpl(BD.NAMESPACE + s);
    }
    
    private BNode bnode(String id) {
        return new BNodeImpl(id);
    }
    
    private Statement stmt(Resource s, URI p, Value o) {
        return new StatementImpl(s, p, o);
    }
    
    private Statement stmt(Resource s, URI p, Value o, Resource c) {
        return new ContextStatementImpl(s, p, o, c);
    }
    
    /**
     * Test the commit semantics in the context of a read-committed view of the
     * database.
     */
    public void test_commit() throws Exception {

        // final LocalTripleStore store = (LocalTripleStore) getStore();
        final BigdataSail sail = getSail();
        try {
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection isolated = 
            (BigdataSailRepositoryConnection) repo.getReadWriteConnection();
        isolated.setAutoCommit(false);
//        final BigdataSailRepositoryConnection unisolated = 
//            (BigdataSailRepositoryConnection) repo.getUnisolatedConnection();
//        unisolated.setAutoCommit(false);


        // read-committed view of the same database.
        // final AbstractTripleStore view = store.asReadCommittedView();
        RepositoryConnection readView = 
            (BigdataSailRepositoryConnection) repo.getReadOnlyConnection();

        try {

            // final long s = 1, p = 2, o = 3;
            final URI s = uri("a"), p = uri("b"), o = uri("c");

            // add the statement.
//            store.addStatements(new SPO[] { //
//                    new SPO(s, p, o, StatementEnum.Explicit) //
//                    },//
//                    1);
            isolated.add(stmt(s, p, o));
            
//            final boolean stmtInUnisolated = unisolated.hasStatement(s, p, o, true);
//
//            if(log.isInfoEnabled()) log.info("stmtInUnisolated: " + stmtInUnisolated);

            final boolean stmtInIsolated = isolated.hasStatement(s, p, o, true);

            if(log.isInfoEnabled()) log.info("stmtInIsolated: " + stmtInIsolated);

            final boolean stmtInView = readView.hasStatement(s, p, o, true);

            if(log.isInfoEnabled()) log.info("stmtInView: " + stmtInView);

//            // not visible in the repo.
//            assertFalse(stmtInUnisolated);

            // not visible in the view.
            assertFalse(stmtInView);

            // visible in the transaction.
            assertTrue(stmtInIsolated);

            // commit the transaction.
            isolated.commit();

            // verify that the write was published.
            readView = repo.getReadOnlyConnection();
            
            // now visible in the view
            /*
             * Note: this will fail if the Journal#getIndex(name,timestamp) does
             * not return an index view with read-committed (vs read-consistent)
             * semantics. For the index view to have read-committed semantics
             * the view MUST update if there is an intervening commit. This is
             * currently handled by returning a ReadCommittedView for this case
             * rather than a BTree.
             */
            assertTrue(readView.hasStatement(s, p, o, true));
            
        } finally {

            readView.close();
            isolated.close();
//            unisolated.close();

        }
        } finally {
            sail.__tearDownUnitTest();
        }
        
    }
    
    /**
     * Test of abort semantics.
     */
    public void test_abort() throws Exception {

        class AbortException extends RuntimeException {
            private static final long serialVersionUID = 1L;
        }

//        final LocalTripleStore store = (LocalTripleStore) getStore();
        final BigdataSail sail = getSail();
        try {
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final RepositoryConnection store = repo.getReadWriteConnection();
        store.setAutoCommit(false);

        // Should be a nop.
        store.rollback();
        
//        final long s = 1, p = 2, o = 3;
        final URI s = uri("a"), p = uri("b"), o = uri("c");

        try {
            
            // add the statement.
//            store.addStatements(new SPO[] { //
//                    new SPO(s, p, o, StatementEnum.Explicit) //
//                    },//
//                    1);
            store.add(stmt(s, p, o));

            // visible in the repo.
            assertTrue(store.hasStatement(s, p, o, true));
            
            // discard the write set.
            store.rollback();

            // no longer visible in the repo.
            assertFalse(store.hasStatement(s, p, o, true));

        } finally {

            store.close();
            
        }
        } finally {
            sail.__tearDownUnitTest();
        }

    }

    public void test_multiple_transaction() throws Exception {

        // final LocalTripleStore store = (LocalTripleStore) getStore();
        final BigdataSail sail = getSail();
        try {
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final RepositoryConnection tx1 = repo.getReadWriteConnection();
        tx1.setAutoCommit(false);
        final RepositoryConnection tx2 = repo.getReadWriteConnection();
        tx2.setAutoCommit(false);

        try {

            // final long s = 1, p = 2, o = 3;
            final URI s = uri("a"), p = uri("b"), o = uri("c");

            final BNode sid1 = bnode("1"), sid2 = bnode("2");

            final URI author = uri("author"), mike = uri("mike"), bryan = uri("bryan");
            
            // add the statement.
//            store.addStatements(new SPO[] { //
//                    new SPO(s, p, o, StatementEnum.Explicit) //
//                    },//
//                    1);
            tx1.add(stmt(s, p, o, sid1));
            
            tx1.add(stmt(sid1, author, mike));
            
            tx1.commit();
            
            assertTrue(tx1.hasStatement(s, p, o, true));

            assertFalse(tx2.hasStatement(s, p, o, true));

            tx2.add(stmt(s, p, o, sid2));
            
            tx2.add(stmt(sid2, author, bryan));
            
            tx2.commit();
            
            assertTrue(tx2.hasStatement(s, p, o, true));
            
            RepositoryConnection view = repo.getReadOnlyConnection();

            assertTrue(view.hasStatement(s, p, o, true));
            
            RepositoryResult<Statement> it = view.getStatements(s, p, o, false);
            
            BigdataStatement stmt = (BigdataStatement) it.next();
            
            Resource sid = stmt.getContext();
            
            assertTrue(view.hasStatement(sid, author, mike, true));
            
            assertTrue(view.hasStatement(sid, author, bryan, true));
            
            view.close();

        } finally {

            tx1.close();
            tx2.close();

        }
        } finally {

            sail.__tearDownUnitTest();

        }
        
    }
    
//    /**
//     * @todo Need to test how the system reacts to concurrent reads and writes.
//     * Simulate real work load.  Incremental writes, outnumbered by reads by
//     * some factor, say somewhere between 10 to 100.  Write size will vary, say 
//     * from 1 to 10000 statements.
//     * <p>
//     * How should we generate the data?
//     * N write tasks
//     * each write tasks update information about one "entity"
//     * each "entity" has P properties
//     * each property has A chance of being an attribute and (1-A) chance of being a link to another "entity"
//     * attribute predicate and link predicate can be constants
//     * attribute value is a randomly assigned literal
//     * link value is randomly chosen from pool of already created "entities"  
//     * 
//     * @throws Exception
//     */
//    public void test_stress() throws Exception {
//        
//        final BigdataSail sail = getSail();
//        sail.initialize();
//        final BigdataSailRepository repo = new BigdataSailRepository(sail);
//
//        try {
//
//            
//            
//        } finally {
//
//            sail.__tearDownUnitTest();
//
//        }
//        
//    }
    
}
