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
/*
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.axioms.OwlAxioms;
import com.bigdata.rdf.changesets.ChangeRecord;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.changesets.InMemChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord.ChangeAction;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.ISPO.ModifiedEnum;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.rdf.vocab.RDFSVocabulary;
import com.bigdata.striterator.ChunkedArrayIterator;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestChangeSets extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(TestChangeSets.class);
    
    public Properties getTriplesNoInference() {
        
        Properties props = super.getProperties();
        
        // triples with sids
        props.setProperty(BigdataSail.Options.QUADS, "false");
        props.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");
        
        // no inference
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    public Properties getTriplesWithInference() {
        
        Properties props = super.getProperties();
        
        // triples with sids
        props.setProperty(BigdataSail.Options.QUADS, "false");
        props.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");
        
        // no inference
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "true");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, OwlAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, RDFSVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "true");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    /**
     * 
     */
    public TestChangeSets() {
    }

    /**
     * @param arg0
     */
    public TestChangeSets(String arg0) {
        super(arg0);
    }

    public void testSimpleAdd() throws Exception {

        final BigdataSail sail = getSail(getTriplesNoInference());
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        final InMemChangeLog changeLog = new InMemChangeLog();
        cxn.setChangeLog(changeLog);
        
        try {
    
            final BigdataValueFactory vf = (BigdataValueFactory) sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            final URI a = vf.createURI(ns+"A");
            final URI b = vf.createURI(ns+"B");
            final URI c = vf.createURI(ns+"C");
            
            final BigdataStatement[] stmts = new BigdataStatement[] {
                vf.createStatement(a, RDFS.SUBCLASSOF, b),
                vf.createStatement(b, RDFS.SUBCLASSOF, c),
            };

            final BigdataStatement[] stmts2 = new BigdataStatement[] {
                vf.createStatement(a, RDFS.SUBCLASSOF, c),
            };

/**/
            cxn.setNamespace("ns", ns);

            // add the stmts[]
            
            for (BigdataStatement stmt : stmts) {
                cxn.add(stmt);
            }

            cxn.commit();//
            
            { // should see all of the stmts[] added
                
                final Collection<IChangeRecord> expected = 
                    new LinkedList<IChangeRecord>();
                for (BigdataStatement stmt : stmts) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.INSERTED));
                }
                
                compare(expected, changeLog.getLastCommit(sail.getDatabase()));
                
            }
            
            // add the stmts[] again

            for (BigdataStatement stmt : stmts) {
                cxn.add(stmt);
            }

            cxn.commit();//
            
            { // shouldn't see any change records
                
                compare(new LinkedList<IChangeRecord>(), changeLog.getLastCommit(sail.getDatabase()));
                
            }
            
            // add the stmts2[]
            
            for (BigdataStatement stmt : stmts2) {
                cxn.add(stmt);
            }

            cxn.commit();//
            
            { // should see all of the stmts2[] added
                
                final Collection<IChangeRecord> expected = 
                    new LinkedList<IChangeRecord>();
                for (BigdataStatement stmt : stmts2) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.INSERTED));
                }
                
                compare(expected, changeLog.getLastCommit(sail.getDatabase()));
                
            }
            
            if (log.isDebugEnabled()) {
                log.debug("\n" + sail.getDatabase().dumpStore(true, true, false));
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testSimpleRemove() throws Exception {

        final BigdataSail sail = getSail(getTriplesNoInference());
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        final InMemChangeLog changeLog = new InMemChangeLog();
        cxn.setChangeLog(changeLog);
        
        try {
    
            final BigdataValueFactory vf = (BigdataValueFactory) sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            final URI a = vf.createURI(ns+"A");
            final URI b = vf.createURI(ns+"B");
            final URI c = vf.createURI(ns+"C");
            
            final BigdataStatement[] stmts = new BigdataStatement[] {
                vf.createStatement(a, RDFS.SUBCLASSOF, b),
                vf.createStatement(b, RDFS.SUBCLASSOF, c),
            };

/**/
            cxn.setNamespace("ns", ns);

            // add the stmts[]
            
            for (BigdataStatement stmt : stmts) {
                cxn.add(stmt);
            }

            cxn.commit();//
            
            // remove the stmts[]

            for (BigdataStatement stmt : stmts) {
                stmt.setModified(ModifiedEnum.NONE);
                cxn.remove(stmt);
            }

            cxn.commit();//
            
            if (log.isDebugEnabled()) {
                log.debug("\ndump store:\n" + sail.getDatabase().dumpStore(true, true, false));
            }
            
            { // should see all of the stmts[] removed
                
                final Collection<IChangeRecord> expected = 
                    new LinkedList<IChangeRecord>();
                for (BigdataStatement stmt : stmts) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.REMOVED));
                }
                
                compare(expected, changeLog.getLastCommit(sail.getDatabase()));
                
            }
            
            // remove the stmts[] again

            for (BigdataStatement stmt : stmts) {
                cxn.remove(stmt);
            }

            cxn.commit();//
            
            { // shouldn't see any change records
                
                compare(new LinkedList<IChangeRecord>(), changeLog.getLastCommit(sail.getDatabase()));
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testSids() throws Exception {

        Properties props = getTriplesNoInference();
        
        if (!Boolean.valueOf(props.getProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS)).booleanValue()) {
            log.warn("cannot run this test without sids enabled");
            return;
        }
        
        final BigdataSail sail = getSail(getTriplesNoInference());
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        final InMemChangeLog changeLog = new InMemChangeLog();
        cxn.setChangeLog(changeLog);
        
        try {
    
            final BigdataValueFactory vf = (BigdataValueFactory) sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            final URI a = vf.createURI(ns+"A");
            final URI b = vf.createURI(ns+"B");
            final URI c = vf.createURI(ns+"C");
            final URI d = vf.createURI(ns+"D");
            final URI x = vf.createURI(ns+"X");
            final URI y = vf.createURI(ns+"Y");
            final URI z = vf.createURI(ns+"Z");
            final BNode sid1 = vf.createBNode();
            final BNode sid2 = vf.createBNode();
            
            final BigdataStatement[] add = new BigdataStatement[] {
                vf.createStatement(a, x, b, sid1),
                vf.createStatement(sid1, y, c),
                vf.createStatement(d, z, sid1),
            };

            final BigdataStatement[] explicitRemove = new BigdataStatement[] {
                vf.createStatement(a, x, b, sid1),
            };

            final BigdataStatement[] inferredRemove = new BigdataStatement[] {
                vf.createStatement(sid1, y, c),
                vf.createStatement(d, z, sid1),
            };
     
/**/
            cxn.setNamespace("ns", ns);

            for (BigdataStatement stmt : add) {
                cxn.add(stmt);
            }

            cxn.commit();//
            
            // resolve bnodes (sids)
            for (int i = 0; i < add.length; i++) {
                add[i] = sail.getDatabase().getStatement(add[i]);
            }
            for (int i = 0; i < explicitRemove.length; i++) {
                explicitRemove[i] = sail.getDatabase().getStatement(explicitRemove[i]);
            }
            for (int i = 0; i < inferredRemove.length; i++) {
                inferredRemove[i] = sail.getDatabase().getStatement(inferredRemove[i]);
            }

            {
                
                final Collection<IChangeRecord> expected = 
                    new LinkedList<IChangeRecord>();
                for (BigdataStatement stmt : add) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.INSERTED));
                }
                
                compare(expected, changeLog.getLastCommit(sail.getDatabase()));
            
            }
        
            for (BigdataStatement stmt : explicitRemove) {
                cxn.remove(stmt);
            }

            cxn.commit();//

            {
                
                final Collection<IChangeRecord> expected = 
                    new LinkedList<IChangeRecord>();
                for (BigdataStatement stmt : explicitRemove) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.REMOVED));
                }
                for (BigdataStatement stmt : inferredRemove) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.REMOVED));
                }
                
                compare(expected, changeLog.getLastCommit(sail.getDatabase()));
            
            }
        
            if (log.isDebugEnabled()) {
                log.debug("\n" + sail.getDatabase().dumpStore(true, true, false));
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testTMAdd() throws Exception {

        final BigdataSail sail = getSail(getTriplesWithInference());
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        final InMemChangeLog changeLog = new InMemChangeLog();
        cxn.setChangeLog(changeLog);
        
        try {

            final BigdataValueFactory vf = (BigdataValueFactory) sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            final URI a = vf.createURI(ns+"A");
            final URI b = vf.createURI(ns+"B");
            final URI c = vf.createURI(ns+"C");
            
            final BigdataStatement[] explicit = new BigdataStatement[] {
                vf.createStatement(a, RDFS.SUBCLASSOF, b),
                vf.createStatement(b, RDFS.SUBCLASSOF, c),
            };

            final BigdataStatement[] inferred = new BigdataStatement[] {
                vf.createStatement(a, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(a, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(a, RDFS.SUBCLASSOF, a),
                vf.createStatement(a, RDFS.SUBCLASSOF, c),
                vf.createStatement(b, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(b, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(b, RDFS.SUBCLASSOF, b),
                vf.createStatement(c, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(c, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(c, RDFS.SUBCLASSOF, c),
            };
 
            final BigdataStatement[] upgrades = new BigdataStatement[] {
                vf.createStatement(a, RDFS.SUBCLASSOF, c),
            };

/**/
            cxn.setNamespace("ns", ns);

            for (BigdataStatement stmt : explicit) {
                cxn.add(stmt);
            }

            cxn.commit();//
            
            if (log.isDebugEnabled()) {
                log.debug("\n" + sail.getDatabase().dumpStore(true, true, false));
            }

            { 
                final Collection<IChangeRecord> expected = 
                    new LinkedList<IChangeRecord>();
                for (BigdataStatement stmt : explicit) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.INSERTED));
                }
                for (BigdataStatement stmt : inferred) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.INSERTED));
                }
                
                compare(expected, changeLog.getLastCommit(sail.getDatabase()));
            }
            
            for (BigdataStatement stmt : upgrades) {
                cxn.add(stmt);
            }

            cxn.commit();//
            
            if (log.isDebugEnabled()) {
                log.debug("\n" + sail.getDatabase().dumpStore(true, true, false));
            }

            { 
                final Collection<IChangeRecord> expected = 
                    new LinkedList<IChangeRecord>();
                for (BigdataStatement stmt : upgrades) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.UPDATED));
                }
                
                compare(expected, changeLog.getLastCommit(sail.getDatabase()));
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testTMRetract() throws Exception {

        final BigdataSail sail = getSail(getTriplesWithInference());
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        final InMemChangeLog changeLog = new InMemChangeLog();
        cxn.setChangeLog(changeLog);
        
        try {
    
        	final BigdataValueFactory vf = (BigdataValueFactory) sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            final URI a = vf.createURI(ns+"A");
            final URI b = vf.createURI(ns+"B");
            final URI c = vf.createURI(ns+"C");
            
            final BigdataStatement[] explicitAdd = new BigdataStatement[] {
                vf.createStatement(a, RDFS.SUBCLASSOF, b),
                vf.createStatement(b, RDFS.SUBCLASSOF, c),
            };

            final BigdataStatement[] inferredAdd = new BigdataStatement[] {
                vf.createStatement(a, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(a, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(a, RDFS.SUBCLASSOF, a),
                vf.createStatement(a, RDFS.SUBCLASSOF, c),
                vf.createStatement(b, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(b, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(b, RDFS.SUBCLASSOF, b),
                vf.createStatement(c, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(c, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(c, RDFS.SUBCLASSOF, c),
            };
 
            final BigdataStatement[] explicitRemove = new BigdataStatement[] {
                vf.createStatement(b, RDFS.SUBCLASSOF, c),
            };

            final BigdataStatement[] inferredRemove = new BigdataStatement[] {
                vf.createStatement(a, RDFS.SUBCLASSOF, c),
                vf.createStatement(c, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(c, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(c, RDFS.SUBCLASSOF, c),
            };
     
/**/
            cxn.setNamespace("ns", ns);

            for (BigdataStatement stmt : explicitAdd) {
                cxn.add(stmt);
            }

            cxn.commit();//

            {
                
                final Collection<IChangeRecord> expected = 
                    new LinkedList<IChangeRecord>();
                for (BigdataStatement stmt : explicitAdd) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.INSERTED));
                }
                for (BigdataStatement stmt : inferredAdd) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.INSERTED));
                }
                
                compare(expected, changeLog.getLastCommit(sail.getDatabase()));
            
            }
        
            for (BigdataStatement stmt : explicitRemove) {
                cxn.remove(stmt);
            }

            cxn.commit();//

            {
                
                final Collection<IChangeRecord> expected = 
                    new LinkedList<IChangeRecord>();
                for (BigdataStatement stmt : explicitRemove) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.REMOVED));
                }
                for (BigdataStatement stmt : inferredRemove) {
                    expected.add(new ChangeRecord(stmt, ChangeAction.REMOVED));
                }
                
                compare(expected, changeLog.getLastCommit(sail.getDatabase()));
            
            }
        
            if (log.isDebugEnabled()) {
                log.debug("\n" + sail.getDatabase().dumpStore(true, true, false));
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testTMUpdate() throws Exception {

        final BigdataSail sail = getSail(getTriplesWithInference());
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        final InMemChangeLog changeLog = new InMemChangeLog();
        cxn.setChangeLog(changeLog);
        
        try {
    
        	final BigdataValueFactory vf = (BigdataValueFactory) sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            final URI a = vf.createURI(ns+"A");
            final URI b = vf.createURI(ns+"B");
            final URI c = vf.createURI(ns+"C");
            
            final BigdataStatement[] explicit = new BigdataStatement[] {
                vf.createStatement(a, RDFS.SUBCLASSOF, b),
                vf.createStatement(b, RDFS.SUBCLASSOF, c),
            };

            final BigdataStatement[] inferred = new BigdataStatement[] {
                vf.createStatement(a, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(a, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(a, RDFS.SUBCLASSOF, a),
                vf.createStatement(a, RDFS.SUBCLASSOF, c),
                vf.createStatement(b, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(b, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(b, RDFS.SUBCLASSOF, b),
                vf.createStatement(c, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(c, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(c, RDFS.SUBCLASSOF, c),
            };
 
//            final BigdataStatement[] updates = new BigdataStatement[] {
            BigdataStatement update =
                vf.createStatement(a, RDFS.SUBCLASSOF, c);
//            };

/**/
            cxn.setNamespace("ns", ns);

            for (BigdataStatement stmt : explicit) {
                cxn.add(stmt);
            }

            cxn.commit();//
            
            if (log.isDebugEnabled()) {
                log.debug("\n" + sail.getDatabase().dumpStore(true, true, false));
            }

            // test adding a statement that is already an inference - should
            // be upgraded to Explicit from Inferred
            cxn.add(update);

            cxn.commit();//
            
            if (log.isDebugEnabled()) {
                log.debug("\n" + sail.getDatabase().dumpStore(true, true, false));
            }

            { 
                final Collection<IChangeRecord> expected = 
                    new LinkedList<IChangeRecord>();
                expected.add(new ChangeRecord(update, ChangeAction.UPDATED));
                
                compare(expected, changeLog.getLastCommit(sail.getDatabase()));

                // get the latest statement type from the db
                update = sail.getDatabase().getStatement(update);
                
                assertTrue("wrong type", update.isExplicit());
            }
            
            // test removing a statement that is still provable as an 
            // inference - should be downgraded from Explicit to Inferred
            cxn.remove(update);

            cxn.commit();//
            
            if (log.isDebugEnabled()) {
                log.debug("\n" + sail.getDatabase().dumpStore(true, true, false));
            }

            { 
                final Collection<IChangeRecord> expected = 
                    new LinkedList<IChangeRecord>();
                expected.add(new ChangeRecord(update, ChangeAction.UPDATED));
                
                compare(expected, changeLog.getLastCommit(sail.getDatabase()));
                
                // get the latest statement type from the db
                update = sail.getDatabase().getStatement(update);
                
                assertTrue("wrong type", update.isInferred());
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    private void compare(final Collection<IChangeRecord> expected, 
            final Collection<IChangeRecord> actual) {
        
        final Collection<IChangeRecord> extra = new LinkedList<IChangeRecord>();
        Collection<IChangeRecord> missing = new LinkedList<IChangeRecord>();

        int resultCount = 0;
        int nmatched = 0;
        for (IChangeRecord rec : actual) {
            resultCount++;
            boolean match = false;
            if(log.isInfoEnabled())
                log.info(rec);
            Iterator<IChangeRecord> it = expected.iterator();
            while (it.hasNext()) {
                if (it.next().equals(rec)) {
                    it.remove();
                    match = true;
                    nmatched++;
                    break;
                }
            }
            if (match == false) {
                extra.add(rec);
            }
        }
        missing = expected;

        for (IChangeRecord rec : extra) {
            if (log.isInfoEnabled()) {
                log.info("extra result: " + rec);
            }
        }
        
        for (IChangeRecord rec : missing) {
            if (log.isInfoEnabled()) {
                log.info("missing result: " + rec);
            }
        }
        
        if (!extra.isEmpty() || !missing.isEmpty()) {
            fail("matchedResults=" + nmatched + ", extraResults="
                    + extra.size() + ", missingResults="
                    + missing.size());
        }
        
    }

}
