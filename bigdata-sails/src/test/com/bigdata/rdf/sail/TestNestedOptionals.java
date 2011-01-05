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
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.SingletonSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Unit tests the optionals aspects of the {@link BigdataSail} implementation.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestNestedOptionals extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(TestNestedOptionals.class);

    protected static final boolean INFO = log.isInfoEnabled();
    
    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();

        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    /**
     * 
     */
    public TestNestedOptionals() {
    }

    /**
     * @param arg0
     */
    public TestNestedOptionals(String arg0) {
        super(arg0);
    }
    
    public void testNestedOptionals() throws Exception {
    	
        final Sail sail = new MemoryStore();
        sail.initialize();
        final Repository repo = new SailRepository(sail);
        final RepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();

            /*
             * Create some terms.
             */
            final URI john = vf.createURI(BD.NAMESPACE + "john");
            final URI mary = vf.createURI(BD.NAMESPACE + "mary");
            final URI leon = vf.createURI(BD.NAMESPACE + "leon");
            final URI paul = vf.createURI(BD.NAMESPACE + "paul");
            final URI brad = vf.createURI(BD.NAMESPACE + "brad");
            final URI fred = vf.createURI(BD.NAMESPACE + "fred");
            final URI knows = vf.createURI(BD.NAMESPACE + "knows");

            /*
             * Create some statements.
             */
            cxn.add(paul, knows, mary);
            cxn.add(paul, knows, brad);
            
            cxn.add(john, knows, mary);
            cxn.add(john, knows, brad);

            cxn.add(mary, knows, brad);
            cxn.add(brad, knows, fred);
            cxn.add(brad, knows, leon);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.commit();
            
            {
            	
	            String query =
	            	"prefix bd: <"+BD.NAMESPACE+"> " +
	                "select * " +
	                "where { " +
	                "  ?a bd:knows ?b . " +
	                "  OPTIONAL { " +
	                "    ?b bd:knows ?c . " +
	                "    ?c bd:knows ?d . " +
	                "  } " +
	                "}"; 
	
	            final SailTupleQuery tupleQuery = (SailTupleQuery)
	                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
	            tupleQuery.setIncludeInferred(false /* includeInferred */);
	            
	            if (INFO) {
	            	log.info(query);
	                final TupleQueryResult result = tupleQuery.evaluate();
	                while (result.hasNext()) {
	                    log.info(result.next());
	                }
	            }
	            
	            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
	            answer.add(createBindingSet());
	            
	//                result = tupleQuery.evaluate();
	//                compare(result, answer);

            }
            
            {
            	
	            String query =
	            	"prefix bd: <"+BD.NAMESPACE+"> " +
	                "select * " +
	                "where { " +
	                "  ?a bd:knows ?b . " +
	                "  OPTIONAL { " +
	                "    ?b bd:knows ?c . " +
	                "    ?c bd:knows ?d . " +
	                "    filter(?a != bd:paul) " +
	                "  } " +
	                "}"; 
	
	            final SailTupleQuery tupleQuery = (SailTupleQuery)
	                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
	            tupleQuery.setIncludeInferred(false /* includeInferred */);
	            
	            if (INFO) {
	            	log.info(query);
	                final TupleQueryResult result = tupleQuery.evaluate();
	                while (result.hasNext()) {
	                    log.info(result.next());
	                }
	            }
	            
	            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
	            answer.add(createBindingSet());
	            
	//                result = tupleQuery.evaluate();
	//                compare(result, answer);

            }
            
            {
            	
	            String query =
	            	"prefix bd: <"+BD.NAMESPACE+"> " +
	                "select * " +
	                "where { " +
	                "  ?a bd:knows ?b . " +
	                "  OPTIONAL { " +
	                "    ?b bd:knows ?c . " +
	                "    ?c bd:knows ?d . " +
	                "    filter(?d != bd:leon) " +
	                "  } " +
	                "}"; 
	
	            final SailTupleQuery tupleQuery = (SailTupleQuery)
	                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
	            tupleQuery.setIncludeInferred(false /* includeInferred */);
	            
	            if (INFO) {
	            	log.info(query);
	                final TupleQueryResult result = tupleQuery.evaluate();
	                while (result.hasNext()) {
	                    log.info(result.next());
	                }
	            }
	            
	            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
	            answer.add(createBindingSet());
	            
	//                result = tupleQuery.evaluate();
	//                compare(result, answer);

            }
            
        } finally {
            cxn.close();
            sail.shutDown();
        }

    }

    public void testNestedOptionals1() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();

            /*
             * Create some terms.
             */
            final URI T1 = vf.createURI(BD.NAMESPACE + "T1");
            final URI T2 = vf.createURI(BD.NAMESPACE + "T2");
            final URI T3 = vf.createURI(BD.NAMESPACE + "T3");
            final URI P1 = vf.createURI(BD.NAMESPACE + "P1");
            final URI P2 = vf.createURI(BD.NAMESPACE + "P2");
            final URI P3 = vf.createURI(BD.NAMESPACE + "P3");
            final URI a = vf.createURI(BD.NAMESPACE + "a");
            final URI b = vf.createURI(BD.NAMESPACE + "b");
            final Literal ap1 = vf.createLiteral(25);
            final Literal ap2 = vf.createLiteral(100);
            final Literal ap3 = vf.createLiteral(7465);
            final Literal bp1 = vf.createLiteral(40);
            final Literal bp2 = vf.createLiteral(250);

            /*
             * Create some statements.
             */
            cxn.add(a, RDF.TYPE, T1);
            cxn.add(a, RDF.TYPE, T2);
            cxn.add(a, P1, ap1);
            cxn.add(a, P2, ap2);
            cxn.add(a, P3, ap3);
            
            cxn.add(b, RDF.TYPE, T1);
            cxn.add(b, P1, bp1);
            cxn.add(b, P2, bp2);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
            if (INFO) log.info("\n" + sail.getDatabase().dumpStore());

            // get P1 for all T1 and P2 only if also T2
            String query = 
                "select *\n" +
                "where {\n" +
                "  ?s <"+RDF.TYPE+"> <"+T1+"> .\n" +
                "  ?s <"+P1+"> ?p1 .\n" +
                "  OPTIONAL {\n" +
                "    ?s <"+RDF.TYPE+"> <"+T2+"> .\n" +
                "    ?s <"+P2+"> ?p2 .\n" +
                "    ?s <"+P3+"> ?p3 .\n" +
                "  }\n" +
                "}"; 

            final BigdataSailTupleQuery tupleQuery = (BigdataSailTupleQuery) 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(false /* includeInferred */);
            
//            if (INFO) log.info(tupleQuery.getTupleExpr());
//              
            if (INFO) {
                final TupleQueryResult result = tupleQuery.evaluate();
                while (result.hasNext()) {
                    log.info(result.next());
                }
            }
            
            final QueryRoot root = (QueryRoot) tupleQuery.getTupleExpr();
            final Projection p = (Projection) root.getArg();
            final LeftJoin leftJoin = (LeftJoin) p.getArg();
            
            final List<Op> tails = collectTails(leftJoin);
            
            if (INFO) {
                System.err.println(query);
                for (Op t : tails) {
                    System.err.println(t);    
                }
            }
              
            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet());
            
//            result = tupleQuery.evaluate();
//            compare(result, answer);
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testNestedOptionals2() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();

            /*
             * Create some terms.
             */
            final URI T1 = vf.createURI(BD.NAMESPACE + "T1");
            final URI T2 = vf.createURI(BD.NAMESPACE + "T2");
            final URI T3 = vf.createURI(BD.NAMESPACE + "T3");
            final URI T4 = vf.createURI(BD.NAMESPACE + "T4");
            final URI T5 = vf.createURI(BD.NAMESPACE + "T5");
            final URI P0 = vf.createURI(BD.NAMESPACE + "P0");
            final URI P1 = vf.createURI(BD.NAMESPACE + "P1");
            final URI P2 = vf.createURI(BD.NAMESPACE + "P2");
            final URI P3 = vf.createURI(BD.NAMESPACE + "P3");
            final URI P4 = vf.createURI(BD.NAMESPACE + "P4");
            final URI P5 = vf.createURI(BD.NAMESPACE + "P5");
            final URI P6 = vf.createURI(BD.NAMESPACE + "P6");
            final URI a = vf.createURI(BD.NAMESPACE + "a");
            final URI b = vf.createURI(BD.NAMESPACE + "b");
            final URI c = vf.createURI(BD.NAMESPACE + "c");
            final URI ap0 = vf.createURI(BD.NAMESPACE + "ap0");
            final URI ap1 = vf.createURI(BD.NAMESPACE + "ap1");
            final URI ap2 = vf.createURI(BD.NAMESPACE + "ap2");
            final URI bp0 = vf.createURI(BD.NAMESPACE + "bp0");
            final URI bp1 = vf.createURI(BD.NAMESPACE + "bp1");
            final URI bp2 = vf.createURI(BD.NAMESPACE + "bp2");
            final URI bp3 = vf.createURI(BD.NAMESPACE + "bp3");
            final URI cp0 = vf.createURI(BD.NAMESPACE + "cp0");
            final URI cp1 = vf.createURI(BD.NAMESPACE + "cp1");
            final URI cp2 = vf.createURI(BD.NAMESPACE + "cp2");
            final URI cp3 = vf.createURI(BD.NAMESPACE + "cp3");

            /*
             * Create some statements.
             */
            cxn.add(a, RDF.TYPE, T1);
            cxn.add(a, RDF.TYPE, T2);
            cxn.add(a, RDF.TYPE, T3);
            cxn.add(a, P0, ap0);
            cxn.add(a, P1, ap1);
            cxn.add(a, P2, ap2);
            
            cxn.add(b, RDF.TYPE, T1);
            cxn.add(b, RDF.TYPE, T2);
            cxn.add(b, P0, bp0);
            cxn.add(b, P1, bp1);
            cxn.add(b, P2, bp2);
            cxn.add(b, P3, bp3);

            cxn.add(c, RDF.TYPE, T1);
            cxn.add(c, RDF.TYPE, T2);
            cxn.add(c, RDF.TYPE, T3);
            cxn.add(c, P0, cp0);
            cxn.add(c, P1, cp1);
            cxn.add(c, P2, cp2);
            cxn.add(c, P3, cp3);
            
            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

            // get P1 for all T1 and P2 only if also T2 and T3 also
            // also, optionally get P3 but only if type T2 and T3 also
            String query = 
                "select *\n" +
                "where {\n" +
                "  ?s <"+RDF.TYPE+"> <"+T1+"> .\n" +         // tail=F, group=0
                "  ?s <"+P0+"> ?p0 .\n" +                    // tail=G, group=0
                "  ?s <"+P1+"> ?p1 .\n" +                    // tail=H, group=0
                "  OPTIONAL {\n" +
                "    ?s <"+RDF.TYPE+"> <"+T4+"> .\n" +       // tail=A, group=1, parent=0
                "    ?s <"+RDF.TYPE+"> <"+T5+"> .\n" +       // tail=B, group=1, parent=0
                "    ?s <"+P4+"> ?p4 .\n" +                  // tail=C, group=1, parent=0
                "    FILTER ( ?p4 > (?p1*?p0+10+20) ) .\n" +                  
                "    OPTIONAL { ?s <"+P5+"> ?p5 . }\n" +     // tail=D, group=2, parent=1
                "    OPTIONAL { ?s <"+P6+"> ?p6 . }\n" +     // tail=E, group=3, parent=1
                "  }\n" +
                "  OPTIONAL {\n" +
                "    ?s <"+RDF.TYPE+"> <"+T2+"> .\n" +       // tail=I, group=4, parent=0
                "    ?s <"+RDF.TYPE+"> <"+T3+"> .\n" +       // tail=J, group=4, parent=0
                "    ?s <"+P2+"> ?p2 .\n" +                  // tail=K, group=4, parent=0
                "    OPTIONAL { ?s <"+P3+"> ?p3 . }\n" +     // tail=L, group=5, parent=4
                "  }\n" +
                "}";
            
            /*
             *  1: F
             *  2: G
             *  3: H
             *  4: A, goto=9
             *  5: B, goto=9
             *  6: C, goto=9
             *  7: D, goto=8
             *  8: E, goto=9
             *  9: I, goto=end
             *  10: J, goto=end
             *  11: K, goto=end
             *  12: L, goto=end
             */

            final BigdataSailTupleQuery tupleQuery = (BigdataSailTupleQuery) 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(false /* includeInferred */);
            
//            if (INFO) log.info(tupleQuery.getTupleExpr());
            
//            if (INFO) {
//                final TupleQueryResult result = tupleQuery.evaluate();
//                while (result.hasNext()) {
//                    log.info(result.next());
//                }
//            }
            
            final QueryRoot root = (QueryRoot) tupleQuery.getTupleExpr();
            final Projection p = (Projection) root.getArg();
            final LeftJoin leftJoin = (LeftJoin) p.getArg();
            
            final List<Op> tails = collectTails(leftJoin);
            
            if (INFO) {
                System.err.println(query);
                for (Op t : tails) {
                    System.err.println(t);    
                }
            }
                
            
            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet());
            
//            final TupleQueryResult result = tupleQuery.evaluate();
//            compare(result, answer);
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    private List<Op> collectTails(final LeftJoin root) {
    
        final List<Op> tails = new LinkedList<Op>();
        
        log.info("\n"+root);

        collectTails(tails, root, false, 0, -1);
        
        return tails;
        
    }
    
    private int group = 0;
    public int getNextGroupId() {
        return ++group;
    }
    
    private void collectTails(final List<Op> tails, final LeftJoin leftJoin, 
            final boolean rslj, final int g, final int pg) {
        
        final ValueExpr ve = leftJoin.getCondition();
        // conditional for tails in this group
        if (ve != null) {
        	final Constraint c = new Constraint(ve);
        	c.setGroup(g);
        	c.setParentGroup(pg);
        	tails.add(c);
        }
        
        final TupleExpr left = leftJoin.getLeftArg();
        
        if (left instanceof StatementPattern) {
            collectTails(tails, (StatementPattern) left, rslj, g, pg);
        } else if (left instanceof Filter) {
            collectTails(tails, (Filter) left, rslj, g, pg);
        } else if (left instanceof Join) {
            collectTails(tails, (Join) left, rslj, g, pg);
        } else if (left instanceof LeftJoin) {
            collectTails(tails, (LeftJoin) left, rslj, g, pg);
        } else if (left instanceof SingletonSet){
            // do nothing
        } else {
            throw new RuntimeException();
        }
        
        final TupleExpr right = leftJoin.getRightArg();
        
        if (right instanceof StatementPattern) {
            collectTails(tails, (StatementPattern) right, true, getNextGroupId(), g);
        } else if (right instanceof Filter) {
            collectTails(tails, (Filter) right, true, getNextGroupId(), g);
        } else if (right instanceof Join) {
            collectTails(tails, (Join) right, true, getNextGroupId(), g);
        } else if (right instanceof LeftJoin) {
            if (left instanceof SingletonSet)
                collectTails(tails, (LeftJoin) right, true, g, pg);
            else
                collectTails(tails, (LeftJoin) right, true, getNextGroupId(), g);
        } else {
            throw new RuntimeException();
        }
        
    }
    
    private void collectTails(final List<Op> tails, final Join join, 
            final boolean rslj, final int g, final int pg) {
        
        final TupleExpr left = join.getLeftArg();
        
        if (left instanceof StatementPattern) {
            collectTails(tails, (StatementPattern) left, rslj, g, pg);
        } else if (left instanceof Filter) {
            collectTails(tails, (Filter) left, rslj, g, pg);
        } else if (left instanceof Join) {
            collectTails(tails, (Join) left, rslj, g, pg);
        } else if (left instanceof LeftJoin) {
            collectTails(tails, (LeftJoin) left, rslj, getNextGroupId(), g);
        } else {
            throw new RuntimeException();
        }
        
        final TupleExpr right = join.getRightArg();
        
        if (right instanceof StatementPattern) {
            collectTails(tails, (StatementPattern) right, rslj, g, pg);
        } else if (right instanceof Filter) {
            collectTails(tails, (Filter) right, rslj, g, pg);
        } else if (right instanceof Join) {
            collectTails(tails, (Join) right, rslj, g, pg);
        } else if (right instanceof LeftJoin) {
            collectTails(tails, (LeftJoin) right, rslj, getNextGroupId(), g);
        } else {
            throw new RuntimeException();
        }
        
    }
    
    private void collectTails(final List<Op> tails, final Filter filter, 
            final boolean rslj, final int g, final int pg) {
        
        final ValueExpr ve = filter.getCondition();
        // make a constraint, attach it to the rule
        if (ve != null) {
        	final Constraint c = new Constraint(ve);
        	c.setGroup(g);
        	c.setParentGroup(pg);
        	tails.add(c);
        }
        
        final TupleExpr arg = filter.getArg();

        if (arg instanceof StatementPattern) {
            collectTails(tails, (StatementPattern) arg, rslj, g, pg);
        } else if (arg instanceof Filter) {
            collectTails(tails, (Filter) arg, rslj, g, pg);
        } else if (arg instanceof Join) {
            collectTails(tails, (Join) arg, rslj, g, pg);
        } else if (arg instanceof LeftJoin) {
            collectTails(tails, (LeftJoin) arg, rslj, getNextGroupId(), g);
        } else {
            throw new RuntimeException();
        }
        
    }
    
    private void collectTails(final List<Op> tails, final StatementPattern sp, 
            final boolean rslj, final int g, final int pg) {

        final Tail t = new Tail(sp);
        t.setGroup(g);
        t.setParentGroup(pg);
        t.setOptional(rslj);
        tails.add(t);
        
    }
    
    private static interface Op {
    	
    	void setGroup(int g);
    	
    	int getGroup();
    	
    	void setParentGroup(int pg);
    	
    	int getParentGroup();
    	
    }
    
    private static class Tail implements Op {
        
        private StatementPattern sp;
        
        private int group, parent;
        
        private boolean optional;
        
        public Tail(StatementPattern sp) {
            
            this.sp = sp;
            
        }
        
        public void setGroup(final int group) {
            
            this.group = group;
            
        }
        
        public int getGroup() {
            
            return group;
            
        }
        
        public void setParentGroup(final int parent) {
            
            this.parent = parent;
            
        }
        
        public int getParentGroup() {
            
            return parent;
            
        }
        
        public void setOptional(final boolean optional) {
            
            this.optional = optional;
            
        }
        
        public boolean getOptional() {
            
            return optional;
            
        }
        
        public String toString() {

            StringBuilder sb = new StringBuilder();
            
            sb.append("Tail: optional=").append(optional);
            sb.append(", group=").append(group);
            sb.append(", parent=").append(parent);
            sb.append(", (");
            sb.append(toString(sp.getSubjectVar())).append(" ");
            sb.append(toString(sp.getPredicateVar())).append(" ");
            sb.append(toString(sp.getObjectVar())).append(")");
            
            return sb.toString();
            
        }
        
        private String toString(Var v) {
            
            return v.hasValue() ? 
                    v.getValue().stringValue().substring(v.getValue().stringValue().indexOf('#')) 
                    : "?"+v.getName();
            
        }
        
    }
    
    private static class Constraint implements Op {
        
        private ValueExpr ve;
        
        private int group, parent;
        
        public Constraint(ValueExpr ve) {
            
            this.ve = ve;
            
        }
        
        public void setGroup(final int group) {
            
            this.group = group;
            
        }
        
        public int getGroup() {
            
            return group;
            
        }
        
        public void setParentGroup(final int parent) {
            
            this.parent = parent;
            
        }
        
        public int getParentGroup() {
            
            return parent;
            
        }
        
        public String toString() {

            StringBuilder sb = new StringBuilder();
            
            sb.append("Constraint: group=").append(group);
            sb.append(", parent=").append(parent);
            sb.append(", filter=").append(ve);
            
            return sb.toString();
            
        }
        
    }
    
}
