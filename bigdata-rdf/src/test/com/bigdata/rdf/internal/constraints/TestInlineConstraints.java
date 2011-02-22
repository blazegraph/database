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
 * Created on March 11, 2008
 */

package com.bigdata.rdf.internal.constraints;

import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.MathExpr.MathOp;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.bop.joinGraph.IEvaluationPlan;
import com.bigdata.bop.joinGraph.IEvaluationPlanFactory;
import com.bigdata.bop.joinGraph.fast.DefaultEvaluationPlanFactory2;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.Rule2BOpUtility;
import com.bigdata.rdf.sail.sop.SOp2BOpUtility;
import com.bigdata.rdf.sail.sop.UnsupportedOperatorException;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ProxyTestCase;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl;

/**
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id: TestOptionals.java 3149 2010-07-07 01:19:11Z mrpersonick $
 */
public class TestInlineConstraints extends ProxyTestCase {

	protected static final Logger log = Logger.getLogger(TestInlineConstraints.class);
	
    /**
     * 
     */
    public TestInlineConstraints() {
        super();
    }

    /**
     * @param name
     */
    public TestInlineConstraints(String name) {
        super(name);
    }
    
    @Override
    public Properties getProperties() {
    	final Properties props = super.getProperties();
    	props.setProperty(BigdataSail.Options.INLINE_DATE_TIMES, "true");
    	return props;
    }
    
    public void testGT() {
        
        // store with no owl:sameAs closure
        AbstractTripleStore db = getStore();

        // do not run if we are not inlining
        if (!db.getLexiconRelation().isInlineLiterals()) {
            return;
        }
        
        try {

            BigdataValueFactory vf = db.getValueFactory();
            
            final BigdataURI A = vf.createURI("http://www.bigdata.com/A");
            final BigdataURI B = vf.createURI("http://www.bigdata.com/B");
            final BigdataURI C = vf.createURI("http://www.bigdata.com/C");
            final BigdataURI X = vf.createURI("http://www.bigdata.com/X");
            final BigdataURI AGE = vf.createURI("http://www.bigdata.com/AGE");
            final BigdataLiteral _25 = vf.createLiteral((double) 25);
            final BigdataLiteral _35 = vf.createLiteral((long) 35);
            final BigdataLiteral _45 = vf.createLiteral((long) 45);
            
            db.addTerms( new BigdataValue[] { A, B, C, X, AGE, _25, _35, _45 } );
            
            {
                StatementBuffer buffer = new StatementBuffer
                    ( db, 100/* capacity */
                      );

                buffer.add(A, RDF.TYPE, X);
                buffer.add(A, AGE, _25);
                buffer.add(B, RDF.TYPE, X);
                buffer.add(B, AGE, _45);
                buffer.add(C, RDF.TYPE, X);
                buffer.add(C, AGE, _35);
                
                // write statements on the database.
                buffer.flush();
                
            }
            
            if (log.isInfoEnabled())
                log.info("\n" +db.dumpStore(true, true, false));
  
            { // works great
                
                final String SPO = db.getSPORelation().getNamespace();
                final IVariable<IV> s = Var.var("s");
                final IConstant<IV> type = new Constant<IV>(db.getIV(RDF.TYPE));
                final IConstant<IV> x = new Constant<IV>(X.getIV());
                final IConstant<IV> age = new Constant<IV>(AGE.getIV());
                final IVariable<IV> a = Var.var("a");
                
                final IRule rule =
                        new Rule("test_greater_than", null, // head
                                new IPredicate[] {
                                    toPredicate(db, s, type, x),
                                    toPredicate(db, s, age, a) 
                                },
                                // constraints on the rule.
                                new IConstraint[] {
                        			ValueExpressionConstraint.wrap(new CompareBOp(a, new Constant<IV>(_35.getIV()), CompareOp.GT))
                                }
                        );
                
                try {
                
                    int numSolutions = 0;
                    
                    final IChunkedOrderedIterator<IBindingSet> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        final IBindingSet bs = solutions.next();
                        
                        if (log.isInfoEnabled())
                        	log.info(bs);
                        
                        assertEquals(bs.get(s).get(), B.getIV());
                        assertEquals(bs.get(a).get(), _45.getIV());
                        
                        numSolutions++;
                        
                    }

                    solutions.close();
                    
                    assertEquals("wrong # of solutions", 1, numSolutions);
                    
                } catch(Exception ex) {
                    
                    ex.printStackTrace();
                    
                }
                
            }

        } finally {
            
            db.__tearDownUnitTest();
            
        }
        
    }

    public void testGE() {
        
        // store with no owl:sameAs closure
        AbstractTripleStore db = getStore();
        
        // do not run if we are not inlining
        if (!db.getLexiconRelation().isInlineLiterals()) {
            return;
        }
        
        try {

            BigdataValueFactory vf = db.getValueFactory();
            
            final BigdataURI A = vf.createURI("http://www.bigdata.com/A");
            final BigdataURI B = vf.createURI("http://www.bigdata.com/B");
            final BigdataURI C = vf.createURI("http://www.bigdata.com/C");
            final BigdataURI X = vf.createURI("http://www.bigdata.com/X");
            final BigdataURI AGE = vf.createURI("http://www.bigdata.com/AGE");
            final BigdataLiteral _25 = vf.createLiteral((double) 25);
            final BigdataLiteral _35 = vf.createLiteral((long) 35);
            final BigdataLiteral _45 = vf.createLiteral((long) 45);
            
            db.addTerms( new BigdataValue[] { A, B, C, X, AGE, _25, _35, _45 } );
            
            {
                StatementBuffer buffer = new StatementBuffer
                    ( db, 100/* capacity */
                      );

                buffer.add(A, RDF.TYPE, X);
                buffer.add(A, AGE, _25);
                buffer.add(B, RDF.TYPE, X);
                buffer.add(B, AGE, _45);
                buffer.add(C, RDF.TYPE, X);
                buffer.add(C, AGE, _35);
                
                // write statements on the database.
                buffer.flush();
                
            }
            
            if (log.isInfoEnabled())
                log.info("\n" +db.dumpStore(true, true, false));
  
            { // works great
                
                final String SPO = db.getSPORelation().getNamespace();
                final IVariable<IV> s = Var.var("s");
                final IConstant<IV> type = new Constant<IV>(db.getIV(RDF.TYPE));
                final IConstant<IV> x = new Constant<IV>(X.getIV());
                final IConstant<IV> age = new Constant<IV>(AGE.getIV());
                final IVariable<IV> a = Var.var("a");
                
                final IRule rule =
                        new Rule("test_greater_than", null, // head
                                new IPredicate[] {
                                    toPredicate(db, s, type, x),
                                    toPredicate(db, s, age, a) 
                                },
                                // constraints on the rule.
                                new IConstraint[] {
                        			ValueExpressionConstraint.wrap(new CompareBOp(a, new Constant<IV>(_35.getIV()), CompareOp.GE))
                                });
                
                try {
                
                    int numSolutions = 0;
                    
                    final IChunkedOrderedIterator<IBindingSet> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        final IBindingSet bs = solutions.next();
                        
                        if (log.isInfoEnabled())
                        	log.info(bs);
                        
                        final IV _s = (IV) bs.get(s).get();
                        final IV _a = (IV) bs.get(a).get();
                        assertTrue(_s.equals(B.getIV()) || _s.equals(C.getIV()));
                        assertTrue(_a.equals(_45.getIV()) || _a.equals(_35.getIV()));
                        
                        numSolutions++;
                        
                    }
                    
                    solutions.close();
                    
                    assertEquals("wrong # of solutions", 2, numSolutions);
                    
                } catch(Exception ex) {
                    
                    ex.printStackTrace();
                    
                }
                
            }

        } finally {
            
            db.__tearDownUnitTest();
            
        }
        
    }

    public void testLT() {
        
        // store with no owl:sameAs closure
        AbstractTripleStore db = getStore();
        
        // do not run if we are not inlining
        if (!db.getLexiconRelation().isInlineLiterals()) {
            return;
        }
        
        try {

            BigdataValueFactory vf = db.getValueFactory();
            
            final BigdataURI A = vf.createURI("http://www.bigdata.com/A");
            final BigdataURI B = vf.createURI("http://www.bigdata.com/B");
            final BigdataURI C = vf.createURI("http://www.bigdata.com/C");
            final BigdataURI X = vf.createURI("http://www.bigdata.com/X");
            final BigdataURI AGE = vf.createURI("http://www.bigdata.com/AGE");
            final BigdataLiteral _25 = vf.createLiteral((short) 25);
            final BigdataLiteral _35 = vf.createLiteral((int)   35);
            final BigdataLiteral _45 = vf.createLiteral((long)  45);
            
            db.addTerms( new BigdataValue[] { A, B, C, X, AGE, _25, _35, _45 } );
            
            {
                StatementBuffer buffer = new StatementBuffer
                    ( db, 100/* capacity */
                      );

                buffer.add(A, RDF.TYPE, X);
                buffer.add(A, AGE, _25);
                buffer.add(B, RDF.TYPE, X);
                buffer.add(B, AGE, _45);
                buffer.add(C, RDF.TYPE, X);
                buffer.add(C, AGE, _35);
                
                // write statements on the database.
                buffer.flush();
                
            }
            
            if (log.isInfoEnabled())
                log.info("\n" +db.dumpStore(true, true, false));
  
            { // works great
                
                final String SPO = db.getSPORelation().getNamespace();
                final IVariable<IV> s = Var.var("s");
                final IConstant<IV> type = new Constant<IV>(db.getIV(RDF.TYPE));
                final IConstant<IV> x = new Constant<IV>(X.getIV());
                final IConstant<IV> age = new Constant<IV>(AGE.getIV());
                final IVariable<IV> a = Var.var("a");
                
                final IRule rule =
                        new Rule("test_less_than", null, // head
                                new IPredicate[] {
                                    toPredicate(db, s, type, x),
                                    toPredicate(db, s, age, a) 
                                },
                                // constraints on the rule.
                                new IConstraint[] {
                        			ValueExpressionConstraint.wrap(new CompareBOp(a, new Constant<IV>(_35.getIV()), CompareOp.LT))
                                });
                
                if (log.isInfoEnabled())
                    log.info("running rule: " + rule);
                
                try {
                
                    int numSolutions = 0;
                    
                    final IChunkedOrderedIterator<IBindingSet> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        final IBindingSet bs = solutions.next();
                        
                        if (log.isInfoEnabled())
                        	log.info(bs);
                        
                        assertEquals(bs.get(s).get(), A.getIV());
                        assertEquals(bs.get(a).get(), _25.getIV());
                        
                        numSolutions++;
                        
                    }
                    
                    solutions.close();
                    
                    assertEquals("wrong # of solutions", 1, numSolutions);
                    
                } catch(Exception ex) {
                    
                    ex.printStackTrace();
                    
                }
                
            }

        } finally {
            
            db.__tearDownUnitTest();
            
        }
        
    }

    public void testLE() {
        
        // store with no owl:sameAs closure
        AbstractTripleStore db = getStore();
        
        // do not run if we are not inlining
        if (!db.getLexiconRelation().isInlineLiterals()) {
            return;
        }
        
        try {

            BigdataValueFactory vf = db.getValueFactory();
            
            final BigdataURI A = vf.createURI("http://www.bigdata.com/A");
            final BigdataURI B = vf.createURI("http://www.bigdata.com/B");
            final BigdataURI C = vf.createURI("http://www.bigdata.com/C");
            final BigdataURI X = vf.createURI("http://www.bigdata.com/X");
            final BigdataURI AGE = vf.createURI("http://www.bigdata.com/AGE");
            final BigdataLiteral _25 = vf.createLiteral((short) 25);
            final BigdataLiteral _35 = vf.createLiteral((int)   35);
            final BigdataLiteral _45 = vf.createLiteral((long)  45);
            
            db.addTerms( new BigdataValue[] { A, B, C, X, AGE, _25, _35, _45 } );
            
            {
                StatementBuffer buffer = new StatementBuffer
                    ( db, 100/* capacity */
                      );

                buffer.add(A, RDF.TYPE, X);
                buffer.add(A, AGE, _25);
                buffer.add(B, RDF.TYPE, X);
                buffer.add(B, AGE, _45);
                buffer.add(C, RDF.TYPE, X);
                buffer.add(C, AGE, _35);
                
                // write statements on the database.
                buffer.flush();
                
            }
            
            if (log.isInfoEnabled())
                log.info("\n" +db.dumpStore(true, true, false));
  
            { // works great
                
                final String SPO = db.getSPORelation().getNamespace();
                final IVariable<IV> s = Var.var("s");
                final IConstant<IV> type = new Constant<IV>(db.getIV(RDF.TYPE));
                final IConstant<IV> x = new Constant<IV>(X.getIV());
                final IConstant<IV> age = new Constant<IV>(AGE.getIV());
                final IVariable<IV> a = Var.var("a");
                
                final IRule rule =
                        new Rule("test_less_than", null, // head
                                new IPredicate[] {
                                    toPredicate(db, s, type, x),
                                    toPredicate(db, s, age, a) 
                                },
                                // constraints on the rule.
                                new IConstraint[] {
                        			ValueExpressionConstraint.wrap(new CompareBOp(a, new Constant<IV>(_35.getIV()), CompareOp.LE))
                                });
                
                if (log.isInfoEnabled())
                    log.info("running rule: " + rule);
                
                try {
                
                    int numSolutions = 0;
                    
                    final IChunkedOrderedIterator<IBindingSet> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        final IBindingSet bs = solutions.next();
                        
                        if (log.isInfoEnabled())
                        	log.info(bs);
                        
                        final IV _s = (IV) bs.get(s).get();
                        final IV _a = (IV) bs.get(a).get();
                        assertTrue(_s.equals(A.getIV()) || _s.equals(C.getIV()));
                        assertTrue(_a.equals(_25.getIV()) || _a.equals(_35.getIV()));
                        
                        numSolutions++;
                        
                    }
                    
                    solutions.close();
                    
                    assertEquals("wrong # of solutions", 2, numSolutions);
                    
                } catch(Exception ex) {
                    
                    ex.printStackTrace();
                    
                }
                
            }

        } finally {
            
            db.__tearDownUnitTest();
            
        }
        
    }

    public void testMath() {
        
        // store with no owl:sameAs closure
        AbstractTripleStore db = getStore();

        // do not run if we are not inlining
        if (!db.getLexiconRelation().isInlineLiterals()) {
            return;
        }
        
        try {

            BigdataValueFactory vf = db.getValueFactory();
            
            final BigdataURI A = vf.createURI("http://www.bigdata.com/A");
            final BigdataURI B = vf.createURI("http://www.bigdata.com/B");
            final BigdataURI C = vf.createURI("http://www.bigdata.com/C");
            final BigdataURI D = vf.createURI("http://www.bigdata.com/D");
            final BigdataURI X = vf.createURI("http://www.bigdata.com/X");
            final BigdataURI AGE = vf.createURI("http://www.bigdata.com/AGE");
            final BigdataLiteral _5 = vf.createLiteral((double) 5);
            final BigdataLiteral _30 = vf.createLiteral((double) 30);
            final BigdataLiteral _25 = vf.createLiteral((double) 25);
            final BigdataLiteral _35 = vf.createLiteral((long) 35);
            final BigdataLiteral _45 = vf.createLiteral((long) 45);
            
            db.addTerms( new BigdataValue[] { A, B, C, X, AGE, _25, _35, _45, D, _5, _30 } );
            
            {
                StatementBuffer buffer = new StatementBuffer
                    ( db, 100/* capacity */
                      );

                buffer.add(A, RDF.TYPE, X);
                buffer.add(A, AGE, _25);
                buffer.add(B, RDF.TYPE, X);
                buffer.add(B, AGE, _45);
                buffer.add(C, RDF.TYPE, X);
                buffer.add(C, AGE, _35);
                buffer.add(D, AGE, _30);
                
                // write statements on the database.
                buffer.flush();
                
            }
            
            if (log.isInfoEnabled())
                log.info("\n" +db.dumpStore(true, true, false));
  
            { // works great
                
                final String SPO = db.getSPORelation().getNamespace();
                final IVariable<IV> s = Var.var("s");
                final IConstant<IV> type = new Constant<IV>(db.getIV(RDF.TYPE));
                final IConstant<IV> x = new Constant<IV>(X.getIV());
                final IConstant<IV> age = new Constant<IV>(AGE.getIV());
                final IVariable<IV> a = Var.var("a");
                final IConstant<IV> d = new Constant<IV>(D.getIV());
                final IVariable<IV> dAge = Var.var("dAge");
                
                final IRule rule =
                        new Rule("test_math", null, // head
                                new IPredicate[] {
                        			toPredicate(db, d, age, dAge),
                                    toPredicate(db, s, type, x),
                                    toPredicate(db, s, age, a) 
                                },
                                // constraints on the rule.
                                new IConstraint[] {
                        			ValueExpressionConstraint.wrap(new CompareBOp(a, new MathBOp(dAge, new Constant<IV>(_5.getIV()), MathOp.PLUS), CompareOp.GT))
                                });
                
                try {
                
                    int numSolutions = 0;
                    
                    final IChunkedOrderedIterator<IBindingSet> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        final IBindingSet bs = solutions.next();
                        
                        if (log.isInfoEnabled())
                        	log.info(bs);
                        
                        assertEquals(bs.get(s).get(), B.getIV());
                        assertEquals(bs.get(a).get(), _45.getIV());
                        
                        numSolutions++;
                        
                    }
                    
                    solutions.close();
                    
                    assertEquals("wrong # of solutions", 1, numSolutions);
                    
                } catch(Exception ex) {
                    
                    ex.printStackTrace();
                    
                }
                
            }

        } finally {
            
            db.__tearDownUnitTest();
            
        }
        
    }

    public void testCompareDates() {
        
        // store with no owl:sameAs closure
        AbstractTripleStore db = getStore();

        // do not run if we are not inlining
        if (!db.getLexiconRelation().isInlineLiterals()) {
            return;
        }
        
        try {

            BigdataValueFactory vf = db.getValueFactory();
            
            final BigdataURI A = vf.createURI("http://www.bigdata.com/A");
            final BigdataURI B = vf.createURI("http://www.bigdata.com/B");
            final BigdataURI C = vf.createURI("http://www.bigdata.com/C");
            final BigdataURI X = vf.createURI("http://www.bigdata.com/X");
            final BigdataURI BIRTHDAY = vf.createURI("http://www.bigdata.com/BIRTHDAY");
            
            final XMLGregorianCalendar c1 = new XMLGregorianCalendarImpl(
            		new GregorianCalendar(1976/* year */, 12/* month */, 2/* day */));
            final XMLGregorianCalendar c2 = new XMLGregorianCalendarImpl(
            		new GregorianCalendar(1986/* year */, 10/* month */, 15/* day */));
            final XMLGregorianCalendar c3 = new XMLGregorianCalendarImpl(
            		new GregorianCalendar(1996/* year */, 5/* month */, 30/* day */));
            
            final BigdataLiteral l1 = vf.createLiteral(c1);
            final BigdataLiteral l2 = vf.createLiteral(c2);
            final BigdataLiteral l3 = vf.createLiteral(c3);
            
            db.addTerms( new BigdataValue[] { A, B, C, X, BIRTHDAY, l1, l2, l3 } );
            
            {
                StatementBuffer buffer = new StatementBuffer
                    ( db, 100/* capacity */
                      );

                buffer.add(A, RDF.TYPE, X);
                buffer.add(A, BIRTHDAY, l1);
                buffer.add(B, RDF.TYPE, X);
                buffer.add(B, BIRTHDAY, l2);
                buffer.add(C, RDF.TYPE, X);
                buffer.add(C, BIRTHDAY, l3);
                
                // write statements on the database.
                buffer.flush();
                
            }
            
            if (log.isInfoEnabled())
                log.info("\n" +db.dumpStore(true, true, false));
  
            { // works great
                
                final String SPO = db.getSPORelation().getNamespace();
                final IVariable<IV> s = Var.var("s");
                final IConstant<IV> type = new Constant<IV>(db.getIV(RDF.TYPE));
                final IConstant<IV> x = new Constant<IV>(X.getIV());
                final IConstant<IV> birthday = new Constant<IV>(BIRTHDAY.getIV());
                final IVariable<IV> a = Var.var("a");
                
                final IRule rule =
                        new Rule("test_greater_than", null, // head
                                new IPredicate[] {
                                    toPredicate(db, s, type, x),
                                    toPredicate(db, s, birthday, a) 
                                },
                                // constraints on the rule.
                                new IConstraint[] {
                                    ValueExpressionConstraint.wrap(new CompareBOp(a, new Constant<IV>(l2.getIV()), CompareOp.GT))
                                });
                
                try {

                	final IV left = l3.getIV();
                	final IV right = l2.getIV();
                	
                	if (log.isInfoEnabled()) {
                	
                		log.info(left + ": " + left.isInline() + ", " + left.isLiteral() + ", " + left.isNumeric());
	                	log.info(right + ": " + right.isInline() + ", " + right.isLiteral() + ", " + right.isNumeric());
	                	
	                	if (IVUtility.canNumericalCompare(left) &&
	                			IVUtility.canNumericalCompare(right)) {
	                		
	                		final int compare = IVUtility.numericalCompare(left, right);
	                		log.info("can numerical compare: " + compare);
	            	        
	                	} else {
	                		
	                		final int compare = (left.compareTo(right));
	                		log.info("cannot numerical compare: " + compare);
	            	        
	                	}
	                	
                	}

                    int numSolutions = 0;
                    
                    final IChunkedOrderedIterator<IBindingSet> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        final IBindingSet bs = solutions.next();
                        
                        if (log.isInfoEnabled())
                        	log.info(bs);
                        
                        assertEquals(bs.get(s).get(), C.getIV());
                        assertEquals(bs.get(a).get(), l3.getIV());
                        
                        numSolutions++;
                        
                    }
                    
                    solutions.close();
                    
                    assertEquals("wrong # of solutions", 1, numSolutions);
                    
                } catch(Exception ex) {
                    
                    ex.printStackTrace();
                    
                }
                
            }

        } finally {
            
            db.__tearDownUnitTest();
            
        }
        
    }

//    private IChunkedOrderedIterator<ISolution> runQuery(AbstractTripleStore db, IRule rule)
//        throws Exception {
//        // run the query as a native rule.
//        final IEvaluationPlanFactory planFactory =
//                DefaultEvaluationPlanFactory2.INSTANCE;
//        final IJoinNexusFactory joinNexusFactory =
//                db.newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
//                        ActionEnum.Query, IJoinNexus.BINDINGS, null, // filter
//                        false, // justify 
//                        false, // backchain
//                        planFactory);
//        final IJoinNexus joinNexus =
//                joinNexusFactory.newInstance(db.getIndexManager());
//        final IEvaluationPlan plan = planFactory.newPlan(joinNexus, rule);
//        StringBuilder sb = new StringBuilder();
//        int order[] = plan.getOrder();
//        for (int i = 0; i < order.length; i++) {
//            sb.append(order[i]);
//            if (i < order.length-1) {
//                sb.append(",");
//            }
//        }
//        if(log.isInfoEnabled())log.info("order: [" + sb.toString() + "]");
//        IChunkedOrderedIterator<ISolution> solutions = joinNexus.runQuery(rule);
//        return solutions;
//    }
    
    private IPredicate toPredicate(final AbstractTripleStore database,
    		final IVariableOrConstant<IV> s,
    		final IVariableOrConstant<IV> p,
    		final IVariableOrConstant<IV> o) {
    	
        // The annotations for the predicate.
        final List<NV> anns = new LinkedList<NV>();

        // Decide on the correct arity for the predicate.
        final BOp[] vars = new BOp[] { s, p, o };

        anns.add(new NV(IPredicate.Annotations.RELATION_NAME,
                new String[] { database.getSPORelation().getNamespace() }));//
        

        // timestamp
        anns.add(new NV(Annotations.TIMESTAMP, database
                .getSPORelation().getTimestamp()));

        /*
         * Explicitly set the access path / iterator flags.
         * 
         * Note: High level query generally permits iterator level parallelism.
         * We set the PARALLEL flag here so it can be used if a global index
         * view is chosen for the access path.
         * 
         * Note: High level query for SPARQL always uses read-only access paths.
         * If you are working with a SPARQL extension with UPDATE or INSERT INTO
         * semantics then you will need to remote the READONLY flag for the
         * mutable access paths.
         */
        anns.add(new NV(IPredicate.Annotations.FLAGS, IRangeQuery.DEFAULT
                | IRangeQuery.PARALLEL | IRangeQuery.READONLY));
        
        return new SPOPredicate(vars, anns.toArray(new NV[anns.size()]));
    	
    }
    		
    		
    
    private IChunkedOrderedIterator<IBindingSet> runQuery(
    		final AbstractTripleStore db, final IRule rule)
    		throws Exception {
    
	    final QueryEngine queryEngine = 
	    	QueryEngineFactory.getQueryController(db.getIndexManager());
	
		final PipelineOp query;
		{
			/*
			 * Note: The ids are assigned using incrementAndGet() so ONE (1) is
			 * the first id that will be assigned when we pass in ZERO (0) as
			 * the initial state of the AtomicInteger.
			 */
			final AtomicInteger idFactory = new AtomicInteger(0);
	
			// Convert the step to a bigdata operator tree.
			query = Rule2BOpUtility.convert(rule, idFactory, db,
					queryEngine, new Properties());
	
			if (log.isInfoEnabled())
				log.info("\n"+BOpUtility.toString2(query));
	
		}
		
	    IRunningQuery runningQuery = null;
    	try {
    		
    		// Submit query for evaluation.
    		runningQuery = queryEngine.eval(query);

    		// The iterator draining the query solutions.
    		final IAsynchronousIterator<IBindingSet[]> it1 = runningQuery
    				.iterator();

    	    // De-chunk the IBindingSet[] visited by that iterator.
    	    final IChunkedOrderedIterator<IBindingSet> it2 = 
    	    	new ChunkedWrappedIterator<IBindingSet>(
    	            new Dechunkerator<IBindingSet>(it1));
    	    
    	    return it2;

		} catch (Throwable t) {
			if (runningQuery != null) {
				// ensure query is halted.
				runningQuery.cancel(true/* mayInterruptIfRunning */);
			}
//			log.error("Remove log stmt"+t,t);// FIXME remove this - I am just looking for the root cause of something in the SAIL.
			throw new QueryEvaluationException(t);
		}
		
    }

}
