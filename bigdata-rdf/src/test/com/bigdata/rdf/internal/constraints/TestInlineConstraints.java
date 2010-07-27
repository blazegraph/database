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

import java.util.Properties;
import org.openrdf.model.vocabulary.RDF;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ProxyTestCase;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IEvaluationPlan;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id: TestOptionals.java 3149 2010-07-07 01:19:11Z mrpersonick $
 */
public class TestInlineConstraints extends ProxyTestCase {

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
                                    new SPOPredicate(SPO, s, type, x),
                                    new SPOPredicate(SPO, s, age, a) 
                                },
                                // constraints on the rule.
                                new IConstraint[] {
                                    new InlineGT(a, _35.getIV())
                                });
                
                try {
                
                    int numSolutions = 0;
                    
                    IChunkedOrderedIterator<ISolution> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        ISolution solution = solutions.next();
                        
                        IBindingSet bs = solution.getBindingSet();
                        
                        assertEquals(bs.get(s).get(), B.getIV());
                        assertEquals(bs.get(a).get(), _45.getIV());
                        
                        numSolutions++;
                        
                    }
                    
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
                                    new SPOPredicate(SPO, s, type, x),
                                    new SPOPredicate(SPO, s, age, a) 
                                },
                                // constraints on the rule.
                                new IConstraint[] {
                                    new InlineGE(a, _35.getIV())
                                });
                
                try {
                
                    int numSolutions = 0;
                    
                    IChunkedOrderedIterator<ISolution> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        ISolution solution = solutions.next();
                        
                        IBindingSet bs = solution.getBindingSet();
                        
                        final IV _s = (IV) bs.get(s).get();
                        final IV _a = (IV) bs.get(a).get();
                        assertTrue(_s.equals(B.getIV()) || _s.equals(C.getIV()));
                        assertTrue(_a.equals(_45.getIV()) || _a.equals(_35.getIV()));
                        
                        numSolutions++;
                        
                    }
                    
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
                                    new SPOPredicate(SPO, s, type, x),
                                    new SPOPredicate(SPO, s, age, a) 
                                },
                                // constraints on the rule.
                                new IConstraint[] {
                                    new InlineLT(a, _35.getIV())
                                });
                
                if (log.isInfoEnabled())
                    log.info("running rule: " + rule);
                
                try {
                
                    int numSolutions = 0;
                    
                    IChunkedOrderedIterator<ISolution> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        ISolution solution = solutions.next();
                        
                        IBindingSet bs = solution.getBindingSet();
                        
                        if (log.isInfoEnabled())
                            log.info("solution: " + bs);
                        
                        assertEquals(bs.get(s).get(), A.getIV());
                        assertEquals(bs.get(a).get(), _25.getIV());
                        
                        numSolutions++;
                        
                    }
                    
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
                                    new SPOPredicate(SPO, s, type, x),
                                    new SPOPredicate(SPO, s, age, a) 
                                },
                                // constraints on the rule.
                                new IConstraint[] {
                                    new InlineLE(a, _35.getIV())
                                });
                
                if (log.isInfoEnabled())
                    log.info("running rule: " + rule);
                
                try {
                
                    int numSolutions = 0;
                    
                    IChunkedOrderedIterator<ISolution> solutions = runQuery(db, rule);
                    
                    while (solutions.hasNext()) {
                        
                        ISolution solution = solutions.next();
                        
                        IBindingSet bs = solution.getBindingSet();
                        
                        if (log.isInfoEnabled())
                            log.info("solution: " + bs);
                        
                        final IV _s = (IV) bs.get(s).get();
                        final IV _a = (IV) bs.get(a).get();
                        assertTrue(_s.equals(A.getIV()) || _s.equals(C.getIV()));
                        assertTrue(_a.equals(_25.getIV()) || _a.equals(_35.getIV()));
                        
                        numSolutions++;
                        
                    }
                    
                    assertEquals("wrong # of solutions", 2, numSolutions);
                    
                } catch(Exception ex) {
                    
                    ex.printStackTrace();
                    
                }
                
            }

        } finally {
            
            db.__tearDownUnitTest();
            
        }
        
    }

    private IChunkedOrderedIterator<ISolution> runQuery(AbstractTripleStore db, IRule rule)
        throws Exception {
        // run the query as a native rule.
        final IEvaluationPlanFactory planFactory =
                DefaultEvaluationPlanFactory2.INSTANCE;
        final IJoinNexusFactory joinNexusFactory =
                db.newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
                        ActionEnum.Query, IJoinNexus.BINDINGS, null, // filter
                        false, // justify 
                        false, // backchain
                        planFactory);
        final IJoinNexus joinNexus =
                joinNexusFactory.newInstance(db.getIndexManager());
        final IEvaluationPlan plan = planFactory.newPlan(joinNexus, rule);
        StringBuilder sb = new StringBuilder();
        int order[] = plan.getOrder();
        for (int i = 0; i < order.length; i++) {
            sb.append(order[i]);
            if (i < order.length-1) {
                sb.append(",");
            }
        }
        if(log.isInfoEnabled())log.info("order: [" + sb.toString() + "]");
        IChunkedOrderedIterator<ISolution> solutions = joinNexus.runQuery(rule);
        return solutions;
    }

}
