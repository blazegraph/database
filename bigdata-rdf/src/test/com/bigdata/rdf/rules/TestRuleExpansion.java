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

package com.bigdata.rdf.rules;

import java.util.HashMap;
import java.util.Map;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.OWL;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.bop.constraint.NEConstant;
import com.bigdata.bop.joinGraph.IEvaluationPlan;
import com.bigdata.bop.joinGraph.IEvaluationPlanFactory;
import com.bigdata.bop.joinGraph.fast.DefaultEvaluationPlanFactory2;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IAccessPathExpander;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestRuleExpansion extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestRuleExpansion() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleExpansion(String name) {
        super(name);
    }

    /**
     * Test the various access paths for backchaining the property collection
     * normally done through owl:sameAs {2,3}.
     */
    public void test_optionals() 
    {
     
        // store with no owl:sameAs closure
        final AbstractTripleStore db = getStore();
        
        try {

            final Map<Value, IV> termIds = new HashMap<Value, IV>();
            
            final URI A = new URIImpl("http://www.bigdata.com/A");
            final URI B = new URIImpl("http://www.bigdata.com/B");
//            final URI C = new URIImpl("http://www.bigdata.com/C");
//            final URI D = new URIImpl("http://www.bigdata.com/D");
//            final URI E = new URIImpl("http://www.bigdata.com/E");

//            final URI V = new URIImpl("http://www.bigdata.com/V");
            final URI W = new URIImpl("http://www.bigdata.com/W");
            final URI X = new URIImpl("http://www.bigdata.com/X");
            final URI Y = new URIImpl("http://www.bigdata.com/Y");
            final URI Z = new URIImpl("http://www.bigdata.com/Z");

            {
//                TMStatementBuffer buffer = new TMStatementBuffer
//                ( inf, 100/* capacity */, BufferEnum.AssertionBuffer
//                  );
                StatementBuffer buffer = new StatementBuffer
                    ( db, 100/* capacity */
                      );

                buffer.add(X, A, Z);
                buffer.add(Y, B, W);
                buffer.add(X, OWL.SAMEAS, Y);
                buffer.add(Z, OWL.SAMEAS, W);
                
                // write statements on the database.
                buffer.flush();
                
                // database at once closure.
                db.getInferenceEngine().computeClosure(null/*focusStore*/);

                // write on the store.
//                buffer.flush();
            }
            
            termIds.put(A, db.getIV(A));
            termIds.put(B, db.getIV(B));
            termIds.put(W, db.getIV(W));
            termIds.put(X, db.getIV(X));
            termIds.put(Y, db.getIV(Y));
            termIds.put(Z, db.getIV(Z));
            termIds.put(OWL.SAMEAS, db.getIV(OWL.SAMEAS));
            
            if (log.isInfoEnabled())
                log.info("\n" +db.dumpStore(true, true, false));
  
            for (Map.Entry<Value, IV> e : termIds.entrySet()) {
                System.err.println(e.getKey() + " = " + e.getValue());
            }
/*            
            SPO[] dbGroundTruth = new SPO[]{
                new SPO(x,a,z,
                        StatementEnum.Explicit),
                new SPO(y,b,w,
                        StatementEnum.Explicit),
                new SPO(x,sameAs,y,
                        StatementEnum.Explicit),
                new SPO(z,sameAs,w,
                        StatementEnum.Explicit),
                // forward closure
                new SPO(a,type,property,
                        StatementEnum.Inferred),
                new SPO(a,subpropof,a,
                        StatementEnum.Inferred),
                new SPO(b,type,property,
                        StatementEnum.Inferred),
                new SPO(b,subpropof,b,
                        StatementEnum.Inferred),
                new SPO(w,sameAs,z,
                        StatementEnum.Inferred),
                new SPO(y,sameAs,x,
                        StatementEnum.Inferred),
                // backward chaining        
                new SPO(x,a,w,
                        StatementEnum.Inferred),
                new SPO(x,b,z,
                        StatementEnum.Inferred),
                new SPO(x,b,w,
                        StatementEnum.Inferred),
                new SPO(y,a,z,
                        StatementEnum.Inferred),
                new SPO(y,a,w,
                        StatementEnum.Inferred),
                new SPO(y,b,z,
                        StatementEnum.Inferred),
            };
*/
            
            {
                
                IAccessPathExpander<ISPO> expander = new IAccessPathExpander<ISPO>() {
                    public boolean backchain() {
                        return false;
                    }
                    public boolean runFirst() {
                        return false;
                    }
                    public IAccessPath<ISPO> getAccessPath(final IAccessPath<ISPO> accessPath) {
                        final IVariableOrConstant<IV> s = accessPath.getPredicate().get(0);
                        final IVariableOrConstant<IV> p = accessPath.getPredicate().get(1);
                        final IVariableOrConstant<IV> o = accessPath.getPredicate().get(2);
                        boolean isValid = true;
                        if (!p.isConstant() || !IVUtility.equals(p.get(), termIds.get(OWL.SAMEAS))) {
                            if (log.isInfoEnabled())
                                log.info("p must be owl:sameAs");
                            isValid = false;
                        }
                        if (s.isVar() && o.isVar()) {
                            if (log.isInfoEnabled())
                                log.info("s and o cannot both be variables");
                            isValid = false;
                        }
                        if (s.isConstant() && o.isConstant()) {
                            if (log.isInfoEnabled())
                                log.info("s and o cannot both be constants");
                            isValid = false;
                        }
                        final SPO spo;
                        if (isValid) {
                            final IV constant = s.isConstant() ? s.get() : o.get();
                            spo = s.isConstant() ?
                                    new SPO(s.get(), p.get(), constant) :
                                    new SPO(constant, p.get(), o.get());
                            if (log.isInfoEnabled())
                                log.info("appending SPO: " + spo.toString(db));
                        } else {
                            spo = null;
                        }
                        return new IAccessPath<ISPO>() {
                            public IIndex getIndex() {
                                return accessPath.getIndex();
                            }
                            public IKeyOrder<ISPO> getKeyOrder() {
                                return accessPath.getKeyOrder();
                            }
                            public IPredicate<ISPO> getPredicate() {
                                return accessPath.getPredicate();
                            }
                            public boolean isEmpty() {
                                return false;
                            }
                            public IChunkedOrderedIterator<ISPO> iterator() {
                                final IChunkedOrderedIterator<ISPO> delegate = accessPath.iterator();
                                if (spo == null) {
                                    return delegate;
                                }
                                final IChunkedOrderedIterator<ISPO> appender = 
                                    new ChunkedArrayIterator<ISPO>(1, new ISPO[] { spo }, SPOKeyOrder.SPO);
                                return new IChunkedOrderedIterator<ISPO>() {
                                    public ISPO next() {
                                        if (delegate.hasNext()) {
                                            return delegate.next();
                                        } else {
                                            return appender.next();
                                        }
                                    }
                                    public ISPO[] nextChunk() {
                                        if (delegate.hasNext()) {
                                            return delegate.nextChunk();
                                        } else {
                                            return appender.nextChunk();
                                        }
                                    }
                                    public void remove() {
                                        throw new UnsupportedOperationException();
                                    }
                                    public boolean hasNext() {
                                        return delegate.hasNext() || appender.hasNext();
                                    }
                                    public IKeyOrder<ISPO> getKeyOrder() {
                                        return delegate.getKeyOrder();
                                    }
                                    public ISPO[] nextChunk(IKeyOrder<ISPO> keyOrder) {
                                        if (delegate.hasNext()) {
                                            return delegate.nextChunk(keyOrder);
                                        } else {
                                            return appender.nextChunk(keyOrder);
                                        }
                                    }
                                    public void close() {
                                        delegate.close();
                                        appender.close();
                                    }
                                };
                            }
                            public IChunkedOrderedIterator<ISPO> iterator(int limit, int capacity) {
                                throw new UnsupportedOperationException();
                            }
                            public IChunkedOrderedIterator<ISPO> iterator(long offset, long limit, int capacity) {
                                throw new UnsupportedOperationException();
                            }
                            public long rangeCount(boolean exact) {
                                return accessPath.rangeCount(exact)+1;
                            }
                            public ITupleIterator<ISPO> rangeIterator() {
                                throw new UnsupportedOperationException();
                            }
                            public long removeAll() {
                                return accessPath.removeAll();
                            }
                        };
                    }
                };
                
                final String SPO = db.getSPORelation().getNamespace();
                final IConstant<IV> s = new Constant<IV>(termIds.get(X));
                final IVariable<IV> _p = Var.var("p");
                final IVariable<IV> _o = Var.var("o");
                final IVariable<IV> _sameS = Var.var("sameS");
                final IVariable<IV> _sameO = Var.var("sameO");
                final IConstant<IV> sameAs = new Constant<IV>(termIds.get(OWL.SAMEAS));
                final IRule rule =
                        new Rule("sameas", null, /*new SPOPredicate(SPO, s, _p, _sameO), // head*/
                                new IPredicate[] {
                                        new SPOPredicate(SPO, _sameS, sameAs, s, expander),
                                        new SPOPredicate(SPO, _sameS, _p, _o),
                                        new SPOPredicate(SPO, _o, sameAs, _sameO, true /*optional*/, expander),
                                },
                                // true, // distinct
                                new IConstraint[] {
                                        new NEConstant(_p, sameAs)
                                    }
                                );
                
                try {
                    int numSolutions = 0;
                    IChunkedOrderedIterator<ISolution> solutions = runQuery(db, rule);
                    while (solutions.hasNext()) {
                        ISolution solution = solutions.next();
                        IBindingSet bindings = solution.getBindingSet();
                        System.err.println(solution);
                        numSolutions++;
                    }
                    assertTrue("wrong # of solutions", numSolutions == 4);
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
        System.err.println("order: [" + sb.toString() + "]");
        IChunkedOrderedIterator<ISolution> solutions = joinNexus.runQuery(rule);
        return solutions;
    }

}
