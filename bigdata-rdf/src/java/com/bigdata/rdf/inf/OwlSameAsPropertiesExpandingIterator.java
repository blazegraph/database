package com.bigdata.rdf.inf;

import java.util.Arrays;
import org.apache.log4j.Logger;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.QueryOptions;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IEvaluationPlan;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

public class OwlSameAsPropertiesExpandingIterator implements
        IChunkedOrderedIterator<ISPO> {
    protected final static Logger log =
            Logger.getLogger(OwlSameAsPropertiesExpandingIterator.class);

    private final static transient long NULL = IRawTripleStore.NULL;

    private final IChunkedOrderedIterator<ISPO> src;

    private final IKeyOrder<ISPO> keyOrder;

    private final long s, p, o;

    private final AbstractTripleStore db;

    private final long sameAs;

    private IChunkedOrderedIterator<ISolution> solutions;

    private ISolutionExpander<ISPO> sameAsSelfExpander;

    public OwlSameAsPropertiesExpandingIterator(long s, long p, long o,
            AbstractTripleStore db, final long sameAs,
            final IKeyOrder<ISPO> keyOrder) {
        this.db = db;
        this.s = s;
        this.p = p;
        this.o = o;
        this.sameAs = sameAs;
        this.keyOrder = keyOrder;
        this.sameAsSelfExpander = new SameAsSelfExpander();
        if (p == sameAs) {
            // we don't need to run the expander when the predicate is
            // owl:sameAs, the forward chainer takes care of that case
            this.src = db.getAccessPath(s, p, o).iterator();
        } else {
            this.src = null;
            try {
                if (s != NULL && o != NULL) {
                    accessSPO();
                } else if (s != NULL && o == NULL) {
                    accessSP();
                } else if (s == NULL && o != NULL) {
                    accessPO();
                } else if (s == NULL && o == NULL) {
                    accessP();
                } else
                    throw new AssertionError();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void accessSPO() throws Exception {
        /*
         construct S ?p O
         where {
         ?sameS sameAs S .
         ?sameO sameAs O .
         ?sameS ?p ?sameO
         }
         */
        final String SPO = db.getSPORelation().getNamespace();
        final IVariable<Long> _sameS = Var.var("sameS");
        final IVariable<Long> _sameO = Var.var("sameO");
        final IConstant<Long> sameAs = new Constant<Long>(this.sameAs);
        final IConstant<Long> s = new Constant<Long>(this.s);
        final IConstant<Long> o = new Constant<Long>(this.o);
        final IVariableOrConstant<Long> _p =
                this.p != NULL ? new Constant<Long>(this.p) : Var.var("p");
        final SPOPredicate head = new SPOPredicate(SPO, s, _p, o);
        final IRule rule =
                new Rule("sameAsSPO", head, new IPredicate[] {
                        new SPOPredicate(SPO, _sameS, sameAs, s,
                                sameAsSelfExpander),
                        new SPOPredicate(SPO, _sameO, sameAs, o,
                                sameAsSelfExpander),
                        new SPOPredicate(SPO, _sameS, _p, _sameO) },
                        QueryOptions.DISTINCT, // distinct
                        // constraints on the rule.
                        new IConstraint[] { new RejectSameAsSelf(head.s(), head
                                .p(), head.o()) });
        runQuery(rule);
    }

    private void accessSP() throws Exception {
        /*
         construct S ?p ?sameO
         where {
         ?sameS sameAs S .
         ?sameS ?p ?o .
         ?o sameAs ?sameO
         }
         */
        final String SPO = db.getSPORelation().getNamespace();
        final IVariable<Long> _sameS = Var.var("sameS");
        final IVariable<Long> _sameO = Var.var("sameO");
        final IConstant<Long> sameAs = new Constant<Long>(this.sameAs);
        final IConstant<Long> s = new Constant<Long>(this.s);
        final IVariable<Long> _o = Var.var("o");
        final IVariableOrConstant<Long> _p =
                this.p != NULL ? new Constant<Long>(this.p) : Var.var("p");
        final SPOPredicate head = new SPOPredicate(SPO, s, _p, _sameO);
        final IRule rule =
                new Rule("sameAsSP", head, new IPredicate[] {
                        new SPOPredicate(SPO, _sameS, sameAs, s,
                                sameAsSelfExpander), // ?s sameAs y
                        new SPOPredicate(SPO, _sameS, _p, _o), // ?s b ?o  -> y b w
                        new SPOPredicate(SPO, _o, sameAs, _sameO, true,
                                sameAsSelfExpander) },
                        QueryOptions.DISTINCT, // distinct
                        // constraints on the rule.
                        new IConstraint[] { new RejectSameAsSelf(head.s(), head
                                .p(), head.o()) });
        runQuery(rule);
    }

    private void accessPO() throws Exception {
        /*
         construct ?sameS ?p O
         where {
         ?sameO sameAs O .
         ?s ?p ?sameO .
         ?s sameAs ?sameS
         }
         */
        final String SPO = db.getSPORelation().getNamespace();
        final IVariable<Long> _sameS = Var.var("sameS");
        final IVariable<Long> _sameO = Var.var("sameO");
        final IConstant<Long> sameAs = new Constant<Long>(this.sameAs);
        final IVariable<Long> _s = Var.var("s");
        final IConstant<Long> o = new Constant<Long>(this.o);
        final IVariableOrConstant<Long> _p =
                this.p != NULL ? new Constant<Long>(this.p) : Var.var("p");
        final SPOPredicate head = new SPOPredicate(SPO, _sameS, _p, o);
        final IRule rule =
                new Rule("sameAsPO", head, new IPredicate[] {
                        new SPOPredicate(SPO, _sameO, sameAs, o,
                                sameAsSelfExpander),
                        new SPOPredicate(SPO, _s, _p, _sameO),
                        new SPOPredicate(SPO, _s, sameAs, _sameS, true,
                                sameAsSelfExpander) }, 
                        QueryOptions.DISTINCT, // distinct
                        // constraints on the rule.
                        new IConstraint[] { new RejectSameAsSelf(head.s(), head
                                .p(), head.o()) });
        runQuery(rule);
    }

    private void accessP() throws Exception {
        /*
         construct ?sameS ?p ?sameO
         where {
         ?s sameAs ?sameS .
         ?o sameAs ?sameO .
         ?s ?p ?o
         }
         */
        final String SPO = db.getSPORelation().getNamespace();
        final IVariable<Long> _sameS = Var.var("sameS");
        final IVariable<Long> _sameO = Var.var("sameO");
        final IConstant<Long> sameAs = new Constant<Long>(this.sameAs);
        final IVariable<Long> _s = Var.var("s");
        final IVariable<Long> _o = Var.var("o");
        final IVariableOrConstant<Long> _p =
                this.p != NULL ? new Constant<Long>(this.p) : Var.var("p");
        final SPOPredicate head = new SPOPredicate(SPO, _sameS, _p, _sameO);
        final IRule rule =
                new Rule("sameAsP", head, new IPredicate[] {
                        new SPOPredicate(SPO, _sameS, sameAs, _s, true, 
                                sameAsSelfExpander),
                        new SPOPredicate(SPO, _sameO, sameAs, _o, true,
                                sameAsSelfExpander),
                        new SPOPredicate(SPO, _s, _p, _o) }, 
                        QueryOptions.DISTINCT, // distinct
                        // constraints on the rule.
                        new IConstraint[] { new RejectSameAsSelf(head.s(), head
                                .p(), head.o()) });
        runQuery(rule);
    }

    private void runQuery(IRule rule)
            throws Exception {
        // run the query as a native rule.
        final IEvaluationPlanFactory planFactory =
                DefaultEvaluationPlanFactory2.INSTANCE;
        final IJoinNexusFactory joinNexusFactory =
                db.newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
                        ActionEnum.Query, IJoinNexus.ELEMENT, null, // filter
                        false, // justify 
                        false, // backchain
                        planFactory);
        final IJoinNexus joinNexus =
                joinNexusFactory.newInstance(db.getIndexManager());
        if (log.isInfoEnabled()) {
            final IEvaluationPlan plan = planFactory.newPlan(joinNexus, rule);
            StringBuilder sb = new StringBuilder();
            int order[] = plan.getOrder();
            for (int i = 0; i < order.length; i++) {
                sb.append(order[i]);
                if (i < order.length - 1) {
                    sb.append(",");
                }
            }
            log.info("order: [" + sb.toString() + "]");
        }
        this.solutions = joinNexus.runQuery(rule);
        // this.resolverator = resolverator;
    }

    public IKeyOrder<ISPO> getKeyOrder() {
        if (src != null) {
            return src.getKeyOrder();
        }
        return keyOrder;
    }

    private ISPO[] chunk;

    private int i = 0;

    public ISPO[] nextChunk() {
        if (src != null) {
            return src.nextChunk();
        }
        final int chunkSize = 10000;
        ISPO[] s = new ISPO[chunkSize];
        int n = 0;
        while (hasNext() && n < chunkSize) {
            ISolution<ISPO> solution = solutions.next();
            // s[n++] = resolverator.resolve(solution);
            ISPO spo = solution.get();
            spo = new SPO(spo.s(), spo.p(), spo.o());
            s[n++] = spo;
        }
        
        // copy so that stmts[] is dense.
        ISPO[] stmts = new ISPO[n];
        System.arraycopy(s, 0, stmts, 0, n);
        
        // fill in the explicit/inferred information, sort by SPO key order
        // since we will use the SPO index to do the value completion
        stmts = db.bulkCompleteStatements(stmts);
        
        // resort into desired order
        Arrays.sort(stmts, 0, n, this.keyOrder.getComparator());
        
        return stmts;
    }

    public ISPO[] nextChunk(IKeyOrder<ISPO> keyOrder) {
        if (src != null) {
            return src.nextChunk(keyOrder);
        }
        if (keyOrder == null)
            throw new IllegalArgumentException();
        ISPO[] stmts = nextChunk();
        if (this.keyOrder != keyOrder) {
            // sort into the required order.
            Arrays.sort(stmts, 0, stmts.length, keyOrder.getComparator());
        }
        return stmts;
    }

    public ISPO next() {
        if (src != null) {
            return src.next();
        }
        if (chunk == null || i == chunk.length) {
            chunk = nextChunk();
            i = 0;
            if (log.isInfoEnabled()) 
                log.info("got a chunk, length = " + chunk.length);
        }
        return chunk[i++];
    }

    public void remove() {
        if (src != null) {
            src.remove();
            return;
        }
        throw new UnsupportedOperationException();
    }

    public void close() {
        if (src != null) {
            src.close();
            return;
        }
        if (solutions != null) {
            solutions.close();
        }
    }

    public boolean hasNext() {
        if (src != null) {
            return src.hasNext();
        }
        if (chunk != null) {
            return i < chunk.length;
        }
        if (solutions != null) {
            return solutions.hasNext();
        }
        return false;
    }

    private class SameAsSelfExpander implements ISolutionExpander<ISPO> {
        public boolean backchain() {
            return false;
        }
        public boolean runFirst() {
            return false;
        }
        public IAccessPath<ISPO> getAccessPath(
                final IAccessPath<ISPO> accessPath) {
            return new SameAsSelfAccessPath(accessPath);
        }
    };

    private class SameAsSelfAccessPath implements IAccessPath<ISPO> {
        private IAccessPath<ISPO> accessPath;

        private SPO spo;

        public SameAsSelfAccessPath(IAccessPath<ISPO> accessPath) {
            this.accessPath = accessPath;
            final IVariableOrConstant<Long> p =
                    accessPath.getPredicate().get(1);
            if (!p.isConstant() || p.get() != sameAs) {
                throw new UnsupportedOperationException("p must be owl:sameAs");
            }
        }

        private IChunkedOrderedIterator<ISPO> getAppender() {
            final IVariableOrConstant<Long> s =
                    accessPath.getPredicate().get(0);
            final IVariableOrConstant<Long> o =
                    accessPath.getPredicate().get(2);
            if (s.isVar() && o.isVar()) {
                throw new UnsupportedOperationException(
                        "s and o cannot both be variables");
            }
            if (s.isConstant() && o.isConstant()
                    && s.get().equals(o.get()) == false) {
                /*
                 * if s and o are both constants, then there is nothing for this
                 * appender to append, unless they are equal to each other, in
                 * which case we can append that solution
                 */
                this.spo = null;
            } else {
                final long constant = s.isConstant() ? s.get() : o.get();
                this.spo =
                        s.isConstant() ? new SPO(s.get(), sameAs, constant)
                                : new SPO(constant, sameAs, o.get());
                if (log.isInfoEnabled()) 
                    log.info("appending SPO: " + spo.toString(db));
            }
            if (spo != null) {
                return new ChunkedArrayIterator<ISPO>(1, new SPO[] { spo },
                        SPOKeyOrder.SPO);
            } else {
                return null;
            }
        }

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
            final IChunkedOrderedIterator<ISPO> appender = getAppender();
            final IChunkedOrderedIterator<ISPO> delegate =
                    accessPath.iterator();
            if (appender == null) {
                return delegate;
            }
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
            return accessPath.rangeCount(exact) + 1;
        }

        public ITupleIterator<ISPO> rangeIterator() {
            throw new UnsupportedOperationException();
        }

        public long removeAll() {
            throw new UnsupportedOperationException();
        }
    };

    private class RejectSameAsSelf implements IConstraint {
        private IVariableOrConstant<Long> _s, _p, _o;

        public RejectSameAsSelf(IVariableOrConstant<Long> _s,
                IVariableOrConstant<Long> _p, IVariableOrConstant<Long> _o) {
            this._s = _s;
            this._p = _p;
            this._o = _o;
        }

        public boolean accept(IBindingSet bindings) {
            long sVal = getValue(_s, bindings);
            long pVal = getValue(_p, bindings);
            long oVal = getValue(_o, bindings);
            // not fully bound yet, just ignore for now
            if (sVal == NULL || pVal == NULL || oVal == NULL) {
                return true;
            }
            if (pVal == sameAs && sVal == oVal) {
                return false;
            }
            return true;
        }

        public long getValue(IVariableOrConstant<Long> _x, IBindingSet bindings) {
            long val;
            if (_x.isConstant()) {
                val = _x.get();
            } else {
                IConstant<Long> bound = bindings.get((IVariable<Long>) _x);
                val = bound != null ? bound.get() : NULL;
            }
            return val;
        }
    }
}
