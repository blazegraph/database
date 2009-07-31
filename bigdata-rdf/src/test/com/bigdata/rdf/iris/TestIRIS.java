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
 * Created on Apr 28, 2009
 */

package com.bigdata.rdf.iris;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.deri.iris.api.IProgramOptimisation.Result;
import org.deri.iris.api.basics.IAtom;
import org.deri.iris.api.basics.ILiteral;
import org.deri.iris.api.basics.IQuery;
import org.deri.iris.api.basics.IRule;
import org.deri.iris.api.basics.ITuple;
import org.deri.iris.api.factory.IBasicFactory;
import org.deri.iris.api.factory.IBuiltinsFactory;
import org.deri.iris.api.factory.ITermFactory;
import org.deri.iris.api.terms.ITerm;
import org.deri.iris.basics.BasicFactory;
import org.deri.iris.builtins.BuiltinsFactory;
import org.deri.iris.optimisations.magicsets.MagicSets;
import org.deri.iris.terms.TermFactory;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rules.AbstractInferenceEngineTestCase;
import com.bigdata.rdf.rules.BaseClosure;
import com.bigdata.rdf.rules.DoNotAddFilter;
import com.bigdata.rdf.rules.MappedProgram;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.rules.RuleRdfs11;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.EQ;
import com.bigdata.relation.rule.EQConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IN;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.relation.rule.NE;
import com.bigdata.relation.rule.NEConstant;
import com.bigdata.relation.rule.OR;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Test suite for IRIS-based truth maintenance on delete.
 * <p>
 * I would use a one up counter to assign the variable names during the
 * conversion and append it to the variable names in our rules.
 * 
 * The constraints are really just additional conditions on the rule. E.g.,
 * {@link RuleRdfs11} looks like this in prolog:
 * 
 * <pre>
 * triple(U,uri(rdfs:subClassOf),X) :-
 *  triple(U,uri(rdfs:subClassOf),V),
 *  triple(V,uri(rdfs:subClassOf),X),
 *  U != V,
 *  V != X.
 * </pre>
 * 
 * The RDF values will be expressed a atoms with the follow arity: uri/1,
 * literal/3, and bnode/1.
 * 
 * <pre>
 * uri(stringValue)
 * literal(stringValue,languageCode,datatTypeUri)
 * bnode(id)
 * </pre>
 * 
 * All for the values for those atoms will be string values.
 * 
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestIRIS extends AbstractInferenceEngineTestCase {

    final IBasicFactory BASIC = BasicFactory.getInstance();
    
    final ITermFactory TERM = TermFactory.getInstance();
    
    final IBuiltinsFactory BUILTINS = BuiltinsFactory.getInstance();
    
    final org.deri.iris.api.basics.IPredicate EQUAL = BASIC.createPredicate( "EQUAL", 2 );
    
    final org.deri.iris.api.basics.IPredicate NOT_EQUAL = BASIC.createPredicate( "NOT_EQUAL", 2 );
    
    final org.deri.iris.api.basics.IPredicate TRIPLE = BASIC.createPredicate("triple", 3);
    

    
    /**
     * 
     */
    public TestIRIS() {
        super();
    }

    /**
     * @param name
     */
    public TestIRIS(String name) {
        super(name);
    }

    /**
     * We are trying to eliminate the use of justification chains inside the
     * database.  These justification chains are used to determine whether or
     * not a statement is grounded by other facts in the database.  To determine 
     * this without justifications, we could use a magic sets optimization
     * against the normal inference rules using the statement to test as the
     * query.
     * 
     * @fixme variables for bigdata rules are assigned locally to the rule. will
     * this create problems for the iris program, where all the rules are mushed
     * together?
     * 
     * @fixme what do we do about the IConstraints on bigdata rules?  do they
     * get promoted to full-fledged IRIS rules in the IRIS program?
     */
    public void testRetractionWithIRIS() {
        
        
        Properties properties = getProperties();
        
        // override the default axiom model.
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        // override the default closure.
        properties.setProperty(Options.CLOSURE_CLASS, SimpleClosure.class.getName());
        
        AbstractTripleStore store = getStore(properties);
        
        try {

            final BigdataValueFactory f = store.getValueFactory();
            final BigdataURI U = f.createURI("http://www.bigdata.com/U");
            final BigdataURI V = f.createURI("http://www.bigdata.com/V");
            final BigdataURI W = f.createURI("http://www.bigdata.com/W");
            final BigdataURI X = f.createURI("http://www.bigdata.com/X");
            final BigdataURI Y = f.createURI("http://www.bigdata.com/Y");
            final BigdataURI Z = f.createURI("http://www.bigdata.com/Z");
            final BigdataURI sco = f.asValue(RDFS.SUBCLASSOF);
            
            // set the stage
            // U sco V and V sco X implies U sco X
            // let's pretend all three statements were originally in the
            // database, and { U sco X } is retracted, leaving only the other
            // two.
            // we should be able to create an adorned program based on our
            // normal inference rules using the "retracted" statement as the
            // query.
            
            store.addStatement(U, sco, V);
            
            store.addStatement(V, sco, W);
            
            store.addStatement(X, sco, Y);
            
            store.addStatement(Y, sco, Z);
            
            if (log.isInfoEnabled())
                log.info("\n\nstore:\n"
                        + store.dumpStore(store,
                                true, true, true, true));
            
            // now get the program from the inference engine
            
            BaseClosure closure = store.getClosureInstance();
            
            MappedProgram program = closure.getProgram(
                    store.getSPORelation().getNamespace(),
                    null
                    );
            
            log.info("bigdata program before magic sets:");
            
            Iterator<IStep> steps = program.steps();
            
            while (steps.hasNext()) {
                
                com.bigdata.relation.rule.IRule rule = 
                    (com.bigdata.relation.rule.IRule) steps.next();
                
                log.info(rule);
                
            }

            // now we convert the bigdata program into an IRIS program

            Collection<IRule> rules = new LinkedList<IRule>();
            
            convertToIRISProgram(program, rules);
            
            if (log.isInfoEnabled()) {
                
                log.info("prolog program before magic sets:");
            
                for (IRule rule : rules) {
                    
                    log.info(rule);
                    
                }
                
            }
            
            // then we create a query for the fact we are looking for
            
            IAtom atom = BASIC.createAtom(
                TRIPLE,
                BASIC.createTuple(
                    TERM.createString(String.valueOf(U.getTermId())), 
                    TERM.createString(String.valueOf(sco.getTermId())), 
                    TERM.createString(String.valueOf(W.getTermId()))
                    )
                );
            
            IQuery query = BASIC.createQuery(
                BASIC.createLiteral(
                    true, // positive 
                    atom
                    )
                );
            
            if (log.isInfoEnabled()) {
                
                log.info("query: " + query);
                
            }
            
            // create the magic sets optimizer
            
            MagicSets magicSets = new MagicSets();
            
            Result result = magicSets.optimise(rules, query);
            
            if (log.isInfoEnabled()) {
                
                log.info("prolog program after magic sets:");
            
                for (IRule rule : result.rules) {
                    
                    log.info(rule);
                    
                }
                
            }
            
            final Properties tmp = store.getProperties();
            tmp.setProperty(AbstractTripleStore.Options.LEXICON, "false");
            // tmp.setProperty(AbstractTripleStore.Options.ONE_ACCESS_PATH, "true");

            final TempMagicStore tempStore = new TempMagicStore(
                    store.getIndexManager().getTempStore(), tmp, store);
            
            // now we take the optimized set of rules and convert it back to a
            // bigdata program
            
            Program magicProgram = 
                convertToBigdataProgram(store, tempStore, result.rules);
                // convertToBigdataProgram(store, rules);
            
            log.info("bigdata program after magic sets:");
            
            // log.info("bigdata program converted back from prolog program:");
            
            steps = magicProgram.steps();
            
            while (steps.hasNext()) {
                
                com.bigdata.relation.rule.IRule rule = 
                    (com.bigdata.relation.rule.IRule) steps.next();
                
                log.info(rule);
                
            }

            // then we somehow run the magic program and see if the fact in
            // question exists in the resulting closure, if it does, then the
            // statement is supported by other facts in the database
            
            computeClosure(store, tempStore, magicProgram);
            
            log.info("\n"+store.dumpStore(store, true, true, true).toString());
            log.info("\n"+tempStore.dumpStore(store, true, true, true).toString());
/*            
            // run the query as a native rule.
            final IEvaluationPlanFactory planFactory =
                    DefaultEvaluationPlanFactory2.INSTANCE;
            
            final IJoinNexusFactory joinNexusFactory =
                    store.newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
                            ActionEnum.Query, IJoinNexus.ELEMENT, null, // filter
                            false, // justify 
                            false, // backchain
                            planFactory);
            
            final IJoinNexus joinNexus =
                    joinNexusFactory.newInstance(store.getIndexManager());
            
            IChunkedOrderedIterator<ISolution> solutions = joinNexus.runQuery(magicProgram);

            while (solutions.hasNext()) {

                ISolution<ISPO> solution = solutions.next();

                System.err.println(solution.get().toString(store));

            }
*/            
        } catch( Exception ex ) {
            
            throw new RuntimeException(ex);
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }

    public synchronized ClosureStats computeClosure(
            AbstractTripleStore database, AbstractTripleStore focusStore, 
            Program program) {

        final boolean justify = false;
        
        final DoNotAddFilter doNotAddFilter = new DoNotAddFilter(
                database.getVocabulary(), database.getAxioms(), 
                true /*forwardChainRdfTypeRdfsResource*/);
        
        try {

            final long begin = System.currentTimeMillis();

            /*
             * FIXME remove IJoinNexus.RULE once we we can generate the
             * justifications from just the bindings and no longer need the rule
             * to generate the justifications (esp. for scale-out).
             */
            final int solutionFlags = IJoinNexus.ELEMENT//
                    | (justify ? IJoinNexus.RULE | IJoinNexus.BINDINGS : 0)//
//                  | IJoinNexus.RULE  // iff debugging.
                  ;
          
            final RuleContextEnum ruleContext = focusStore == null
                ? RuleContextEnum.DatabaseAtOnceClosure
                : RuleContextEnum.TruthMaintenance
                ;
            
            final IJoinNexusFactory joinNexusFactory = database
                    .newJoinNexusFactory(ruleContext, ActionEnum.Insert,
                            solutionFlags, doNotAddFilter, justify,
                            false/* backchain */,
                            DefaultEvaluationPlanFactory2.INSTANCE);

            final IJoinNexus joinNexus = joinNexusFactory.newInstance(database
                    .getIndexManager());

            final long mutationCount = joinNexus.runMutation(program);

            final long elapsed = System.currentTimeMillis() - begin;

            return new ClosureStats(mutationCount, elapsed);

        } catch (Exception ex) {

            throw new RuntimeException(ex);
            
        }
        
    }
    
    
    /**
     * Turn a bigdata IStep (either a program or a rule) into a set of IRIS
     * rules.  Uses recursion where needed.
     * 
     * @param step 
     *              the bigdata step
     * 
     * @param rules 
     *              the set of IRIS rules
     */
    private void convertToIRISProgram(IStep step, Collection<IRule> rules) {

        if (step.isRule()) {
            
            com.bigdata.relation.rule.IRule bigdataRule =
                (com.bigdata.relation.rule.IRule) step;

            rules.add(convertToIRISRule(bigdataRule));
            
        } else {
            
            IProgram program = (IProgram) step;
            
            Iterator<IStep> substeps = program.steps();
    
            while (substeps.hasNext()) {
                
                IStep substep = substeps.next();
                
                convertToIRISProgram(substep, rules);
                
            }

        }
        
    }
    
    /**
     * Convert a bigdata rule into an IRIS rule.
     * 
     * @param bigdataRule
     *                  the bigdata rule
     * @return
     *                  the IRIS rule
     */
    private IRule convertToIRISRule(
        com.bigdata.relation.rule.IRule bigdataRule) {
        
        ILiteral[] head = new ILiteral[1];
        
        head[0] = convertToIRISLiteral(bigdataRule.getHead());
        
        int tailCount = bigdataRule.getTailCount();
        
        int constraintCount = bigdataRule.getConstraintCount();
        
        ILiteral[] body = new ILiteral[tailCount+constraintCount];
        
        for (int i = 0; i < tailCount; i++) {
            
            body[i] = convertToIRISLiteral(bigdataRule.getTail(i));
            
        }
        
        for (int i = 0; i < constraintCount; i++) {
            
            body[tailCount+i] = 
                convertToIRISLiteral(bigdataRule.getConstraint(i));
            
        }
        
        IRule rule = BASIC.createRule(
            Arrays.asList(head), 
            Arrays.asList(body)
            );
        /*
        if (log.isInfoEnabled()) {
            log.info("rule: " + bigdataRule);
            log.info("converted to: " + rule);
        }
        */
        return rule;
        
    }
    
    /**
     * Convert a bigdata predicate to an IRIS literal.
     * 
     * @param bigdataPred
     *                  the bigdata predicate
     * @return
     *                  the IRIS literal
     */
    private ILiteral convertToIRISLiteral(IPredicate bigdataPred) {

        ITerm[] terms = new ITerm[bigdataPred.arity()];
        
        for (int i = 0; i < terms.length; i++) {
            
            IVariableOrConstant<Long> bigdataTerm = bigdataPred.get(i);
            
            if (bigdataTerm.isConstant()) {

                terms[i] = TERM.createString(String.valueOf(bigdataTerm.get()));
                
            } else {
                
                terms[i] = TERM.createVariable(bigdataTerm.getName());
                
            }
            
        }
        
        return BASIC.createLiteral(
            true, 
            BASIC.createPredicate("triple", terms.length),
            BASIC.createTuple(terms)
            );
        
    }
    
    /**
     * Convert a bigdata constraint to an IRIS literal.
     * 
     * @param constraint
     *                  the bigdata constraint
     * @return
     *                  the IRIS literal
     */
    private ILiteral convertToIRISLiteral(IConstraint constraint) {

        if (constraint instanceof NE) {
            
            return convertToIRISLiteral((NE) constraint);
               
        } else if (constraint instanceof NEConstant) {
            
            return convertToIRISLiteral((NEConstant) constraint);
            
        } else if (constraint instanceof EQ) {
            
            return convertToIRISLiteral((EQ) constraint);
            
        } else if (constraint instanceof EQConstant) {
            
            return convertToIRISLiteral((EQConstant) constraint);
            
        } else if (constraint instanceof IN) {
            
            return convertToIRISLiteral((IN) constraint);
            
        } else if (constraint instanceof OR) {
            
            return convertToIRISLiteral((OR) constraint);
            
        }  
        
        throw new IllegalArgumentException(
            "unrecognized constraint type: " + constraint.getClass()
            );
        
    }
        
    /**
     * Convert a bigdata constraint to an IRIS literal.
     * 
     * @param constraint
     *                  the bigdata constraint
     * @return
     *                  the IRIS literal
     */
    private ILiteral convertToIRISLiteral(NE constraint) {

        ITerm t0 = TERM.createVariable(constraint.x.getName());
        
        ITerm t1 = TERM.createVariable(constraint.y.getName());
        
        return BASIC.createLiteral(
            true, 
            BUILTINS.createUnequal(t0, t1)
            );
        
    }
    
    /**
     * Convert a bigdata constraint to an IRIS literal.
     * 
     * @param constraint
     *                  the bigdata constraint
     * @return
     *                  the IRIS literal
     */
    private ILiteral convertToIRISLiteral(NEConstant constraint) {

        ITerm t0 = TERM.createVariable(constraint.var.getName());
        
        ITerm t1 = TERM.createString(String.valueOf(constraint.val.get()));
        
        return BASIC.createLiteral(
            true, 
            BUILTINS.createUnequal(t0, t1)
            );
        
    }
    
    /**
     * Convert a bigdata constraint to an IRIS literal.
     * 
     * @param constraint
     *                  the bigdata constraint
     * @return
     *                  the IRIS literal
     */
    private ILiteral convertToIRISLiteral(EQ constraint) {

        ITerm t0 = TERM.createVariable(constraint.x.getName());
        
        ITerm t1 = TERM.createVariable(constraint.y.getName());
        
        return BASIC.createLiteral(
            true, 
            BUILTINS.createEqual(t0, t1)
            );
        
    }
    
    /**
     * Convert a bigdata constraint to an IRIS literal.
     * 
     * @param constraint
     *                  the bigdata constraint
     * @return
     *                  the IRIS literal
     */
    private ILiteral convertToIRISLiteral(EQConstant constraint) {

        ITerm t0 = TERM.createVariable(constraint.var.getName());
        
        ITerm t1 = TERM.createString(String.valueOf(constraint.val.get()));
        
        return BASIC.createLiteral(
            true, 
            BUILTINS.createEqual(t0, t1)
            );
        
    }
    
    /**
     * Convert a bigdata constraint to an IRIS literal.
     * 
     * @param constraint
     *                  the bigdata constraint
     * @return
     *                  the IRIS literal
     */
    private ILiteral convertToIRISLiteral(IN constraint) {

        throw new UnsupportedOperationException("not yet implemented");
        
    }
    
    /**
     * Convert a bigdata constraint to an IRIS literal.
     * 
     * @param constraint
     *                  the bigdata constraint
     * @return
     *                  the IRIS literal
     */
    private ILiteral convertToIRISLiteral(OR constraint) {

        throw new UnsupportedOperationException("not yet implemented");
        
    }
    
    /**
     * Convert a set of IRIS rules into a bigdata program.
     * 
     * @param db
     *                  the abstract triple store
     * @param rules
     *                  the IRIS rules
     * @return
     *                  the bigdata program
     */
    private Program convertToBigdataProgram(AbstractTripleStore db, 
            TempMagicStore focus, Collection<IRule> rules) {

/*
All IRIS rules seem to have one literal in the head and zero or more body 
literals.  We need to make sure that a bigdata rule with no tails will
automatically fire (create the head).  I'll need to check the literal in
the body and create a bigdata predicate or constraint based on the literal
type (triple vs. NOT_EQUAL for example).
 */
//        MappedProgram program = new MappedProgram("magicProgram",
//                null/* focusStore */, true/* parallel */, true/* closure */
//                );
        
        MagicProgram program = new MagicProgram("magicProgram",
                focus.getSPORelation().getNamespace(), 
                true/* parallel */, true/* closure */
                );
        
        int i = 0;
        
        for (IRule rule : rules) {
            
            assert(rule.getHead().size() == 1);
            
            IPredicate head = 
                convertToBigdataPredicate(db, focus, rule.getHead().get(0));
            
            Collection<IPredicate> tails = new LinkedList<IPredicate>();
            
            Collection<IConstraint> constraints = new LinkedList<IConstraint>();
            
            List<ILiteral> body = rule.getBody();
            
            for (ILiteral literal : body) {
            
                org.deri.iris.api.basics.IPredicate p = literal.getAtom().getPredicate();
                
                if (p.equals(EQUAL) || p.equals(NOT_EQUAL)) {
                    
                    constraints.add(convertToBigdataConstraint(literal));
                    
                } else {

                    tails.add(convertToBigdataPredicate(db, focus, literal));
                    
                }

            }
            
            // the datalog program has facts that need to be asserted
            if (tails.size() == 0) {
            
                if (head instanceof MagicPredicate) {
                    
                    MagicPredicate p = (MagicPredicate) head;
                    
                    if (p.isFullyBound() == false) {
                        
                        throw new RuntimeException("????");
                        
                    }
                    
                    MagicRelation relation = (MagicRelation)
                        focus.getIndexManager().getResourceLocator().locate(
                                p.getOnlyRelationName(), focus.getTimestamp());
                    
                    IMagicTuple tuple = p.toMagicTuple();
                    
                    log.info("inserting magic tuple: " + tuple);
                    
                    long numInserted = relation.insert(new IMagicTuple[] { tuple }, 1);
                    
                    log.info("inserted: " + numInserted);
                    
                } else {
                    
                    throw new RuntimeException("????");
                    
                }
                
            } else {
            
                program.addStep(new Rule(
                    "magic"+i++,
                    head,
                    tails.toArray(new IPredicate[tails.size()]),
                    constraints.toArray(new IConstraint[constraints.size()])
                    ));

            }
            
        }
        
        return program;
        
    }
   
    /**
     * Convert an IRIS literal to a bigdata predicate.
     * 
     * @param literal
     *              the IRIS literal
     * @return
     *              the bigdata predicate
     */
    private IPredicate convertToBigdataPredicate(
        AbstractTripleStore db, TempMagicStore focus, ILiteral literal) {

        if (TRIPLE.equals(literal.getAtom().getPredicate())) {
            
            ITuple tuple = literal.getAtom().getTuple();
    
            IVariableOrConstant<Long> s = convertToBigdataTerm(tuple.get(0));
            
            IVariableOrConstant<Long> p = convertToBigdataTerm(tuple.get(1));
            
            IVariableOrConstant<Long> o = convertToBigdataTerm(tuple.get(2));
            
            return new SPOPredicate(
                db.getSPORelation().getNamespace(), // will get set correctly later by TMUtility
                s, p, o
                );

        } else { // magic
            
            ITuple tuple = literal.getAtom().getTuple();
            
            org.deri.iris.api.basics.IPredicate predicate = 
                literal.getAtom().getPredicate();
            
            IVariableOrConstant<Long>[] terms = 
                new IVariableOrConstant[predicate.getArity()];
            
            for (int i = 0; i < terms.length; i++) {
                
                terms[i] = convertToBigdataTerm(tuple.get(i));
                
            }
            
            // createRelation tests for existence first
            MagicRelation relation = focus.createRelation(
                    predicate.getPredicateSymbol(), predicate.getArity());
            
            return new MagicPredicate(
                relation.getNamespace(),
                terms
                );
            
        }
        
    }
    
    /**
     * Convert an IRIS term to a bigdata term (IVariableOrConstant).
     * 
     * @param term
     *              the IRIS term
     * @return
     *              the bigdata term
     */
    private IVariableOrConstant<Long> convertToBigdataTerm(ITerm term) {
     
        String value = (String) term.getValue();
        
        if (term.isGround()) {
            
            return new Constant<Long>(Long.valueOf(value));
            
        } else {
            
            return Var.var(value);
            
        }
        
    }
    
    /**
     * Convert an IRIS literal to a bigdata constraint.
     * 
     * @param literal
     *              the IRIS literal
     * @return
     *              the bigdata constraint
     */
    private IConstraint convertToBigdataConstraint(ILiteral literal) {

        ITuple tuple = literal.getAtom().getTuple();
        
        org.deri.iris.api.basics.IPredicate p = literal.getAtom().getPredicate(); 
        
        if (NOT_EQUAL.equals(p)) {

            ITerm t0 = tuple.get(0);
            
            ITerm t1 = tuple.get(1);
            
            if (t1.isGround()) {
                
                return new NEConstant(
                    Var.var((String)t0.getValue()),
                    new Constant<Long>(Long.valueOf((String)t1.getValue()))
                    );
                
            } else {
                
                return new NE(
                    Var.var((String)t0.getValue()),
                    Var.var((String)t1.getValue())
                    );                    
                
            }

        } else if (EQUAL.equals(p)) {

            ITerm t0 = tuple.get(0);
            
            ITerm t1 = tuple.get(1);
            
            if (t1.isGround()) {
                
                return new EQConstant(
                    Var.var((String)t0.getValue()),
                    new Constant<Long>(Long.valueOf((String)t1.getValue()))
                    );
                
            } else {
                
                return new EQ(
                    Var.var((String)t0.getValue()),
                    Var.var((String)t1.getValue())
                    );                    
                
            }
            
        } else {
            
            throw new UnsupportedOperationException("unrecognized predicate: " + p);
            
        }
        
    }
    
}
