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

package com.bigdata.rdf.magic;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.deri.iris.api.IProgramOptimisation.Result;
import org.deri.iris.api.basics.ILiteral;
import org.deri.iris.api.basics.IQuery;
import org.deri.iris.api.basics.ITuple;
import org.deri.iris.api.factory.IBasicFactory;
import org.deri.iris.api.factory.IBuiltinsFactory;
import org.deri.iris.api.factory.ITermFactory;
import org.deri.iris.api.terms.ITerm;
import org.deri.iris.basics.BasicFactory;
import org.deri.iris.builtins.BuiltinsFactory;
import org.deri.iris.optimisations.magicsets.MagicSets;
import org.deri.iris.terms.TermFactory;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.constraint.EQ;
import com.bigdata.bop.constraint.EQConstant;
import com.bigdata.bop.constraint.INBinarySearch;
import com.bigdata.bop.constraint.NE;
import com.bigdata.bop.constraint.NEConstant;
import com.bigdata.bop.constraint.OR;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.rules.MappedProgram;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.Rule;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class IRISUtils {

    protected static final Logger log = Logger.getLogger(IRISUtils.class);
    
    protected static final boolean INFO = log.isInfoEnabled();
    
    protected static final boolean DEBUG = log.isDebugEnabled();
    
    
    final static IBasicFactory BASIC = BasicFactory.getInstance();
    
    final static ITermFactory TERM = TermFactory.getInstance();
    
    final static IBuiltinsFactory BUILTINS = BuiltinsFactory.getInstance();
    
    final static org.deri.iris.api.basics.IPredicate EQUAL = 
        BASIC.createPredicate("EQUAL", 2);
    
    final static org.deri.iris.api.basics.IPredicate NOT_EQUAL = 
        BASIC.createPredicate("NOT_EQUAL", 2);
    
    final static org.deri.iris.api.basics.IPredicate TRIPLE = 
        BASIC.createPredicate("triple", 3);
    

    /**
     * Take a bigdata program and a bigdata rule and create a modified program
     * using a magic sets optimization.  This will dramatically limit the amount
     * of inference needed to answer the original query.
     * <p>
     * This method will cause writes to be made on the focus store.  See
     * {@link #irisToBigdata(AbstractTripleStore, TempMagicStore, Collection)}
     * for details.
     * 
     * @param baseProgram
     *                  the original bigdata program
     * @param query
     *                  the bigdata query
     * @param store
     *                  the database
     * @param focusStore
     *                  the focus store, where magic relations and predicates
     *                  will go
     * @return
     */
    public static Program magicSets(Program baseProgram, IRule query, 
            AbstractTripleStore store, TempMagicStore focusStore) {
        
        // create the magic sets optimizer
        IQuery irisQuery = convertToIRISQuery(query);
        Collection<org.deri.iris.api.basics.IRule> irisProgram = 
            bigdataToIRIS(baseProgram);
        
        MagicSets magicSets = new MagicSets();
        Result result = magicSets.optimise(irisProgram, irisQuery);
        
        if (INFO) {
            log.info("prolog program after magic sets:");
            for (org.deri.iris.api.basics.IRule rule : result.rules) {
                log.info(rule);
            }
        }
        
        return irisToBigdata(result.rules, store, focusStore);
      
    }
    
    /**
     * Turn a bigdata query into an IRIS query.
     * 
     * @param query
     *              the bigdata query
     * @return
     *              the IRIS query
     */
    private static IQuery convertToIRISQuery(IRule query) {
        
        ILiteral[] literals = 
            new ILiteral[query.getTailCount() + query.getConstraintCount()];
        for (int i = 0; i < query.getTailCount(); i++) {
            literals[i] = convertToIRISLiteral(query.getTail(i));
        }
        for (int i = 0; i < query.getConstraintCount(); i++) {
            literals[i+query.getTailCount()] = 
                convertToIRISLiteral(query.getConstraint(i));
        }
        
        IQuery irisQuery = BASIC.createQuery(literals);
            
        if (INFO) {
            log.info("query:");
            log.info(irisQuery);
        }
        
        return irisQuery;
        
    }
    
    /**
     * Turn a bigdata Program into a set of IRIS rules.
     * 
     * @param step 
     *              the bigdata step
     * @return 
     *              the set of IRIS rules
     */
    public static Collection<org.deri.iris.api.basics.IRule> bigdataToIRIS(
            Program bigdataProgram) {
        
        Collection<org.deri.iris.api.basics.IRule> irisProgram = 
            new LinkedList<org.deri.iris.api.basics.IRule>();
        bigdataToIRIS(bigdataProgram, irisProgram);
        return irisProgram;
        
    }
    
    /**
     * Turn a bigdata IStep (either a program or a rule) into a set of IRIS
     * rules.  Uses recursion where needed.
     * 
     * @param step 
     *              the bigdata step
     * @param rules 
     *              the set of IRIS rules
     */
    private static void bigdataToIRIS(IStep step, 
            Collection<org.deri.iris.api.basics.IRule> rules) {

        if (step.isRule()) {
            IRule bigdataRule = (IRule) step;
            rules.add(convertToIRISRule(bigdataRule));
        } else {
            IProgram program = (IProgram) step;
            Iterator<IStep> substeps = program.steps();
            while (substeps.hasNext()) {
                IStep substep = substeps.next();
                bigdataToIRIS(substep, rules);
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
    private static org.deri.iris.api.basics.IRule convertToIRISRule(
            IRule bigdataRule) {
        
        ILiteral[] head = new ILiteral[1];
        head[0] = convertToIRISLiteral(bigdataRule.getHead());
        int tailCount = bigdataRule.getTailCount();
        int constraintCount = bigdataRule.getConstraintCount();
        ILiteral[] body = new ILiteral[tailCount + constraintCount];
        for (int i = 0; i < tailCount; i++) {
            body[i] = convertToIRISLiteral(bigdataRule.getTail(i));
        }
        for (int i = 0; i < constraintCount; i++) {
            body[tailCount + i] =
                    convertToIRISLiteral(bigdataRule.getConstraint(i));
        }
        org.deri.iris.api.basics.IRule rule = BASIC.createRule(
            Arrays.asList(head), 
            Arrays.asList(body)
            );
        
        if (log.isInfoEnabled()) {
            log.info("rule: " + bigdataRule);
            log.info("converted to: " + rule);
        }
        
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
    private static ILiteral convertToIRISLiteral(IPredicate bigdataPred) {

        if (bigdataPred instanceof SPOPredicate == false) {
            throw new RuntimeException("only SPO predicates for now");
        }
        
        ITerm[] terms = new ITerm[bigdataPred.arity()];
        for (int i = 0; i < terms.length; i++) {
            final IVariableOrConstant<IV> bigdataTerm = bigdataPred.get(i);
            if (bigdataTerm.isConstant()) {
                final IV iv = bigdataTerm.get();
                terms[i] = TERM.createString(iv.toString());
            } else {
                terms[i] = TERM.createVariable(bigdataTerm.getName());
            }
        }
        
        return BASIC.createLiteral(
            true, 
            TRIPLE,
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
    private static ILiteral convertToIRISLiteral(IConstraint constraint) {

        if (constraint instanceof NE) {
            return convertToIRISLiteral((NE) constraint);
        } else if (constraint instanceof NEConstant) {
            return convertToIRISLiteral((NEConstant) constraint);
        } else if (constraint instanceof EQ) {
            return convertToIRISLiteral((EQ) constraint);
        } else if (constraint instanceof EQConstant) {
            return convertToIRISLiteral((EQConstant) constraint);
        } else if (constraint instanceof INBinarySearch) {
            return convertToIRISLiteral((INBinarySearch) constraint);
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
    private static ILiteral convertToIRISLiteral(final NE constraint) {

        final ITerm t0 = TERM.createVariable(((IVariable<?>) constraint.get(0))
                .getName());

        final ITerm t1 = TERM.createVariable(((IVariable<?>) constraint.get(1))
                .getName());

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
    private static ILiteral convertToIRISLiteral(final NEConstant constraint) {

        final ITerm t0 = TERM.createVariable(((IVariable<?>) constraint.get(0))
                .getName());

        final ITerm t1 = TERM.createString(String
                .valueOf(((IConstant<?>) constraint.get(1)).get()));

//        ITerm t1 = TERM.createString(String.valueOf(constraint.val.get()));
        
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
    private static ILiteral convertToIRISLiteral(final EQ constraint) {

        final ITerm t0 = TERM.createVariable(((IVariable<?>) constraint.get(0))
                .getName());

        final ITerm t1 = TERM.createVariable(((IVariable<?>) constraint.get(1))
                .getName());

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
    private static ILiteral convertToIRISLiteral(final EQConstant constraint) {

        final ITerm t0 = TERM.createVariable(((IVariable<?>) constraint.get(0))
                .getName());

        final ITerm t1 = TERM.createString(String
                .valueOf(((IConstant<?>) constraint.get(1)).get()));

//        ITerm t0 = TERM.createVariable(constraint.var.getName());
//        ITerm t1 = TERM.createString(String.valueOf(constraint.val.get()));
        
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
    private static ILiteral convertToIRISLiteral(INBinarySearch constraint) {

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
    private static ILiteral convertToIRISLiteral(OR constraint) {

        throw new UnsupportedOperationException("not yet implemented");
        
    }
    
    /**
     * Convert a set of IRIS rules into a bigdata program.  Along the way, this
     * will create the necessary relations in the focus store to house the magic
     * predicates, and may create specific instances of magic predicates in the
     * focus store as well.
     * 
     * @param irisProgram
     *                  the IRIS rules
     * @param store
     *                  the abstract triple store
     * @param focusStore
     *                  the focus store, where magic predicates will be written
     *                  to / read from
     * @return
     *                  the bigdata program
     */
    public static Program irisToBigdata(
            Collection<org.deri.iris.api.basics.IRule> irisProgram,
            AbstractTripleStore store, 
            TempMagicStore focusStore) {

/*
All IRIS rules seem to have one literal in the head and zero or more body 
literals.  We need to make sure that a bigdata rule with no tails will
automatically fire (create the head).  I'll need to check the literal in
the body and create a bigdata predicate or constraint based on the literal
type (triple vs. NOT_EQUAL for example).
 */
        
        MappedProgram bigdataProgram = new MappedProgram("magicProgram",
                focusStore.getSPORelation().getNamespace(), 
                true/* parallel */, true/* closure */
                );
        
        int i = 0;
        
        for (org.deri.iris.api.basics.IRule rule : irisProgram) {
            
            assert(rule.getHead().size() == 1);
            
            IPredicate head = convertToBigdataPredicate(
                    store, focusStore, rule.getHead().get(0));
            Collection<IPredicate> tails = new LinkedList<IPredicate>();
            Collection<IConstraint> constraints = new LinkedList<IConstraint>();
            List<ILiteral> body = rule.getBody();
            for (ILiteral literal : body) {
                org.deri.iris.api.basics.IPredicate p = 
                    literal.getAtom().getPredicate();
                if (p.equals(EQUAL) || p.equals(NOT_EQUAL)) {
                    constraints.add(convertToBigdataConstraint(literal));
                } else {
                    tails.add(convertToBigdataPredicate(
                            store, focusStore, literal));
                }
            }
            
            // the datalog program has facts that need to be asserted
            if (tails.size() == 0) {
                if (head.getVariableCount() > 0) {
                    throw new RuntimeException(
                            "iris rule with unbound variables in the " +
                            "head and no tails: does not compute");
                }
                if (head instanceof MagicPredicate) {
                    
                    final MagicPredicate p = (MagicPredicate) head;
                    // get the proper relation for this predicate
                    IResourceLocator<?> locator = 
                        focusStore.getIndexManager().getResourceLocator();
                    final MagicRelation relation = (MagicRelation) locator.locate(
                            p.getOnlyRelationName(), focusStore.getTimestamp());
                    final MagicTuple tuple = new MagicTuple(p);
                    if (INFO) log.info("inserting magic tuple: " + tuple);
                    long numInserted = relation.insert(
                            new IMagicTuple[] { tuple }, 1);
                    if (INFO) log.info("inserted: " + numInserted);
                    
                } else {
                    
                    final SPOPredicate p = (SPOPredicate) head;
                    final SPO spo = new SPO(p);
                    if (INFO) log.info("inserting spo: " + spo);
                    long numInserted = focusStore.addStatements(
                            new SPO[] { spo }, 1);
                    if (INFO) log.info("inserted: " + numInserted);
                    
                }
            } else {
                
                bigdataProgram.addStep(new Rule(
                    "magic"+i++,
                    head,
                    tails.toArray(new IPredicate[tails.size()]),
                    constraints.toArray(new IConstraint[constraints.size()])
                    ));
                
            }
        }
        
        return bigdataProgram;
        
    }
   
    /**
     * Convert an IRIS literal to a bigdata predicate.
     * 
     * @param literal
     *              the IRIS literal
     * @return
     *              the bigdata predicate
     */
    private static IPredicate convertToBigdataPredicate(
        AbstractTripleStore db, TempMagicStore focus, ILiteral literal) {

        if (TRIPLE.equals(literal.getAtom().getPredicate())) {
            
            ITuple tuple = literal.getAtom().getTuple();
            IVariableOrConstant<IV> s = convertToBigdataTerm(tuple.get(0));
            IVariableOrConstant<IV> p = convertToBigdataTerm(tuple.get(1));
            IVariableOrConstant<IV> o = convertToBigdataTerm(tuple.get(2));
            
            return new SPOPredicate(
                db.getSPORelation().getNamespace(), // will get set correctly later by TMUtility
                s, p, o
                );

        } else { // magic
            
            ITuple tuple = literal.getAtom().getTuple();
            org.deri.iris.api.basics.IPredicate predicate = 
                literal.getAtom().getPredicate();
            IVariableOrConstant<IV>[] terms = 
                new IVariableOrConstant[predicate.getArity()];
            for (int i = 0; i < terms.length; i++) {
                terms[i] = convertToBigdataTerm(tuple.get(i));
            }
            
            // create the magic relation for this predicate, if necessary
            MagicRelation relation = 
                focus.getMagicRelation(predicate.getPredicateSymbol());
            if (relation == null) {
                
                if (INFO) log.info("creating new magic relation: " + 
                        predicate.getPredicateSymbol());
            
                relation = focus.createRelation(
                    predicate.getPredicateSymbol(), predicate.getArity());
                
            }
            
            return new MagicPredicate(terms, NV.asMap(new NV[] { new NV(
                    IPredicate.Annotations.RELATION_NAME, new String[]{relation
                            .getNamespace()}) }));

        }
        
    }
    
    /**
     * Convert an IRIS term to a bigdata term (IVariableOrConstant).
     * <p>
     * 
     * @param term
     *              the IRIS term
     * @return
     *              the bigdata term
     */
    private static IVariableOrConstant<IV> convertToBigdataTerm(ITerm term) {
     
        String value = (String) term.getValue();
        if (term.isGround()) {
            return new Constant<IV>(IVUtility.fromString(value));
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
    private static IConstraint convertToBigdataConstraint(ILiteral literal) {

        ITuple tuple = literal.getAtom().getTuple();
        org.deri.iris.api.basics.IPredicate p = 
            literal.getAtom().getPredicate(); 
        
        ITerm t0 = tuple.get(0);
        ITerm t1 = tuple.get(1);
        
        if (NOT_EQUAL.equals(p)) {

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
