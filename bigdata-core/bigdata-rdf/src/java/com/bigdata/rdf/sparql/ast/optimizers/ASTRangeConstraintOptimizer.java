/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Nov 20, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.bop.IPredicate;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.AbstractIV;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.spo.SPOKeyOrder;

/**
 * AST optimizer recognizes datatype and/or value range constraints and applies
 * them to as range constraints to {@link StatementPatternNode}s.
 * <p>
 * Datatype constraints on a variable always bind on the Object position of a
 * statement as it is only in the Object position that a Literal may be bound.
 * Due to the nature of the indices, which are organized in key ranges for each
 * of the {@link DTE}s, a datatype constraint implies one or more key ranges of
 * a statement index, depending on whether the datatype constraint is "ground"
 * (a concrete datatype) or non-ground (a set of datatypes, any of which are
 * legal given the value expressions in which the variable may appear). This
 * requires reasoning about datatype constraints, including the most restrictive
 * hierarchy of implicit conversions which are permitted by the value
 * expressions in which the variable with the range constraint is used.
 * <p>
 * The most direct way to specify a datatype constraint is to explicit specify a
 * datatype using {@link FunctionRegistry#DATATYPE}. However, even in this case
 * the datatype constraint may be part of an OR, in which case the effective
 * datatype constraint is the UNION of the specified datatypes. Likewise, a
 * value expression which will provably fail (e.g., by causing a type error) for
 * a datatype effectively provides an exclusion for that datatype.
 * <p>
 * Range constraints are identified by the comparator operators (GT, GTE, LT,
 * LTE). Range constraints apply to all datatypes which the variable may take
 * on. The most effectively constraint occurs when the datatype and the range
 * are both known. For example, the variable has a known datatype constraint of
 * <code>xsd:float</code> and a value range constraint of <code>(4.:5.]</code>.
 * <p>
 * Datatype and range constraints have the most utility where we would otherwise
 * be 0-bound statement index (OS(C)P). The utility of the constraint decreases
 * as the #of known bound components of the key increases. E.g., a datatype or
 * value range constraint on PO(C)S provides less utility than one one OS(C)P,
 * etc. This is because the datatype / value range constraint does less to
 * improve the selectivity of the predicate as the #of known bound key
 * components increases.
 * 
 * <h3>Modeling non-ground datatypes</h3>
 * 
 * The most effective way to model non-ground datatypes is with a UNION of the
 * SPs for the set of ground datatypes. However, due to the nature of default
 * graph access path semantics, this UNION MUST be expressed by an expander
 * pattern until we find an alternative way to model default graphs of compound
 * predicates. Otherwise the DISTINCT SPO semantics of the default graph will
 * not be applied across all produced solutions for a given source solution.
 * This wrinkle does not apply for triples-mode joins, sids-mode joins, or for
 * named graph joins, all of which may be translated into a UNION of the SPs for
 * the distinct allowable ground datatypes.
 * <p>
 * See <a href="https://sourceforge.net/apps/trac/bigdata/ticket/407" >Default
 * graphs always uses SCAN + FILTER and lacks efficient PARALLEL SUBQUERY code
 * path 407</a> for more on this issue.
 * 
 * <h3>Forming the to/from key</h3>
 * 
 * Datatype constraints may be applied to both inline and non-inline IVs. For
 * example, we can apply a datatype constraint which excludes everything except
 * for inline Unicode datatypes. However, in order to be consistent it must be
 * that the {@link ILexiconConfiguration} is such that NO values consistent with
 * the datatype constraint may appear outside of that implied key constraint.
 * For example, it must not be possible that values of that datatype could
 * appear in either the TERMS or BLOBS index. Thus, applying datatype
 * constraints to non-inline IVs requires reasoning about the
 * {@link ILexiconConfiguration}.
 * <p>
 * The following schema was excerpted from {@link AbstractIV}. See that class
 * for more detailed and authoriative information about the encoding of
 * {@link IV}s.
 * 
 * <pre>
 * [valueType]    : 2 bits (Literal)
 * [inline]       : 1 bit  (true)
 * [extension]    : 1 bit   (false unless datatype is an extension type)
 * [dataTypeCode] : 4 bits (the DTE code)
 * ----
 * extensionIV    : IFF the datatype is an extension type.
 * ----
 * natural encoding of the value for the specified DTE.
 * </pre>
 * 
 * Value ranges constraints apply only to inline {@link IV}s. This means that
 * the valueType will always be {@link VTE#LITERAL} and the bit flag
 * inline:=true. The extension bit will either be set or cleared depending on
 * whether the datatype is an extension type. (Reasoning about extension types
 * would require an extension to how they are declared.)
 * <p>
 * The value range constraint, if any, is applied after the optional extension
 * {@link IV}. This means that all statements which satisfy the {@link DTE}, the
 * optional extensionIV, and the value range constraint will actually have the
 * correct datatype.
 * 
 * TODO Static optimizer. The static optimizer must put the joins into an order
 * which is consistent with the selectivity of the predicates. When a predicate
 * has a datatype and/or value range constraint, then that MUST be considered by
 * the static optimizer in order to produce a join ordering which benefits from
 * the added selectivity of that constraint.
 * <p>
 * The static optimizer needs to treat SPs with attached datatype and/or range
 * constraints as "somewhat" more bound. When the statement index would begin
 * with O, e.g., OSP or OSCP, the SP is effectively 1-bound rather than 0-bound.
 * If the datatype is ground, then the range count for that datatype and value
 * range is the range count of interest for the purposes of the static join
 * order optimizer. If the datatype is non-ground, then the sum of the fast
 * range counts across each possible ground datatype for the value range
 * constraint (when cast to the appropriate {@link DTE}) gives the effective
 * range count.
 * 
 * TODO Identify implicit datatype constraints by examining the in scope value
 * expression(s) in which each variable appears and determining which datatypes
 * are (in)consistent with those value expression(s).
 * 
 * TODO Historically, the range constraints were attached as RangeBOp AND left
 * in place as normal FILTERs. This is because the range constraints were not
 * integrated into the optimizers in any depth. If the RangeBOp would up
 * attached to a JOIN where it could be imposed, then it was. If not, then the
 * FILTERs would handle the constraint eventually. The code in RangeBOp reflects
 * this practice.
 * 
 * TODO Integrate code to attach RangeBOps to predicates. (This code is from the
 * old BigdataEvaluationStrategyImpl3 class. It should be moved into an
 * IASTOptimizer for recognizing range constraints.)
 * 
 * TODO Ranges can be just upper or lower. They do not need to be both. They can
 * even be an excluded middle. Ranges can also be just a datatype constraint,
 * since that implies the key range in the OS(C)P index in which the variable
 * may take on that datatype.
 * 
 * TODO The big thing about handling range constraints is make sure that we
 * query each part of the OS(C)P index which corresponds to a datatype which
 * could have a value legally promotable within the context in which the
 * LT/LTE/GT/GTE filter(s) occur. For example, x>5 && x<10 needs to do a
 * key-range scan for xsd:int, xsd:integer, .... The big win comes when we can
 * recognize a datatype constraint at the same time such that we only touch one
 * part of the index for that key range of values.
 * 
 * TODO Each GT(E)/LT(E) constraint should be broken down into a separate filter
 * so we can apply one even when the other might depend on a variable which is
 * not yet bound.
 * 
 * @see SPOKeyOrder#getFromKey(IKeyBuilder, IPredicate)
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/238 (lift range
 *      constraints onto access path)
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/407 (default graph join
 *      optimization)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTRangeConstraintOptimizer.java 5704 2011-11-20 15:37:22Z
 *          thompsonbry $
 */
public class ASTRangeConstraintOptimizer implements IASTOptimizer {

    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        // TODO Auto-generated method stub
        return null;
    }

}

//private void attachRangeBOps(final SOpGroup g) {
//
//  final Map<IVariable,Collection<IValueExpression>> lowerBounds =
//      new LinkedHashMap<IVariable,Collection<IValueExpression>>();
//  final Map<IVariable,Collection<IValueExpression>> upperBounds =
//      new LinkedHashMap<IVariable,Collection<IValueExpression>>();
//  
//  for (SOp sop : g) {
//      final BOp bop = sop.getBOp();
//      if (!(bop instanceof SPARQLConstraint)) {
//          continue;
//      }
//      final SPARQLConstraint c = (SPARQLConstraint) bop;
//      if (!(c.getValueExpression() instanceof CompareBOp)) {
//          continue;
//      }
//      final CompareBOp compare = (CompareBOp) c.getValueExpression();
//      final IValueExpression left = compare.get(0);
//      final IValueExpression right = compare.get(1);
//      final CompareOp op = compare.op();
//      if (left instanceof IVariable) {
//          final IVariable var = (IVariable) left;
//          final IValueExpression ve = right;
//          if (op == CompareOp.GE || op == CompareOp.GT) {
//              // ve is a lower bound
//              Collection bounds = lowerBounds.get(var);
//              if (bounds == null) {
//                  bounds = new LinkedList<IValueExpression>();
//                  lowerBounds.put(var, bounds);
//              }
//              bounds.add(ve);
//          } else if (op == CompareOp.LE || op == CompareOp.LT) {
//              // ve is an upper bound
//              Collection bounds = upperBounds.get(var);
//              if (bounds == null) {
//                  bounds = new LinkedList<IValueExpression>();
//                  upperBounds.put(var, bounds);
//              }
//              bounds.add(ve);
//          }
//      } 
//      if (right instanceof IVariable) {
//          final IVariable var = (IVariable) right;
//          final IValueExpression ve = left;
//          if (op == CompareOp.LE || op == CompareOp.LT) {
//              // ve is a lower bound
//              Collection bounds = lowerBounds.get(var);
//              if (bounds == null) {
//                  bounds = new LinkedList<IValueExpression>();
//                  lowerBounds.put(var, bounds);
//              }
//              bounds.add(ve);
//          } else if (op == CompareOp.GE || op == CompareOp.GT) {
//              // ve is an upper bound
//              Collection bounds = upperBounds.get(var);
//              if (bounds == null) {
//                  bounds = new LinkedList<IValueExpression>();
//                  upperBounds.put(var, bounds);
//              }
//              bounds.add(ve);
//          }
//      }
//  }
//  
//  final Map<IVariable,RangeBOp> rangeBOps = 
//      new LinkedHashMap<IVariable,RangeBOp>();
//  
//  for (IVariable v : lowerBounds.keySet()) {
//      if (!upperBounds.containsKey(v))
//          continue;
//      
//      IValueExpression from = null;
//      for (IValueExpression ve : lowerBounds.get(v)) {
//          if (from == null)
//              from = ve;
//          else
//              from = new MathBOp(ve, from, MathOp.MAX,this.tripleSource.getDatabase().getNamespace());
//      }
//
//      IValueExpression to = null;
//      for (IValueExpression ve : upperBounds.get(v)) {
//          if (to == null)
//              to = ve;
//          else
//              to = new MathBOp(ve, to, MathOp.MIN,this.tripleSource.getDatabase().getNamespace());
//      }
//      
//      final RangeBOp rangeBOp = new RangeBOp(v, from, to); 
//      
//      if (log.isInfoEnabled()) {
//          log.info("found a range bop: " + rangeBOp);
//      }
//      
//      rangeBOps.put(v, rangeBOp);
//  }
//  
//  for (SOp sop : g) {
//      final BOp bop = sop.getBOp();
//      if (!(bop instanceof IPredicate)) {
//          continue;
//      }
//      final IPredicate pred = (IPredicate) bop;
//      final IVariableOrConstant o = pred.get(2);
//      if (o.isVar()) {
//          final IVariable v = (IVariable) o;
//          if (!rangeBOps.containsKey(v)) {
//              continue;
//          }
//          final RangeBOp rangeBOp = rangeBOps.get(v);
//          final IPredicate rangePred = (IPredicate)
//              pred.setProperty(SPOPredicate.Annotations.RANGE, rangeBOp);
//          if (log.isInfoEnabled())
//              log.info("range pred: " + rangePred);
//          sop.setBOp(rangePred);
//      }
//  }
//}
//
//public void testSimpleRange() throws Exception {
//    
////  final Sail sail = new MemoryStore();
////  sail.initialize();
////  final Repository repo = new SailRepository(sail);
//
//  final BigdataSail sail = getSail();
//  try {
//  sail.initialize();
//  final BigdataSailRepository repo = new BigdataSailRepository(sail);
//  
//  final RepositoryConnection cxn = repo.getConnection();
//  
//  try {
//      cxn.setAutoCommit(false);
//
//      final ValueFactory vf = sail.getValueFactory();
//
//      /*
//       * Create some terms.
//       */
//      final URI mike = vf.createURI(BD.NAMESPACE + "mike");
//      final URI bryan = vf.createURI(BD.NAMESPACE + "bryan");
//      final URI person = vf.createURI(BD.NAMESPACE + "person");
//      final URI age = vf.createURI(BD.NAMESPACE + "age");
//      final Literal _1 = vf.createLiteral(1);
//      final Literal _2 = vf.createLiteral(2);
//      final Literal _3 = vf.createLiteral(3);
//      final Literal _4 = vf.createLiteral(4);
//      final Literal _5 = vf.createLiteral(5);
//      
//      /*
//       * Create some statements.
//       */
//      cxn.add(mike, age, _2);
//      cxn.add(mike, RDF.TYPE, person);
//      cxn.add(bryan, age, _4);
//      cxn.add(bryan, RDF.TYPE, person);
//      
//      /*
//       * Note: The either flush() or commit() is required to flush the
//       * statement buffers to the database before executing any operations
//       * that go around the sail.
//       */
//      cxn.commit();
//      
//      {
//          
//          String query =
//              QueryOptimizerEnum.queryHint(QueryOptimizerEnum.None) +
//              "prefix bd: <"+BD.NAMESPACE+"> " +
//              "prefix rdf: <"+RDF.NAMESPACE+"> " +
//              "prefix rdfs: <"+RDFS.NAMESPACE+"> " +
//              
//              "select * " +
//              "where { " +
////                "bd:productA bd:property1 ?origProperty1 . " +
////                "?product bd:property1 ?simProperty1 . " +
////                "FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120)) " +
//              "  ?x bd:age ?age . " +
//              "  ?x rdf:type bd:person . " +
//              "  filter(?age > 1 && ?age < 3) " +
//              "}"; 
//
//          final SailTupleQuery tupleQuery = (SailTupleQuery)
//              cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//          tupleQuery.setIncludeInferred(false /* includeInferred */);
//          
////        final Collection<BindingSet> answer = new LinkedList<BindingSet>();
////        answer.add(createBindingSet(
////                new BindingImpl("a", paul),
////                new BindingImpl("b", mary)
////                ));
////        answer.add(createBindingSet(
////                new BindingImpl("a", brad),
////                new BindingImpl("b", john)
////                ));
////
////        final TupleQueryResult result = tupleQuery.evaluate();
////          compare(result, answer);
//
//      }
//      
//  } finally {
//      cxn.close();
//  }
//  } finally {
//      sail.__tearDownUnitTest();//shutDown();
//  }
//
//}
