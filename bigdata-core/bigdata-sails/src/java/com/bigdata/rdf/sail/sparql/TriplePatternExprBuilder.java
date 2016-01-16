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
 * Created on Aug 27, 2011
 */

package com.bigdata.rdf.sail.sparql;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.eclipse.jetty.util.log.Log;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.sparql.ast.ASTBind;
import com.bigdata.rdf.sail.sparql.ast.ASTBlankNodePropertyList;
import com.bigdata.rdf.sail.sparql.ast.ASTCollection;
import com.bigdata.rdf.sail.sparql.ast.ASTObjectList;
import com.bigdata.rdf.sail.sparql.ast.ASTPathAlternative;
import com.bigdata.rdf.sail.sparql.ast.ASTPathElt;
import com.bigdata.rdf.sail.sparql.ast.ASTPathMod;
import com.bigdata.rdf.sail.sparql.ast.ASTPathOneInPropertySet;
import com.bigdata.rdf.sail.sparql.ast.ASTPathSequence;
import com.bigdata.rdf.sail.sparql.ast.ASTPropertyList;
import com.bigdata.rdf.sail.sparql.ast.ASTPropertyListPath;
import com.bigdata.rdf.sail.sparql.ast.ASTTRefPattern;
import com.bigdata.rdf.sail.sparql.ast.ASTVar;
import com.bigdata.rdf.sail.sparql.ast.Node;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.PathNode;
import com.bigdata.rdf.sparql.ast.PathNode.PathAlternative;
import com.bigdata.rdf.sparql.ast.PathNode.PathElt;
import com.bigdata.rdf.sparql.ast.PathNode.PathMod;
import com.bigdata.rdf.sparql.ast.PathNode.PathNegatedPropertySet;
import com.bigdata.rdf.sparql.ast.PathNode.PathOneInPropertySet;
import com.bigdata.rdf.sparql.ast.PathNode.PathSequence;
import com.bigdata.rdf.sparql.ast.optimizers.ASTPropertyPathOptimizer;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Class handles triple patterns and property paths.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @openrdf
 */
public class TriplePatternExprBuilder extends ValueExprBuilder {

	private static final transient Logger log = Logger.getLogger(TriplePatternExprBuilder.class);
	
    /**
     * @param context
     */
    public TriplePatternExprBuilder(final BigdataASTContext context) {
        super(context);
    }

    @Override
    final public Object visit(final ASTPropertyList propListNode, final Object data)
            throws VisitorException {
        
        final TermNode subject = (TermNode) data;
        
        final TermNode predicate = (TermNode) propListNode.getVerb().jjtAccept(
                this, null);
        
        @SuppressWarnings("unchecked")
        final List<TermNode> objectList = (List<TermNode>) propListNode
                .getObjectList().jjtAccept(this, null);

        for (TermNode object : objectList) {
            
            graphPattern.addSP(subject, predicate, object);
            
        }

        final ASTPropertyList nextPropList = propListNode.getNextPropertyList();
        
        if (nextPropList != null) {
            
            nextPropList.jjtAccept(this, subject);
            
        }

        return null;

    }

    @Override
    final public Object visit(final ASTPropertyListPath propListNode,
                final Object data) throws VisitorException {

        final TermNode subject = (TermNode) data;

        final BOp verb = (BOp) propListNode.getVerb().jjtAccept(
                this, data);

        @SuppressWarnings("unchecked")
        final List<TermNode> objectList = (List<TermNode>) propListNode
        	.getObjectList().jjtAccept(this, null);

        if (verb instanceof TermNode) {
        	
        	final TermNode predicate = (TermNode) verb;
        	
            for (TermNode object : objectList) {

            	graphPattern.addSP(subject, predicate, object);

            }
        	
        } else {
        
        	final PathAlternative pathAlt = (PathAlternative) verb;
        	
        	/*
        	 * Even simple triple patterns are going through the property path
        	 * code logic.  Easier to just recognize those here, although
        	 * we could just as easily do it in the optimizer.  But this would
        	 * break a lot of the test cases.
        	 */
        	if (ASTPropertyPathOptimizer.isSimpleIRI(pathAlt)) {
        		
        		final ConstantNode predicate = 
    				ASTPropertyPathOptimizer.getSimpleIRI(pathAlt);
        		
                for (TermNode object : objectList) {

                	graphPattern.addSP(subject, predicate, object);

                }
                
        	} else {
        	
        		if (log.isDebugEnabled())
        			log.debug(BOpUtility.toString(pathAlt));
            	
	        	final PathNode path = new PathNode(pathAlt);
	        	
		        for (TermNode object : objectList) {
		
		        	graphPattern.addPP(subject, path, object);
		
		        }
        		
        	}
	        
        }
        
//        if (verbPath instanceof VarNode) {
//
//            @SuppressWarnings("unchecked")
//            final List<TermNode> objectList = (List<TermNode>) propListNode
//                    .getObjectList().jjtAccept(this, null);
//
//            for (TermNode object : objectList) {
//
//                graphPattern.addSP(subject, verbPath, object);
//                
//            }
//
//        } else {
//
//            /*
//             * PROPERTY PATHS
//             * 
//             * Note: This code path is a single IRI (degenerative case) or a
//             * more complex path. This is handled by the aspect of the visitor
//             * which deals with property paths, even in the case where the
//             * triple pattern is as simple as (var const var).
//             * 
//             * FIXME This code block should be empty when we incorporate support
//             * for property paths from the Sesame visitor methods. It currently
//             * duplicates the code block above and provides handling for the
//             * case when the predicate is a constant.
//             * 
//             * Just need to make this block empty and let the visitor do the work.
//             */
//
////            @SuppressWarnings("unchecked")
////            final List<TermNode> objectList = (List<TermNode>) propListNode
////                    .getObjectList().jjtAccept(this, null);
////
////            for (TermNode object : objectList) {
////
////                graphPattern.addSP(subject, verbPath, object);
////                
////            }
//
//        }

        final ASTPropertyListPath nextPropList = propListNode
                .getNextPropertyList();

        if (nextPropList != null) {

            nextPropList.jjtAccept(this, subject);

        }

        return null;

    }

    @Override
    public PathAlternative visit(final ASTPathAlternative pathAltNode, Object data)
    		throws VisitorException {

    	final int numChildren = pathAltNode.jjtGetNumChildren();
    	
    	final PathSequence[] args = new PathSequence[numChildren];
    	
    	for (int i = 0; i < numChildren; i++) {
    	
    		args[i] = (PathSequence) pathAltNode.jjtGetChild(i).jjtAccept(this, data);
    		
    	}
    	
    	return new PathAlternative(args);
    	
	}

	@Override
	public PathSequence visit(ASTPathSequence pathSeqNode, Object data)
			throws VisitorException {

    	final int numChildren = pathSeqNode.jjtGetNumChildren();
    	
    	final PathElt[] args = new PathElt[numChildren];
    	
    	for (int i = 0; i < numChildren; i++) {
    	
    		args[i] = (PathElt) pathSeqNode.jjtGetChild(i).jjtAccept(this, data);
    		
    	}
    	
    	return new PathSequence(args);
		
	}
	
	@Override
	public Object visit(ASTPathElt pathEltNode, Object data)
			throws VisitorException {

		final BOp arg;
		if (pathEltNode.isNegatedPropertySet()) {
			
	    	final int numChildren = pathEltNode.jjtGetNumChildren();
	    	
	    	final PathOneInPropertySet[] args = new PathOneInPropertySet[numChildren];
	    	
	    	for (int i = 0; i < numChildren; i++) {
	    	
	    		args[i] = (PathOneInPropertySet) pathEltNode.jjtGetChild(i).jjtAccept(this, data);
	    		
	    	}
	    	
	    	arg = new PathNegatedPropertySet(args);
			
		} else {
			
			// handle the iri and nested path cases
			arg = (BOp) pathEltNode.jjtGetChild(0).jjtAccept(this, data);
			
		}
		
		final boolean inverse = pathEltNode.isInverse();
		
		final PathMod mod = toPathMod(pathEltNode.getPathMod());
		
    	return new PathElt(arg, inverse, mod);
		
	}
	
	private PathMod toPathMod(ASTPathMod pathModNode) {

		if (pathModNode == null)
			return null;
		
    	final long lower = pathModNode.getLowerBound();
    	final long upper = pathModNode.getUpperBound();
    	
    	if (upper == 1) {
    		return PathMod.ZERO_OR_ONE;
    	} else if (lower == 1) {
    		return PathMod.ONE_OR_MORE;
    	} else {
    		return PathMod.ZERO_OR_MORE;
    	}
		
	}

	@Override
	public Object visit(final ASTPathOneInPropertySet node, Object data)
			throws VisitorException {

		final ConstantNode arg = (ConstantNode) node.jjtGetChild(0).jjtAccept(this, data);
		
		final boolean inverse = node.isInverse();
		
		return new PathOneInPropertySet(arg, inverse);
		
	}
	
    @Override
    final public List<TermNode> visit(final ASTObjectList node, final Object data)
            throws VisitorException {
        
        final int childCount = node.jjtGetNumChildren();
        
        final List<TermNode> result = new ArrayList<TermNode>(childCount);

        for (int i = 0; i < childCount; i++) {
            
            result.add((TermNode) node.jjtGetChild(i).jjtAccept(this, null));
            
        }

        return result;
    }

    @Override
    final public VarNode visit(final ASTBlankNodePropertyList node, final Object data)
            throws VisitorException {
        
        final VarNode bnodeVar = context.createAnonVar(node.getVarName());
        
        super.visit(node, bnodeVar);
        
        return bnodeVar;
        
    }
    
    /**
     * Handle the RDF Collection syntax.
     */
    @Override
    public VarNode visit(final ASTCollection node, final Object data)
            throws VisitorException {

        final String listVarName = node.getVarName();
        
        final VarNode rootListVar = context.createAnonVar(listVarName);

        TermNode listVar = rootListVar;

        final int childCount = node.jjtGetNumChildren();
        
        for (int i = 0; i < childCount; i++) {

            final TermNode childValue = (TermNode) node.jjtGetChild(i)
                    .jjtAccept(this, null);

            graphPattern.addSP(listVar, context.createConstVar(RDF.FIRST),
                    childValue);

            TermNode nextListVar;
            if (i == childCount - 1) {
                nextListVar = context.createConstVar(RDF.NIL);
            } else {
                nextListVar = context.createAnonVar(listVarName + "-" + (i + 1));
            }

            graphPattern.addSP(listVar, context.createConstVar(RDF.REST),
                    nextListVar);

            listVar = nextListVar;

        }

        return rootListVar;
    }

    //
    // FIXME Property paths (Implement).
    //

//	@Override
//	public Object visit(ASTPathAlternative pathAltNode, Object data)
//		throws VisitorException
//	{
//
//		int altCount = pathAltNode.jjtGetNumChildren();
//
//		if (altCount > 1) {
//			GraphPattern parentGP = graphPattern;
//			Union union = new Union();
//			Union currentUnion = union;
//			for (int i = 0; i < altCount - 1; i++) {
//				graphPattern = new GraphPattern(parentGP);
//				pathAltNode.jjtGetChild(i).jjtAccept(this, data);
//				TupleExpr arg = graphPattern.buildTupleExpr();
//				currentUnion.setLeftArg(arg);
//				if (i == altCount - 2) { // second-to-last item
//					graphPattern = new GraphPattern(parentGP);
//					pathAltNode.jjtGetChild(i + 1).jjtAccept(this, data);
//					arg = graphPattern.buildTupleExpr();
//					currentUnion.setRightArg(arg);
//				}
//				else {
//					Union newUnion = new Union();
//					currentUnion.setRightArg(newUnion);
//					currentUnion = newUnion;
//				}
//			}
//
//			parentGP.addRequiredTE(union);
//			graphPattern = parentGP;
//		}
//		else {
//			pathAltNode.jjtGetChild(0).jjtAccept(this, data);
//		}
//
//		return null;
//	}
//
//	@Override
//	public PathOneInPropertySet visit(ASTPathOneInPropertySet node, Object data)
//		throws VisitorException
//	{
//
//		PropertySetElem result = new PropertySetElem();
//		result.setInverse(node.isInverse());
//		ValueConstant predicate = (ValueConstant)node.jjtGetChild(0).jjtAccept(this, data);
//		result.setPredicate(predicate);
//		
//		return result;
//	}
//
//	private ASTObjectList getObjectList(Node node) {
//		if (node == null) {
//			return null;
//		}
//		if (node instanceof ASTPropertyListPath) {
//			return ((ASTPropertyListPath)node).getObjectList();
//		}
//		else {
//			return getObjectList(node.jjtGetParent());
//		}
//	}
//
//	private boolean checkInverse(Node node) {
//		if (node instanceof ASTPathElt) {
//			return ((ASTPathElt)node).isInverse();
//		}
//		else {
//			Node parent = node.jjtGetParent();
//			if (parent != null) {
//				return checkInverse(parent);
//			}
//			else {
//				return false;
//			}
//		}
//	}
//
//	@Override
//	public Object visit(ASTPathSequence pathSeqNode, Object data)
//		throws VisitorException
//	{
//
//		ValueExpr subject = (ValueExpr)data;
//		Var subjVar = valueExpr2Var(subject);
//
//		// check if we should invert subject and object.
//		boolean invertSequence = checkInverse(pathSeqNode);
//
//		@SuppressWarnings("unchecked")
//		List<ValueExpr> objectList = (List<ValueExpr>)getObjectList(pathSeqNode).jjtAccept(this, null);
//
//		List<ASTPathElt> pathElements = pathSeqNode.getPathElements();
//
//		int pathLength = pathElements.size();
//
//		GraphPattern pathSequencePattern = new GraphPattern(graphPattern);
//
//		Scope scope = pathSequencePattern.getStatementPatternScope();
//		Var contextVar = pathSequencePattern.getContextVar();
//
//		Var startVar = subjVar;
//
//		for (int i = 0; i < pathLength; i++) {
//			ASTPathElt pathElement = pathElements.get(i);
//
//			ASTPathMod pathMod = pathElement.getPathMod();
//
//			long lowerBound = Long.MIN_VALUE;
//			long upperBound = Long.MIN_VALUE;
//
//			if (pathMod != null) {
//				lowerBound = pathMod.getLowerBound();
//				upperBound = pathMod.getUpperBound();
//
//				if (upperBound == Long.MIN_VALUE) {
//					upperBound = lowerBound;
//				}
//				else if (lowerBound == Long.MIN_VALUE) {
//					lowerBound = upperBound;
//				}
//			}
//
//			if (pathElement.isNegatedPropertySet()) {
//
//				// create a temporary negated property set object and set the
//				// correct subject and object vars to continue
//				// the path sequence.
//
//				NegatedPropertySet nps = new NegatedPropertySet();
//				nps.setScope(scope);
//				nps.setSubjectVar(startVar);
//				nps.setContextVar(contextVar);
//
//				for (Node child : pathElement.jjtGetChildren()) {
//					nps.addPropertySetElem((PropertySetElem)child.jjtAccept(this, data));
//				}
//
//				if (i == pathLength - 1) {
//					nps.setObjectList(objectList);
//				}
//				else {
//					// not last element in path.
//					Var nextVar = createAnonVar(startVar.getName() + "-property-set-" + i);
//
//					List<ValueExpr> nextVarList = new ArrayList<ValueExpr>();
//					nextVarList.add(nextVar);
//					nps.setObjectList(nextVarList);
//
//					startVar = nextVar;
//				}
//
//				// convert the NegatedPropertySet to a proper TupleExpr
//				pathSequencePattern.addRequiredTE(createTupleExprForNegatedPropertySet(nps, i));
//
//			}
//			else if (pathElement.isNestedPath()) {
//				GraphPattern parentGP = graphPattern;
//
//				graphPattern = new GraphPattern(parentGP);
//
//				if (i == pathLength - 1) {
//					// last element in the path
//					pathElement.jjtGetChild(0).jjtAccept(this, startVar);
//
//					TupleExpr te = graphPattern.buildTupleExpr();
//
//					for (ValueExpr object : objectList) {
//						Var objVar = valueExpr2Var(object);
//						te = handlePathModifiers(scope, startVar, te, objVar, contextVar, lowerBound, upperBound);
//						pathSequencePattern.addRequiredTE(te);
//					}
//				}
//				else {
//					// not the last element in the path, introduce an anonymous var
//					// to connect.
//					Var nextVar = createAnonVar(subjVar.getName() + "-nested-" + i);
//
//					pathElement.jjtGetChild(0).jjtAccept(this, startVar);
//
//					TupleExpr te = graphPattern.buildTupleExpr();
//
//					// replace all object list occurrences with the intermediate var.
//
//					te = replaceVarOccurrence(te, objectList, nextVar);
//					te = handlePathModifiers(scope, startVar, te, nextVar, contextVar, lowerBound, upperBound);
//					pathSequencePattern.addRequiredTE(te);
//
//					startVar = nextVar;
//				}
//
//				graphPattern = parentGP;
//			}
//			else {
//
//				ValueExpr pred = (ValueExpr)pathElement.jjtAccept(this, data);
//				Var predVar = valueExpr2Var(pred);
//
//				TupleExpr te;
//
//				if (i == pathLength - 1) {
//					// last element in the path, connect to list of defined objects
//					for (ValueExpr object : objectList) {
//						Var objVar = valueExpr2Var(object);
//						Var endVar = objVar;
//
//						if (invertSequence) {
//							endVar = subjVar;
//						}
//
//						if (pathElement.isInverse()) {
//							te = new StatementPattern(scope, endVar, predVar, startVar, contextVar);
//							te = handlePathModifiers(scope, endVar, te, startVar, contextVar, lowerBound, upperBound);
//						}
//						else {
//							te = new StatementPattern(scope, startVar, predVar, endVar, contextVar);
//							te = handlePathModifiers(scope, startVar, te, endVar, contextVar, lowerBound, upperBound);
//
//						}
//
//						pathSequencePattern.addRequiredTE(te);
//					}
//				}
//				else {
//					// not the last element in the path, introduce an anonymous var
//					// to connect.
//					Var nextVar = createAnonVar(predVar.getName() + "-" + i);
//
//					if (invertSequence && startVar.equals(subjVar)) { // first
//																						// element in
//																						// inverted
//																						// sequence
//						for (ValueExpr object : objectList) {
//							Var objVar = valueExpr2Var(object);
//							startVar = objVar;
//
//							if (pathElement.isInverse()) {
//								Var temp = startVar;
//								startVar = nextVar;
//								nextVar = temp;
//							}
//
//							te = new StatementPattern(scope, startVar, predVar, nextVar, contextVar);
//							te = handlePathModifiers(scope, startVar, te, nextVar, contextVar, lowerBound,
//									upperBound);
//
//							pathSequencePattern.addRequiredTE(te);
//						}
//					}
//					else {
//
//						if (pathElement.isInverse()) {
//							startVar = nextVar;
//							nextVar = subjVar;
//						}
//
//						te = new StatementPattern(scope, startVar, predVar, nextVar, contextVar);
//						te = handlePathModifiers(scope, startVar, te, nextVar, contextVar, lowerBound, upperBound);
//
//						pathSequencePattern.addRequiredTE(te);
//					}
//
//					// set the subject for the next element in the path.
//					startVar = (pathElement.isInverse() ? startVar : nextVar);
//				}
//			}
//		}
//
//		// add the created path sequence to the graph pattern.
//		for (TupleExpr te : pathSequencePattern.getRequiredTEs()) {
//			graphPattern.addRequiredTE(te);
//		}
//
//		return null;
//	}
//
//	private TupleExpr createTupleExprForNegatedPropertySet(NegatedPropertySet nps, int index) {
//		Var subjVar = nps.getSubjectVar();
//
//		Var predVar = createAnonVar("nps-" + subjVar.getName() + "-" + index);
//		// Var predVarInverse = createAnonVar("nps-inverse-" + subjVar.getName() +
//		// "-" + index);
//
//		ValueExpr filterCondition = null;
//		ValueExpr filterConditionInverse = null;
//
//		// build (inverted) filter conditions for each negated path element.
//		for (PropertySetElem elem : nps.getPropertySetElems()) {
//			ValueConstant predicate = elem.getPredicate();
//
//			if (elem.isInverse()) {
//				Compare compare = new Compare(predVar, predicate, CompareOp.NE);
//				if (filterConditionInverse == null) {
//					filterConditionInverse = compare;
//				}
//				else {
//					filterConditionInverse = new And(compare, filterConditionInverse);
//				}
//			}
//			else {
//				Compare compare = new Compare(predVar, predicate, CompareOp.NE);
//				if (filterCondition == null) {
//					filterCondition = compare;
//				}
//				else {
//					filterCondition = new And(compare, filterCondition);
//				}
//			}
//		}
//
//		TupleExpr patternMatch = null;
//
//		// build a regular statement pattern (or a join of several patterns if the
//		// object list has more than
//		// one item)
//		if (filterCondition != null) {
//			for (ValueExpr objVar : nps.getObjectList()) {
//				if (patternMatch == null) {
//					patternMatch = new StatementPattern(nps.getScope(), subjVar, predVar, (Var)objVar,
//							nps.getContextVar());
//				}
//				else {
//					patternMatch = new Join(new StatementPattern(nps.getScope(), subjVar, predVar, (Var)objVar,
//							nps.getContextVar()), patternMatch);
//				}
//			}
//		}
//
//		TupleExpr patternMatchInverse = null;
//
//		// build a inverse statement pattern (or a join of several patterns if the
//		// object list has more than
//		// one item):
//		if (filterConditionInverse != null) {
//			for (ValueExpr objVar : nps.getObjectList()) {
//				if (patternMatchInverse == null) {
//					patternMatchInverse = new StatementPattern(nps.getScope(), (Var)objVar, predVar, subjVar,
//							nps.getContextVar());
//				}
//				else {
//					patternMatchInverse = new Join(new StatementPattern(nps.getScope(), (Var)objVar, predVar,
//							subjVar, nps.getContextVar()), patternMatchInverse);
//				}
//			}
//		}
//
//		TupleExpr completeMatch = null;
//
//		if (patternMatch != null) {
//			completeMatch = new Filter(patternMatch, filterCondition);
//		}
//
//		if (patternMatchInverse != null) {
//			if (completeMatch == null) {
//				completeMatch = new Filter(patternMatchInverse, filterConditionInverse);
//			}
//			else {
//				completeMatch = new Union(new Filter(patternMatchInverse, filterConditionInverse), completeMatch);
//			}
//		}
//
//		return completeMatch;
//	}
//
//	private TupleExpr replaceVarOccurrence(TupleExpr te, List<ValueExpr> objectList, Var replacementVar)
//		throws VisitorException
//	{
//		for (ValueExpr objExpr : objectList) {
//			Var objVar = valueExpr2Var(objExpr);
//			VarReplacer replacer = new VarReplacer(objVar, replacementVar);
//			te.visit(replacer);
//		}
//		return te;
//	}
//
//	private TupleExpr handlePathModifiers(Scope scope, Var subjVar, TupleExpr te, Var endVar, Var contextVar,
//			long lowerBound, long upperBound)
//		throws VisitorException
//	{
//
//		TupleExpr result = te;
//
//		if (lowerBound >= 0L) {
//
//			if (lowerBound < upperBound) {
//
//				if (upperBound < Long.MAX_VALUE) {
//					// upperbound is fixed-length
//
//					// create set of unions for all path lengths between lower
//					// and upper bound.
//					Union union = new Union();
//					Union currentUnion = union;
//
//					for (long length = lowerBound; length < upperBound; length++) {
//
//						TupleExpr path = createPath(scope, subjVar, te, endVar, contextVar, length);
//
//						currentUnion.setLeftArg(path);
//						if (length == upperBound - 1) {
//							path = createPath(scope, subjVar, te, endVar, contextVar, length + 1);
//							currentUnion.setRightArg(path);
//						}
//						else {
//							Union nextUnion = new Union();
//							currentUnion.setRightArg(nextUnion);
//							currentUnion = nextUnion;
//						}
//					}
//
//					result = union;
//				}
//				else {
//					// upperbound is abitrary-length
//
//					result = new ArbitraryLengthPath(scope, subjVar, te, endVar, contextVar, lowerBound);
//				}
//			}
//			else {
//				// create single path of fixed length.
//				TupleExpr path = createPath(scope, subjVar, te, endVar, contextVar, lowerBound);
//				result = path;
//			}
//		}
//
//		return result;
//	}
//
//	private TupleExpr createPath(Scope scope, Var subjVar, TupleExpr pathExpression, Var endVar,
//			Var contextVar, long length)
//		throws VisitorException
//	{
//		if (pathExpression instanceof StatementPattern) {
//			Var predVar = ((StatementPattern)pathExpression).getPredicateVar();
//
//			if (length == 0L) {
//				return new ZeroLengthPath(scope, subjVar, endVar, contextVar);
//			}
//			else {
//				GraphPattern gp = new GraphPattern();
//				gp.setContextVar(contextVar);
//				gp.setStatementPatternScope(scope);
//
//				Var nextVar = null;
//
//				for (long i = 0L; i < length; i++) {
//					if (i < length - 1) {
//						nextVar = createAnonVar(predVar.getName() + "-path-" + length + "-" + i);
//					}
//					else {
//						nextVar = endVar;
//					}
//					gp.addRequiredSP(subjVar, predVar, nextVar);
//					subjVar = nextVar;
//				}
//				return gp.buildTupleExpr();
//			}
//		}
//		else {
//			if (length == 0L) {
//				return new ZeroLengthPath(scope, subjVar, endVar, contextVar);
//			}
//			else {
//				GraphPattern gp = new GraphPattern();
//				gp.setContextVar(contextVar);
//				gp.setStatementPatternScope(scope);
//
//				Var nextVar = null;
//				for (long i = 0L; i < length; i++) {
//					if (i < length - 1L) {
//						nextVar = createAnonVar(subjVar.getName() + "-expression-path-" + length + "-" + i);
//					}
//					else {
//						nextVar = endVar;
//					}
//
//					// create a clone of the path expression.
//					TupleExpr clone = pathExpression.clone();
//
//					VarReplacer replacer = new VarReplacer(endVar, nextVar);
//					clone.visit(replacer);
//
//					gp.addRequiredTE(clone);
//
//					subjVar = nextVar;
//				}
//				return gp.buildTupleExpr();
//			}
//		}
//	}
//
//	 protected class VarCollector extends QueryModelVisitorBase<VisitorException> {
//
//		private final Set<Var> collectedVars = new HashSet<Var>();
//
//		@Override
//		public void meet(Var var) {
//			collectedVars.add(var);
//		}
//
//		/**
//		 * @return Returns the collectedVars.
//		 */
//		public Set<Var> getCollectedVars() {
//			return collectedVars;
//		}
//
//	}
//
//	private class VarReplacer extends QueryModelVisitorBase<VisitorException> {
//
//		private Var toBeReplaced;
//
//		private Var replacement;
//
//		public VarReplacer(Var toBeReplaced, Var replacement) {
//			this.toBeReplaced = toBeReplaced;
//			this.replacement = replacement;
//		}
//
//		@Override
//		public void meet(Var var) {
//			if (toBeReplaced.equals(var)) {
//				QueryModelNode parent = var.getParentNode();
//				parent.replaceChildNode(var, replacement);
//				replacement.setParentNode(parent);
//			}
//		}
//	}
//
//	private class SubSelectAlphaConvertor extends QueryModelVisitorBase<VisitorException> {
//
//		private Var toBeReplaced;
//
//		private Var replacement;
//
//		private Projection projection;
//
//		public SubSelectAlphaConvertor(Projection projection, Var toBeReplaced, Var replacement) {
//			this.projection = projection;
//			this.toBeReplaced = toBeReplaced;
//			this.replacement = replacement;
//		}
//
//		public void meet(Projection projection)
//			throws VisitorException
//		{
//			if (projection.getBindingNames().contains(toBeReplaced.getName())) {
//				super.meet(projection);
//			}
//		}
//
//		public void meet(ProjectionElem elem)
//			throws VisitorException
//		{
//			if (elem.getSourceName().equals(toBeReplaced.getName())) {
//				elem.setSourceName(replacement.getName());
//				elem.setTargetName(replacement.getName());
//			}
//			super.meet(elem);
//		}
//
//		public void meet(ExtensionElem elem)
//			throws VisitorException
//		{
//			if (elem.getName().equals(toBeReplaced.getName())) {
//				elem.setName(replacement.getName());
//			}
//			super.meet(elem);
//		}
//
//		@Override
//		public void meet(Group group)
//			throws VisitorException
//		{
//			super.meet(group);
//
//			String name = toBeReplaced.getName();
//
//			Set<String> groupBindings = group.getGroupBindingNames();
//			Set<String> newGroupBindings = new HashSet<String>(groupBindings);
//
//			if (groupBindings.contains(name)) {
//				newGroupBindings.remove(name);
//				newGroupBindings.add(replacement.getName());
//
//				group.setGroupBindingNames(newGroupBindings);
//			}
//		}
//
//		@Override
//		public void meet(StatementPattern sp)
//			throws VisitorException
//		{
//			if (toBeReplaced.equals(sp.getContextVar())) {
//				if (projection.getProjectionContext() != null
//						&& projection.getProjectionContext().equals(sp.getContextVar()))
//				{
//					Var newSpContextVar = createAnonVar(sp.getContextVar().getName()
//							+ UUID.randomUUID().toString());
//					sp.setContextVar(newSpContextVar);
//				}
//			}
//			super.meet(sp);
//		}
//
//		@Override
//		public void meet(Var var) {
//			if (toBeReplaced.equals(var)) {
//				QueryModelNode parent = var.getParentNode();
//
//				parent.replaceChildNode(var, replacement);
//				replacement.setParentNode(parent);
//			}
//		}
//	}

    /*
     * Reification done right.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/526 (Reification
     * done right)
     */
    
    /**
     * This is invoked in two different contexts. One for a bare triple
     * reference pattern:
     * 
     * <pre>
     * <<a b c>> d e
     * </pre>
     * 
     * and the other for a BIND() of a triple reference pattern onto a statement
     * identifier variable:
     * 
     * <pre>
     * BIND(<<a b c>> as ?sidVar)
     * </pre>
     * 
     * In both cases we translate the triple reference pattern into a new
     * {@link StatementPatternNode}. We reach back to the parent to decide which
     * of the two invocation contexts applies and either generate a new SID
     * variable or use the one from the BIND(). Then we set the SID variable on
     * the {@link StatementPatternNode} and add it to the current graph pattern
     * group.
     * 
     * <p>
     * If either the subject or object position in the triple reference pattern
     * is a triple reference pattern, then it will have been turned into a
     * {@link StatementPatternNode} by recursion through this method and we
     * replace it with the SID variable which was assigned to that
     * {@link StatementPatternNode}.
     * 
     * @return The SID variable.  This allows the existing code paths to
     * handle nested triple reference patterns without change.
     */
    public VarNode visit(final ASTTRefPattern node,
            final Object data) throws VisitorException {
        
        /*
         * Accept and convert.
         */

        final TermNode s = (TermNode) node.jjtGetChild(0).jjtAccept(this, data);
        
        final TermNode p = (TermNode) node.jjtGetChild(1).jjtAccept(this, data);
        
        final TermNode o = (TermNode) node.jjtGetChild(2).jjtAccept(this, data);
        
        /*
         * Check constraints.
         */

        if(s instanceof ConstantNode) {

            final BigdataValue v = ((ConstantNode)s).getValue();
            
            if (v instanceof Literal) {

                throw new VisitorException(
                        "Subject in triple reference pattern may not be literal.");
            
            }
            
        } else {
            if (((VarNode) s).isAnonymous()) {
                /*
                 * Blank nodes have already been translated into variables.
                 * 
                 * TODO We could run into trouble here if there anonymous
                 * variables are introduced for anything other than a blank node
                 * (and there are). We need to explicitly mark anonymous
                 * variables which correspond to blank nodes in the original
                 * query and then test for isBlankNode() here.
                 */
                throw new VisitorException(
                        "Subject in triple reference pattern may not be blank node.");
            }
        }

        if (p instanceof ConstantNode) {

            final BigdataValue v = ((ConstantNode) p).getValue();

            if (!(v instanceof URI)) {

                throw new VisitorException(
                        "Predicate in triple reference pattern must be IRI.");

            }

        }

        if (o instanceof ConstantNode) {

            final BigdataValue v = ((ConstantNode) o).getValue();

            if (v instanceof BNode) {

                throw new VisitorException(
                        "Object in triple reference pattern may not be blank node.");

            }

        } else {
            
            if (((VarNode) o).isAnonymous()) {
                /*
                 * Blank nodes have already been translated into variables.
                 */
                throw new VisitorException(
                        "Object in triple reference pattern may not be blank node.");
            }
            
        }
        
        /*
         * Note: The caller will associate the SID variable with this
         * StatementPatternNode if it appears in a BIND(<<...>> AS ?sid)
         * construction. That will overwrite the variable which we assign here.
         * 
         * @see GroupGraphPatternBuilder#visit(ASTBind,Object)
         */

        final StatementPatternNode sp = new StatementPatternNode(s, p, o,
                graphPattern.getContext(),
                graphPattern.getStatementPatternScope());

        final Node parent = node.jjtGetParent();
        
        final VarNode sidVar;
        
        if(parent instanceof ASTBind) {
            
            final ASTBind bind = (ASTBind) parent;

            final Node aliasNode = bind.jjtGetChild(1);

            final String alias = ((ASTVar) aliasNode).getName();

            // Use the specified variable.
            sidVar = new VarNode(alias);
            
//          sidVar.setSID(true);
            
        } else {

            // Create a new variable.
            sidVar = context.createSidVar();

        }

        // Set the SID variable on the SP.
        sp.setSid(sidVar);
        
        // add to the current join group.
        graphPattern.addSP(sp);

//      log.error("node:=" + node + " => " + sp);

        // Return the SID variable.
        return sidVar;
        
    }
 
}
