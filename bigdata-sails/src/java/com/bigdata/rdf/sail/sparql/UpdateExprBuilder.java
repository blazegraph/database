/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 10, 2012
 */
/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.sparql;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOpUtility;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.sail.sparql.ast.ASTAdd;
import com.bigdata.rdf.sail.sparql.ast.ASTClear;
import com.bigdata.rdf.sail.sparql.ast.ASTCopy;
import com.bigdata.rdf.sail.sparql.ast.ASTCreate;
import com.bigdata.rdf.sail.sparql.ast.ASTDrop;
import com.bigdata.rdf.sail.sparql.ast.ASTGraphOrDefault;
import com.bigdata.rdf.sail.sparql.ast.ASTGraphRefAll;
import com.bigdata.rdf.sail.sparql.ast.ASTInsertData;
import com.bigdata.rdf.sail.sparql.ast.ASTLoad;
import com.bigdata.rdf.sail.sparql.ast.ASTMove;
import com.bigdata.rdf.sail.sparql.ast.ASTQuadsNotTriples;
import com.bigdata.rdf.sail.sparql.ast.Node;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.ast.AddGraph;
import com.bigdata.rdf.sparql.ast.ClearGraph;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.CopyGraph;
import com.bigdata.rdf.sparql.ast.CreateGraph;
import com.bigdata.rdf.sparql.ast.DropGraph;
import com.bigdata.rdf.sparql.ast.InsertData;
import com.bigdata.rdf.sparql.ast.LoadGraph;
import com.bigdata.rdf.sparql.ast.MoveGraph;
import com.bigdata.rdf.sparql.ast.QuadsData;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.spo.ISPO;

/**
 * Extension of TupleExprBuilder that builds Update Expressions.
 * 
 * @author Jeen Broekstra
 * @author Bryan Thompson
 */
public class UpdateExprBuilder extends BigdataExprBuilder {

//    private static final Logger log = Logger.getLogger(UpdateExprBuilder.class);

//  private Set<Var> getProjectionVars(Collection<StatementPattern> statementPatterns) {
//  Set<Var> vars = new LinkedHashSet<Var>(statementPatterns.size() * 2);
//
//  for (StatementPattern sp : statementPatterns) {
//      vars.add(sp.getSubjectVar());
//      vars.add(sp.getPredicateVar());
//      vars.add(sp.getObjectVar());
//      if (sp.getContextVar() != null) {
//          vars.add(sp.getContextVar());
//      }
//  }
//
//  return vars;
//}
    
    public UpdateExprBuilder(final BigdataASTContext context) {
        
        super(context);
    }

//	@Override
//	public UpdateExpr visit(ASTUpdate node, Object data)
//		throws VisitorException
//	{
//		if (node instanceof ASTModify) {
//			return this.visit((ASTModify)node, data);
//		}
//		else if (node instanceof ASTInsertData) {
//			return this.visit((ASTInsertData)node, data);
//		}
//
//		return null;
//	}

    /**
     * Note: Variables in QuadDatas are disallowed in INSERT DATA requests (see
     * Notes 8 in the grammar). That is, the INSERT DATA statement only allows
     * to insert ground triples. Blank nodes in QuadDatas are assumed to be
     * disjoint from the blank nodes in the Graph Store, i.e., will be inserted
     * with "fresh" blank nodes.
     */
	@Override
    public InsertData visit(final ASTInsertData node, final Object data)
            throws VisitorException
	{

	    final InsertData op = new InsertData();
	    
        /*
         * Collect QuadsData.
         */
		final GroupGraphPattern parentGP = graphPattern;
		graphPattern = new GroupGraphPattern();

		// inherit scope & context : TODO When would it ever be non-null?
		graphPattern.setStatementPatternScope(parentGP.getStatementPatternScope());
		graphPattern.setContextVar(parentGP.getContext());

		// visit [QuadsData].
		/* TODO This seems wrong to me. If we have QUADS then TRIPLEs, won't
		 this attach the GRAPH context to the TRIPLES also? I've put together several tests which examine this. */
		{

		    // first child.
            final Node child0 = node.jjtGetChild(0);
            
            final Object algebraExpr = child0.jjtAccept(this, data);

            if (algebraExpr instanceof ConstantNode) { // named graph identifier

                final ConstantNode context = (ConstantNode) algebraExpr;
                graphPattern.setContextVar(context);
                graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);
                
            }

            // remaining children (i=1...).
            for (int i = 1; i < node.jjtGetNumChildren(); i++) {

                node.jjtGetChild(i).jjtAccept(this, data);

            }

		}
		
		final QuadsData insertExpr = graphPattern.buildGroup(new QuadsData());

		graphPattern = parentGP;

		final List<BigdataStatement> stmts = new LinkedList<BigdataStatement>(); 
        final Iterator<StatementPatternNode> itr = BOpUtility.visitAll(
                insertExpr, StatementPatternNode.class);
        while (itr.hasNext()) {
            final StatementPatternNode sp = itr.next();
            final BigdataStatement spo = context.valueFactory.createStatement(
                    (Resource) sp.s().getValue(), (URI) sp.p().getValue(),
                    (Value) sp.o().getValue(), (Resource) (sp.c() != null ? sp
                            .c().getValue() : null), StatementEnum.Explicit);
            stmts.add(spo);
        }
        final ISPO[] a = stmts.toArray(new ISPO[stmts.size()]);
        op.setData(a);
//      TupleExpr insertExpr = graphPattern.buildTupleExpr();
//		// Retrieve all StatementPatterns from the insert expression
//		List<StatementPattern> statementPatterns = StatementPatternCollector.process(insertExpr);
//
//		Set<Var> projectionVars = getProjectionVars(statementPatterns);
//
//		// Create BNodeGenerators for all anonymous variables
//		Map<Var, ExtensionElem> extElemMap = new HashMap<Var, ExtensionElem>();
//
//		for (Var var : projectionVars) {
//			if (var.isAnonymous() && !extElemMap.containsKey(var)) {
//				ValueExpr valueExpr;
//
//				if (var.hasValue()) {
//					valueExpr = new ValueConstant(var.getValue());
//				}
//				else {
//					valueExpr = new BNodeGenerator();
//				}
//
//				extElemMap.put(var, new ExtensionElem(valueExpr, var.getName()));
//			}
//		}
//
//      TupleExpr result = new SingletonSet();
//
//		if (!extElemMap.isEmpty()) {
//			result = new Extension(result, extElemMap.values());
//		}
//
//		// Create a Projection for each StatementPattern in the clause
//		List<ProjectionElemList> projList = new ArrayList<ProjectionElemList>();
//
//		for (StatementPattern sp : statementPatterns) {
//			ProjectionElemList projElemList = new ProjectionElemList();
//
//			projElemList.addElement(new ProjectionElem(sp.getSubjectVar().getName(), "subject"));
//			projElemList.addElement(new ProjectionElem(sp.getPredicateVar().getName(), "predicate"));
//			projElemList.addElement(new ProjectionElem(sp.getObjectVar().getName(), "object"));
//
//			if (sp.getContextVar() != null) {
//				projElemList.addElement(new ProjectionElem(sp.getContextVar().getName(), "context"));
//			}
//
//			projList.add(projElemList);
//		}
//
//		if (projList.size() == 1) {
//			result = new Projection(result, projList.get(0));
//		}
//		else if (projList.size() > 1) {
//			result = new MultiProjection(result, projList);
//		}
//		else {
//			// Empty constructor
//			result = new EmptySet();
//		}
//
//		result = new Reduced(result);

//		return new InsertData(result);
		return op;
	}

    /**
     * Note: For DELETE DATA, QuadData denotes triples to be removed and is as
     * described in INSERT DATA, with the difference that in a DELETE DATA
     * operation neither variables nor blank nodes are allowed (see Notes 8+9 in
     * the grammar).
     */
//	@Override
//	public DeleteData visit(ASTDeleteData node, Object data)
//		throws VisitorException
//	{
//
//		TupleExpr result = new SingletonSet();
//
//		// Collect construct triples
//		GraphPattern parentGP = graphPattern;
//		graphPattern = new GraphPattern();
//
//		// inherit scope & context
//		graphPattern.setStatementPatternScope(parentGP.getStatementPatternScope());
//		graphPattern.setContextVar(parentGP.getContextVar());
//
//		Object algebraExpr = node.jjtGetChild(0).jjtAccept(this, data);
//
//		if (algebraExpr instanceof ValueExpr) { // named graph identifier
//			Var contextVar = valueExpr2Var((ValueExpr)algebraExpr);
//			graphPattern.setContextVar(contextVar);
//			graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);
//		}
//
//		for (int i = 1; i < node.jjtGetNumChildren(); i++) {
//			node.jjtGetChild(i).jjtAccept(this, data);
//		}
//
//		TupleExpr deleteExpr = graphPattern.buildTupleExpr();
//
//		graphPattern = parentGP;
//
//		// Retrieve all StatementPatterns from the insert expression
//		List<StatementPattern> statementPatterns = StatementPatternCollector.process(deleteExpr);
//
//		Set<Var> projectionVars = getProjectionVars(statementPatterns);
//
//		// Create BNodeGenerators for all anonymous variables
//		Map<Var, ExtensionElem> extElemMap = new HashMap<Var, ExtensionElem>();
//
//		for (Var var : projectionVars) {
//			if (var.isAnonymous() && !extElemMap.containsKey(var)) {
//				ValueExpr valueExpr;
//
//				if (var.hasValue()) {
//					valueExpr = new ValueConstant(var.getValue());
//				}
//				else {
//					valueExpr = new BNodeGenerator();
//				}
//
//				extElemMap.put(var, new ExtensionElem(valueExpr, var.getName()));
//			}
//		}
//
//		if (!extElemMap.isEmpty()) {
//			result = new Extension(result, extElemMap.values());
//		}
//
//		// Create a Projection for each StatementPattern in the clause
//		List<ProjectionElemList> projList = new ArrayList<ProjectionElemList>();
//
//		for (StatementPattern sp : statementPatterns) {
//			ProjectionElemList projElemList = new ProjectionElemList();
//
//			projElemList.addElement(new ProjectionElem(sp.getSubjectVar().getName(), "subject"));
//			projElemList.addElement(new ProjectionElem(sp.getPredicateVar().getName(), "predicate"));
//			projElemList.addElement(new ProjectionElem(sp.getObjectVar().getName(), "object"));
//
//			if (sp.getContextVar() != null) {
//				projElemList.addElement(new ProjectionElem(sp.getContextVar().getName(), "context"));
//			}
//
//			projList.add(projElemList);
//		}
//
//		if (projList.size() == 1) {
//			result = new Projection(result, projList.get(0));
//		}
//		else if (projList.size() > 1) {
//			result = new MultiProjection(result, projList);
//		}
//		else {
//			// Empty constructor
//			result = new EmptySet();
//		}
//
//		result = new Reduced(result);
//
//		return new DeleteData(result);
//	}

//    @Override
//    public TupleExpr visit(ASTQuadsNotTriples node, Object data)
//        throws VisitorException
//    {
//        GraphPattern parentGP = graphPattern;
//        graphPattern = new GraphPattern();
//
//        ValueExpr contextNode = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, data);
//
//        Var contextVar = valueExpr2Var(contextNode);
//        graphPattern.setContextVar(contextVar);
//        graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);
//
//        for (int i = 1; i < node.jjtGetNumChildren(); i++) {
//            node.jjtGetChild(i).jjtAccept(this, data);
//        }
//
//        TupleExpr result = graphPattern.buildTupleExpr();
//        parentGP.addRequiredTE(result);
//
//        graphPattern = parentGP;
//
//        return result;
//    }
    
	@Override
	public QuadsData visit(final ASTQuadsNotTriples node, Object data)
		throws VisitorException
	{
		
	    final GroupGraphPattern parentGP = graphPattern;
		
	    graphPattern = new GroupGraphPattern();

        final ConstantNode contextNode = (ConstantNode) node.jjtGetChild(0)
                .jjtAccept(this, data);

		graphPattern.setContextVar(contextNode);

		graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);

        for (int i = 1; i < node.jjtGetNumChildren(); i++) {

            node.jjtGetChild(i).jjtAccept(this, data);

        }

        final QuadsData group = graphPattern.buildGroup(new QuadsData());

        parentGP.add(group);
        
		graphPattern = parentGP;

		return group;
	}

//	@Override
//	public Modify visit(ASTDeleteWhere node, Object data)
//		throws VisitorException
//	{
//		// Collect construct triples
//		GraphPattern parentGP = graphPattern;
//		graphPattern = new GraphPattern();
//
//		// inherit scope & context
//		graphPattern.setStatementPatternScope(parentGP.getStatementPatternScope());
//		graphPattern.setContextVar(parentGP.getContextVar());
//
//		for (int i = 0; i < node.jjtGetNumChildren(); i++) {
//			node.jjtGetChild(i).jjtAccept(this, data);
//		}
//
//		TupleExpr whereExpr = graphPattern.buildTupleExpr();
//		graphPattern = parentGP;
//
//		TupleExpr deleteExpr = whereExpr.clone();
//		Modify modify = new Modify(deleteExpr, null, whereExpr);
//
//		return modify;
//	}

	@Override
	public LoadGraph visit(final ASTLoad node, Object data)
		throws VisitorException
	{

        final ConstantNode sourceGraph = (ConstantNode) node.jjtGetChild(0)
                .jjtAccept(this, data);

		final LoadGraph op = new LoadGraph();
		
		op.setSourceGraph(sourceGraph);

        if (node.isSilent())
            op.setSilent(true);
		
		if (node.jjtGetNumChildren() > 1) {

		    final ConstantNode targetGraph = (ConstantNode) node.jjtGetChild(1)
                    .jjtAccept(this, data);
		    
			op.setTargetGraph(targetGraph);
			
		}

		return op;
	}

    /**
     * Note: DROP and CLEAR have the identical semantics for bigdata since it
     * does not support empty graphs.
     */
	@Override
	public ClearGraph visit(final ASTClear node, Object data)
		throws VisitorException
	{
	
	    final ClearGraph op = new ClearGraph();
		
		if(node.isSilent())
		    op.setSilent(true);

        final ASTGraphRefAll graphRef = node.jjtGetChild(ASTGraphRefAll.class);

		if (graphRef.jjtGetNumChildren() > 0) {

		    final ConstantNode targetGraph = (ConstantNode) graphRef
                    .jjtGetChild(0).jjtAccept(this, data);
			
		    op.setTargetGraph(targetGraph);

        } else {
            
            if (graphRef.isDefault()) {
            
                op.setScope(Scope.DEFAULT_CONTEXTS);
                
            } else if (graphRef.isNamed()) {
                
                op.setScope(Scope.NAMED_CONTEXTS);
                
            }

        }
		
		return op;
		
	}

    /**
     * Note: DROP and CLEAR have the identical semantics for bigdata since it
     * does not support empty graphs.
     */
    @Override
    public DropGraph visit(final ASTDrop node, final Object data)
            throws VisitorException {

        final DropGraph op = new DropGraph();
        
        if(node.isSilent())
            op.setSilent(true);

        final ASTGraphRefAll graphRef = node.jjtGetChild(ASTGraphRefAll.class);

        if (graphRef.jjtGetNumChildren() > 0) {

            final ConstantNode targetGraph = (ConstantNode) graphRef
                    .jjtGetChild(0).jjtAccept(this, data);
            
            op.setTargetGraph(targetGraph);

        } else {
            
            if (graphRef.isDefault()) {
            
                op.setScope(Scope.DEFAULT_CONTEXTS);
                
            } else if (graphRef.isNamed()) {
                
                op.setScope(Scope.NAMED_CONTEXTS);
                
            }

        }
        
        return op;
        
    }

    @Override
    public CreateGraph visit(final ASTCreate node, Object data)
            throws VisitorException {

        final ConstantNode targetGraph = (ConstantNode) node.jjtGetChild(0)
                .jjtAccept(this, data);

        final CreateGraph op = new CreateGraph();
        
        op.setTargetGraph(targetGraph);
        
        if (node.isSilent())
            op.setSilent(true);
        
        return op;
        
    }

    @Override
    public CopyGraph visit(final ASTCopy node, final Object data)
            throws VisitorException {

        final CopyGraph op = new CopyGraph();

        if (node.isSilent())
            op.setSilent(true);

        final ASTGraphOrDefault sourceNode = (ASTGraphOrDefault) node
                .jjtGetChild(0);

        if (sourceNode.jjtGetNumChildren() > 0) {

            final ConstantNode sourceGraph = (ConstantNode) sourceNode
                    .jjtGetChild(0).jjtAccept(this, data);

            op.setSourceGraph(sourceGraph);

        }

        final ASTGraphOrDefault destinationNode = (ASTGraphOrDefault) node
                .jjtGetChild(1);

        if (destinationNode.jjtGetNumChildren() > 0) {

            final ConstantNode targetGraph = (ConstantNode) destinationNode
                    .jjtGetChild(0).jjtAccept(this, data);

            op.setTargetGraph(targetGraph);

        }

        return op;

    }

    @Override
    public MoveGraph visit(final ASTMove node, final Object data)
            throws VisitorException {

        final MoveGraph op = new MoveGraph();
        
        if (node.isSilent())
            op.setSilent(true);

        final ASTGraphOrDefault sourceNode = (ASTGraphOrDefault) node
                .jjtGetChild(0);
        
        if (sourceNode.jjtGetNumChildren() > 0) {
        
            final ConstantNode sourceGraph = (ConstantNode) sourceNode
                    .jjtGetChild(0).jjtAccept(this, data);
            
            op.setSourceGraph(sourceGraph);
        
        }

        final ASTGraphOrDefault targetNode = (ASTGraphOrDefault) node
                .jjtGetChild(1);
        
        if (targetNode.jjtGetNumChildren() > 0) {
        
            final ConstantNode targetGraph = (ConstantNode) targetNode
                    .jjtGetChild(0).jjtAccept(this, data);
            
            op.setTargetGraph(targetGraph);
        }
        
        return op;

    }

    @Override
    public AddGraph visit(final ASTAdd node, final Object data)
            throws VisitorException {

        final AddGraph op = new AddGraph();

        if (node.isSilent())
            op.setSilent(true);

        final ASTGraphOrDefault sourceNode = (ASTGraphOrDefault) node
                .jjtGetChild(0);

        if (sourceNode.jjtGetNumChildren() > 0) {

            final ConstantNode sourceGraph = (ConstantNode) sourceNode
                    .jjtGetChild(0).jjtAccept(this, data);

            op.setSourceGraph(sourceGraph);

        }

        final ASTGraphOrDefault targetNode = (ASTGraphOrDefault) node
                .jjtGetChild(1);

        if (targetNode.jjtGetNumChildren() > 0) {

            final ConstantNode targetGraph = (ConstantNode) targetNode
                    .jjtGetChild(0).jjtAccept(this, data);

            op.setTargetGraph(targetGraph);

        }

        return op;

    }

    /*
     * DELETE/INSERT
     */
    
//	@Override
//	public Modify visit(ASTModify node, Object data)
//		throws VisitorException
//	{
//
//		ValueConstant with = null;
//		ASTIRI withNode = node.getWithClause();
//		if (withNode != null) {
//			with = (ValueConstant)withNode.jjtAccept(this, data);
//		}
//
//		if (with != null) {
//			graphPattern.setContextVar(valueExpr2Var(with));
//			graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);
//		}
//
//		ASTGraphPatternGroup whereClause = node.getWhereClause();
//
//		TupleExpr where = null;
//		if (whereClause != null) {
//			where = (TupleExpr)whereClause.jjtAccept(this, data);
//		}
//
//		TupleExpr delete = null;
//		ASTDeleteClause deleteNode = node.getDeleteClause();
//		if (deleteNode != null) {
//			delete = (TupleExpr)deleteNode.jjtAccept(this, data);
//		}
//
//		TupleExpr insert = null;
//		ASTInsertClause insertNode = node.getInsertClause();
//		if (insertNode != null) {
//			insert = (TupleExpr)insertNode.jjtAccept(this, data);
//		}
//
//		Modify modifyExpr = new Modify(delete, insert, where);
//
//		return modifyExpr;
//	}
//
//	@Override
//	public TupleExpr visit(ASTDeleteClause node, Object data)
//		throws VisitorException
//	{
//		TupleExpr result = (TupleExpr)data;
//
//		// Collect construct triples
//		GraphPattern parentGP = graphPattern;
//
//		graphPattern = new GraphPattern();
//
//		// inherit scope & context
//		graphPattern.setStatementPatternScope(parentGP.getStatementPatternScope());
//		graphPattern.setContextVar(parentGP.getContextVar());
//
//		for (int i = 0; i < node.jjtGetNumChildren(); i++) {
//			node.jjtGetChild(i).jjtAccept(this, data);
//		}
//		TupleExpr deleteExpr = graphPattern.buildTupleExpr();
//
//		graphPattern = parentGP;
//
//		return deleteExpr;
//
//	}
//
//	@Override
//	public TupleExpr visit(ASTInsertClause node, Object data)
//		throws VisitorException
//	{
//		TupleExpr result = (TupleExpr)data;
//
//		// Collect construct triples
//		GraphPattern parentGP = graphPattern;
//		graphPattern = new GraphPattern();
//
//		// inherit scope & context
//		graphPattern.setStatementPatternScope(parentGP.getStatementPatternScope());
//		graphPattern.setContextVar(parentGP.getContextVar());
//
//		for (int i = 0; i < node.jjtGetNumChildren(); i++) {
//			node.jjtGetChild(i).jjtAccept(this, data);
//		}
//
//		TupleExpr insertExpr = graphPattern.buildTupleExpr();
//
//		graphPattern = parentGP;
//
//		return insertExpr;
//
//		/*
//		// Retrieve all StatementPatterns from the insert expression
//		List<StatementPattern> statementPatterns = StatementPatternCollector.process(insertExpr);
//
//		Set<Var> projectionVars = getProjectionVars(statementPatterns);
//
//		// Create BNodeGenerators for all anonymous variables
//		Map<Var, ExtensionElem> extElemMap = new HashMap<Var, ExtensionElem>();
//
//		for (Var var : projectionVars) {
//			if (var.isAnonymous() && !extElemMap.containsKey(var)) {
//				ValueExpr valueExpr;
//
//				if (var.hasValue()) {
//					valueExpr = new ValueConstant(var.getValue());
//				}
//				else {
//					valueExpr = new BNodeGenerator();
//				}
//
//				extElemMap.put(var, new ExtensionElem(valueExpr, var.getName()));
//			}
//		}
//
//		if (!extElemMap.isEmpty()) {
//			result = new Extension(result, extElemMap.values());
//		}
//
//		// Create a Projection for each StatementPattern in the clause
//		List<ProjectionElemList> projList = new ArrayList<ProjectionElemList>();
//
//		for (StatementPattern sp : statementPatterns) {
//			ProjectionElemList projElemList = new ProjectionElemList();
//
//			projElemList.addElement(new ProjectionElem(sp.getSubjectVar().getName(), "subject"));
//			projElemList.addElement(new ProjectionElem(sp.getPredicateVar().getName(), "predicate"));
//			projElemList.addElement(new ProjectionElem(sp.getObjectVar().getName(), "object"));
//
//			if (sp.getContextVar() != null) {
//				projElemList.addElement(new ProjectionElem(sp.getContextVar().getName(), "context"));
//			}
//
//			projList.add(projElemList);
//		}
//
//		if (projList.size() == 1) {
//			result = new Projection(result, projList.get(0));
//		}
//		else if (projList.size() > 1) {
//			result = new MultiProjection(result, projList);
//		}
//		else {
//			// Empty constructor
//			result = new EmptySet();
//		}
//
//		*/
//	}

}