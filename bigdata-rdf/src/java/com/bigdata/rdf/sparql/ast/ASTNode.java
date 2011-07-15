package com.bigdata.rdf.sparql.ast;

public class ASTNode {

	/*

QueryRoot extends GroupNodeBase implements IGroupNode
-Dataset
-Distinct
-Order by expressions
-Limit / offset

Tuple Expr: Join Group, Union, Statement Pattern, Constraint
TupleExpr = IBindingsProducerNode?
ValueExpr = IValueExpressionNode?

Only an IGroupNode can have children (Union, Join Group), 
and those children will be IBindingsProducerNodes 
(other groups, statements patterns, constraints)

IGroupNode:
-children: IBindingsProducerNode[]
-group ID?

IBindingsProducerNode:
-parent: IGroupNode

abstractBindingsProducerNode
-setParent(IGroupNode)

abstract GroupNodeBase Iterable<IBindingsProducerNode>
-addChild(IBindingsProducerNode)
-removeChild(IBindingsProducerNode)
-iterator()

Join Group extends ASTNode implements IBindingsProducerNode
-Optional
-Statement patterns
-Constraints
-Sub groups

Union extends ASTNode implements IBindingsProducerNode
-Sub queries can be groups or statement patterns, but not constraints

Statement Pattern extends ASTNode implements IBindingsProducerNode
-optional: boolean
-scope: Sesame Scope object
-S, P, O, C: TermNode

Constraint extends ASTNode implements IBindingsProducerNode
-argument: IValueExpressionNode

Value Expr
-Unary: IsBNode, IsLiteral, IsResource, IsURI, Datatype, Label, Lang, Str, Not
-Binary: And, Or, Compare, SameTerm, LangMatches, Math, Regex
-Term: Var, Constant

	 */
	
}
