/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.model.vocabulary.FN;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.AggregateOperator;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.ArbitraryLengthPath;
import org.openrdf.query.algebra.Avg;
import org.openrdf.query.algebra.BNodeGenerator;
import org.openrdf.query.algebra.Bound;
import org.openrdf.query.algebra.Coalesce;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.Count;
import org.openrdf.query.algebra.Datatype;
import org.openrdf.query.algebra.Difference;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.EmptySet;
import org.openrdf.query.algebra.Exists;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.Group;
import org.openrdf.query.algebra.GroupConcat;
import org.openrdf.query.algebra.GroupElem;
import org.openrdf.query.algebra.IRIFunction;
import org.openrdf.query.algebra.If;
import org.openrdf.query.algebra.IsBNode;
import org.openrdf.query.algebra.IsLiteral;
import org.openrdf.query.algebra.IsNumeric;
import org.openrdf.query.algebra.IsURI;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Lang;
import org.openrdf.query.algebra.LangMatches;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.MathExpr;
import org.openrdf.query.algebra.Max;
import org.openrdf.query.algebra.Min;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.algebra.Or;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.OrderElem;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.Reduced;
import org.openrdf.query.algebra.Regex;
import org.openrdf.query.algebra.SameTerm;
import org.openrdf.query.algebra.Sample;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.algebra.Str;
import org.openrdf.query.algebra.Sum;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.ZeroLengthPath;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.sparql.ast.ASTAbs;
import org.openrdf.query.parser.sparql.ast.ASTAnd;
import org.openrdf.query.parser.sparql.ast.ASTAskQuery;
import org.openrdf.query.parser.sparql.ast.ASTAvg;
import org.openrdf.query.parser.sparql.ast.ASTBNodeFunc;
import org.openrdf.query.parser.sparql.ast.ASTBind;
import org.openrdf.query.parser.sparql.ast.ASTBlankNode;
import org.openrdf.query.parser.sparql.ast.ASTBlankNodePropertyList;
import org.openrdf.query.parser.sparql.ast.ASTBound;
import org.openrdf.query.parser.sparql.ast.ASTCeil;
import org.openrdf.query.parser.sparql.ast.ASTCoalesce;
import org.openrdf.query.parser.sparql.ast.ASTCollection;
import org.openrdf.query.parser.sparql.ast.ASTCompare;
import org.openrdf.query.parser.sparql.ast.ASTConcat;
import org.openrdf.query.parser.sparql.ast.ASTConstraint;
import org.openrdf.query.parser.sparql.ast.ASTConstruct;
import org.openrdf.query.parser.sparql.ast.ASTConstructQuery;
import org.openrdf.query.parser.sparql.ast.ASTContains;
import org.openrdf.query.parser.sparql.ast.ASTCount;
import org.openrdf.query.parser.sparql.ast.ASTDatatype;
import org.openrdf.query.parser.sparql.ast.ASTDay;
import org.openrdf.query.parser.sparql.ast.ASTDescribe;
import org.openrdf.query.parser.sparql.ast.ASTDescribeQuery;
import org.openrdf.query.parser.sparql.ast.ASTEncodeForURI;
import org.openrdf.query.parser.sparql.ast.ASTExistsFunc;
import org.openrdf.query.parser.sparql.ast.ASTFalse;
import org.openrdf.query.parser.sparql.ast.ASTFloor;
import org.openrdf.query.parser.sparql.ast.ASTFunctionCall;
import org.openrdf.query.parser.sparql.ast.ASTGraphGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTGraphPatternGroup;
import org.openrdf.query.parser.sparql.ast.ASTGroupClause;
import org.openrdf.query.parser.sparql.ast.ASTGroupConcat;
import org.openrdf.query.parser.sparql.ast.ASTGroupCondition;
import org.openrdf.query.parser.sparql.ast.ASTHavingClause;
import org.openrdf.query.parser.sparql.ast.ASTHours;
import org.openrdf.query.parser.sparql.ast.ASTIRI;
import org.openrdf.query.parser.sparql.ast.ASTIRIFunc;
import org.openrdf.query.parser.sparql.ast.ASTIf;
import org.openrdf.query.parser.sparql.ast.ASTIn;
import org.openrdf.query.parser.sparql.ast.ASTIsBlank;
import org.openrdf.query.parser.sparql.ast.ASTIsIRI;
import org.openrdf.query.parser.sparql.ast.ASTIsLiteral;
import org.openrdf.query.parser.sparql.ast.ASTIsNumeric;
import org.openrdf.query.parser.sparql.ast.ASTLang;
import org.openrdf.query.parser.sparql.ast.ASTLangMatches;
import org.openrdf.query.parser.sparql.ast.ASTLimit;
import org.openrdf.query.parser.sparql.ast.ASTLowerCase;
import org.openrdf.query.parser.sparql.ast.ASTMD5;
import org.openrdf.query.parser.sparql.ast.ASTMath;
import org.openrdf.query.parser.sparql.ast.ASTMax;
import org.openrdf.query.parser.sparql.ast.ASTMin;
import org.openrdf.query.parser.sparql.ast.ASTMinusGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTMinutes;
import org.openrdf.query.parser.sparql.ast.ASTMonth;
import org.openrdf.query.parser.sparql.ast.ASTNot;
import org.openrdf.query.parser.sparql.ast.ASTNotExistsFunc;
import org.openrdf.query.parser.sparql.ast.ASTNotIn;
import org.openrdf.query.parser.sparql.ast.ASTNow;
import org.openrdf.query.parser.sparql.ast.ASTNumericLiteral;
import org.openrdf.query.parser.sparql.ast.ASTObjectList;
import org.openrdf.query.parser.sparql.ast.ASTOffset;
import org.openrdf.query.parser.sparql.ast.ASTOptionalGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTOr;
import org.openrdf.query.parser.sparql.ast.ASTOrderClause;
import org.openrdf.query.parser.sparql.ast.ASTOrderCondition;
import org.openrdf.query.parser.sparql.ast.ASTPathAlternative;
import org.openrdf.query.parser.sparql.ast.ASTPathElt;
import org.openrdf.query.parser.sparql.ast.ASTPathMod;
import org.openrdf.query.parser.sparql.ast.ASTPathOneInPropertySet;
import org.openrdf.query.parser.sparql.ast.ASTPathSequence;
import org.openrdf.query.parser.sparql.ast.ASTProjectionElem;
import org.openrdf.query.parser.sparql.ast.ASTPropertyList;
import org.openrdf.query.parser.sparql.ast.ASTPropertyListPath;
import org.openrdf.query.parser.sparql.ast.ASTQName;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ASTRDFLiteral;
import org.openrdf.query.parser.sparql.ast.ASTRand;
import org.openrdf.query.parser.sparql.ast.ASTRegexExpression;
import org.openrdf.query.parser.sparql.ast.ASTRound;
import org.openrdf.query.parser.sparql.ast.ASTSHA1;
import org.openrdf.query.parser.sparql.ast.ASTSHA224;
import org.openrdf.query.parser.sparql.ast.ASTSHA256;
import org.openrdf.query.parser.sparql.ast.ASTSHA384;
import org.openrdf.query.parser.sparql.ast.ASTSHA512;
import org.openrdf.query.parser.sparql.ast.ASTSameTerm;
import org.openrdf.query.parser.sparql.ast.ASTSeconds;
import org.openrdf.query.parser.sparql.ast.ASTSelect;
import org.openrdf.query.parser.sparql.ast.ASTSelectQuery;
import org.openrdf.query.parser.sparql.ast.ASTStr;
import org.openrdf.query.parser.sparql.ast.ASTStrDt;
import org.openrdf.query.parser.sparql.ast.ASTStrEnds;
import org.openrdf.query.parser.sparql.ast.ASTStrLang;
import org.openrdf.query.parser.sparql.ast.ASTStrLen;
import org.openrdf.query.parser.sparql.ast.ASTStrStarts;
import org.openrdf.query.parser.sparql.ast.ASTString;
import org.openrdf.query.parser.sparql.ast.ASTSubstr;
import org.openrdf.query.parser.sparql.ast.ASTSum;
import org.openrdf.query.parser.sparql.ast.ASTTimezone;
import org.openrdf.query.parser.sparql.ast.ASTTrue;
import org.openrdf.query.parser.sparql.ast.ASTTz;
import org.openrdf.query.parser.sparql.ast.ASTUnionGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTUpperCase;
import org.openrdf.query.parser.sparql.ast.ASTVar;
import org.openrdf.query.parser.sparql.ast.ASTYear;
import org.openrdf.query.parser.sparql.ast.Node;
import org.openrdf.query.parser.sparql.ast.SimpleNode;
import org.openrdf.query.parser.sparql.ast.VisitorException;

/**
 * @author Arjohn Kampman
 */
public class TupleExprBuilder extends ASTVisitorBase {

	/*-----------*
	 * Variables *
	 *-----------*/

	private ValueFactory valueFactory;

	private GraphPattern graphPattern;

	private int constantVarID = 1;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public TupleExprBuilder(ValueFactory valueFactory) {
		this.valueFactory = valueFactory;
	}

	/*---------*
	 * Methods *
	 *---------*/

	private Var valueExpr2Var(ValueExpr valueExpr) {
		if (valueExpr instanceof Var) {
			return (Var)valueExpr;
		}
		else if (valueExpr instanceof ValueConstant) {
			return createConstVar(((ValueConstant)valueExpr).getValue());
		}
		else if (valueExpr == null) {
			throw new IllegalArgumentException("valueExpr is null");
		}
		else {
			throw new IllegalArgumentException("valueExpr is a: " + valueExpr.getClass());
		}
	}

	private Var createConstVar(Value value) {
		Var var = createAnonVar("-const-" + constantVarID++);
		var.setValue(value);
		return var;
	}

	private Var createAnonVar(String varName) {
		Var var = new Var(varName);
		var.setAnonymous(true);
		return var;
	}

	private FunctionCall createFunctionCall(String uri, SimpleNode node, int minArgs, int maxArgs)
		throws VisitorException
	{
		FunctionCall functionCall = new FunctionCall(uri);

		int noOfArguments = node.jjtGetNumChildren();

		if (noOfArguments > maxArgs || noOfArguments < minArgs) {
			throw new VisitorException("unexpected number of arguments (" + noOfArguments + ") for function "
					+ uri);
		}

		for (int i = 0; i < noOfArguments; i++) {
			Node argNode = node.jjtGetChild(i);
			functionCall.addArg((ValueExpr)argNode.jjtAccept(this, null));
		}

		return functionCall;
	}

	@Override
	public TupleExpr visit(ASTQueryContainer node, Object data)
		throws VisitorException
	{
		// Skip the prolog, any information it contains should already have been
		// processed
		return (TupleExpr)node.getQuery().jjtAccept(this, null);
	}

	@Override
	public TupleExpr visit(ASTSelectQuery node, Object data)
		throws VisitorException
	{
		GraphPattern parentGP = graphPattern;

		// Start with building the graph pattern
		graphPattern = new GraphPattern();
		node.getWhereClause().jjtAccept(this, null);
		TupleExpr tupleExpr = graphPattern.buildTupleExpr();

		// Apply grouping
		ASTGroupClause groupNode = node.getGroupClause();
		if (groupNode != null) {

			tupleExpr = (TupleExpr)groupNode.jjtAccept(this, tupleExpr);
		}

		// Apply HAVING group filter condition
		ASTHavingClause havingNode = node.getHavingClause();
		if (havingNode != null) {

			// add implicit group if necessary
			if (!(tupleExpr instanceof Group)) {
				tupleExpr = new Group(tupleExpr);
			}

			// FIXME is the child of a HAVING node always a compare?
			Compare condition = (Compare)havingNode.jjtGetChild(0).jjtAccept(this, tupleExpr);

			// retrieve any aggregate operators from the condition.
			AggregateCollector collector = new AggregateCollector();
			collector.meet(condition);

			// replace operator occurrences with an anonymous var, and alias it
			// to the group
			Extension extension = new Extension();
			for (AggregateOperator operator : collector.getOperators()) {
				Var var = createAnonVar("-const-" + constantVarID++);

				// replace occurrence of the operator in the filter condition
				// with the variable.
				AggregateOperatorReplacer replacer = new AggregateOperatorReplacer(operator, var);
				replacer.meet(condition);

				String alias = var.getName();

				// create an extension linking the operator to the variable
				// name.
				ExtensionElem pe = new ExtensionElem(operator, alias);
				extension.addElement(pe);

				// add the aggregate operator to the group.
				GroupElem ge = new GroupElem(alias, operator);

				// FIXME quite often the aggregate in the HAVING clause will be
				// a
				// duplicate of an aggregate in the projection. We could perhaps
				// optimize for that, to avoid
				// having to evaluate twice.
				((Group)tupleExpr).addGroupElement(ge);
			}

			extension.setArg(tupleExpr);
			tupleExpr = new Filter(extension, condition);
		}

		// Apply result ordering
		ASTOrderClause orderNode = node.getOrderClause();
		if (orderNode != null) {
			List<OrderElem> orderElemements = (List<OrderElem>)orderNode.jjtAccept(this, null);
			tupleExpr = new Order(tupleExpr, orderElemements);
		}

		// Apply projection
		tupleExpr = (TupleExpr)node.getSelect().jjtAccept(this, tupleExpr);

		// Process limit and offset clauses
		ASTLimit limitNode = node.getLimit();
		long limit = -1L;
		if (limitNode != null) {
			limit = (Long)limitNode.jjtAccept(this, null);
		}

		ASTOffset offsetNode = node.getOffset();
		long offset = -1L;
		if (offsetNode != null) {
			offset = (Long)offsetNode.jjtAccept(this, null);
		}

		if (offset >= 1L || limit >= 0L) {
			tupleExpr = new Slice(tupleExpr, offset, limit);
		}

		if (parentGP != null) {
			parentGP.addRequiredTE(tupleExpr);
			graphPattern = parentGP;
		}

		return tupleExpr;
	}

	@Override
	public TupleExpr visit(ASTSelect node, Object data)
		throws VisitorException
	{
		TupleExpr result = (TupleExpr)data;

		Extension extension = new Extension();

		ProjectionElemList projElemList = new ProjectionElemList();

		for (ASTProjectionElem projElemNode : node.getProjectionElemList()) {

			Node child = projElemNode.jjtGetChild(0);

			String alias = projElemNode.getAlias();
			if (alias != null) {
				// aliased projection element
				ValueExpr valueExpr = (ValueExpr)child.jjtAccept(this, null);

				projElemList.addElement(new ProjectionElem(alias));

				AggregateCollector collector = new AggregateCollector();
				valueExpr.visit(collector);

				if (collector.getOperators().size() > 0) {
					for (AggregateOperator operator : collector.getOperators()) {
						// Apply implicit grouping if necessary
						GroupFinder groupFinder = new GroupFinder();
						result.visit(groupFinder);
						Group group = groupFinder.getGroup();

						boolean existingGroup = true;
						if (group == null) {
							group = new Group(result);
							existingGroup = false;
						}

						if (operator.equals(valueExpr)) {
							group.addGroupElement(new GroupElem(alias, operator));
							extension.setArg(group);
						}
						else {

							ValueExpr expr = (ValueExpr)operator.getParentNode();

							Extension anonymousExtension = new Extension();
							Var anonVar = createConstVar(null);
							expr.replaceChildNode(operator, anonVar);
							anonymousExtension.addElement(new ExtensionElem(operator, anonVar.getName()));

							anonymousExtension.setArg(result);
							result = anonymousExtension;
							group.addGroupElement(new GroupElem(anonVar.getName(), operator));

						}

						if (!existingGroup) {
							result = group;
						}
					}
				}
				extension.addElement(new ExtensionElem(valueExpr, alias));
			}
			else if (child instanceof ASTVar) {
				Var projVar = (Var)child.jjtAccept(this, null);
				projElemList.addElement(new ProjectionElem(projVar.getName()));
			}
			else {
				throw new IllegalStateException("required alias for non-Var projection elements not found");
			}
		}

		if (!extension.getElements().isEmpty()) {
			extension.setArg(result);
			result = extension;
		}

		result = new Projection(result, projElemList);

		if (node.isDistinct()) {
			result = new Distinct(result);
		}
		else if (node.isReduced()) {
			result = new Reduced(result);
		}

		return result;
	}

	private class GroupFinder extends QueryModelVisitorBase<VisitorException> {

		private Group group;

		@Override
		public void meet(Projection projection) {
			// stop tree traversal on finding a projection: we do not wish to find
			// the group in a sub-select.
		}

		@Override
		public void meet(Group group) {
			this.group = group;
		}

		public Group getGroup() {
			return group;
		}
	}

	@Override
	public TupleExpr visit(ASTConstructQuery node, Object data)
		throws VisitorException
	{
		// Start with building the graph pattern
		graphPattern = new GraphPattern();
		node.getWhereClause().jjtAccept(this, null);
		TupleExpr tupleExpr = graphPattern.buildTupleExpr();

		// Apply result ordering
		ASTOrderClause orderNode = node.getOrderClause();
		if (orderNode != null) {
			List<OrderElem> orderElemements = (List<OrderElem>)orderNode.jjtAccept(this, null);
			tupleExpr = new Order(tupleExpr, orderElemements);
		}

		// Process construct clause
		ASTConstruct constructNode = node.getConstruct();
		if (!constructNode.isWildcard()) {
			tupleExpr = (TupleExpr)constructNode.jjtAccept(this, tupleExpr);
		}
		else {
			// create construct clause from graph pattern.
			ConstructorBuilder cb = new ConstructorBuilder();

			// SPARQL does not allow distinct or reduced right now. Leaving
			// functionality in construct builder for
			// possible future use.
			tupleExpr = cb.buildConstructor(tupleExpr, false, false);
		}

		// process limit and offset clauses
		ASTLimit limitNode = node.getLimit();
		long limit = -1L;
		if (limitNode != null) {
			limit = (Long)limitNode.jjtAccept(this, null);
		}

		ASTOffset offsetNode = node.getOffset();
		long offset = -1;
		if (offsetNode != null) {
			offset = (Long)offsetNode.jjtAccept(this, null);
		}

		if (offset >= 1 || limit >= 0) {
			tupleExpr = new Slice(tupleExpr, offset, limit);
		}

		return tupleExpr;
	}

	@Override
	public TupleExpr visit(ASTConstruct node, Object data)
		throws VisitorException
	{
		TupleExpr result = (TupleExpr)data;

		// Collect construct triples
		graphPattern = new GraphPattern();
		super.visit(node, null);
		TupleExpr constructExpr = graphPattern.buildTupleExpr();

		// Retrieve all StatementPattern's from the construct expression
		List<StatementPattern> statementPatterns = StatementPatternCollector.process(constructExpr);

		Set<Var> constructVars = getConstructVars(statementPatterns);

		// Create BNodeGenerator's for all anonymous variables
		Map<Var, ExtensionElem> extElemMap = new HashMap<Var, ExtensionElem>();

		for (Var var : constructVars) {
			if (var.isAnonymous() && !extElemMap.containsKey(var)) {
				ValueExpr valueExpr;

				if (var.hasValue()) {
					valueExpr = new ValueConstant(var.getValue());
				}
				else {
					valueExpr = new BNodeGenerator();
				}

				extElemMap.put(var, new ExtensionElem(valueExpr, var.getName()));
			}
		}

		if (!extElemMap.isEmpty()) {
			result = new Extension(result, extElemMap.values());
		}

		// Create a Projection for each StatementPattern in the constructor
		List<ProjectionElemList> projList = new ArrayList<ProjectionElemList>();

		for (StatementPattern sp : statementPatterns) {
			ProjectionElemList projElemList = new ProjectionElemList();

			projElemList.addElement(new ProjectionElem(sp.getSubjectVar().getName(), "subject"));
			projElemList.addElement(new ProjectionElem(sp.getPredicateVar().getName(), "predicate"));
			projElemList.addElement(new ProjectionElem(sp.getObjectVar().getName(), "object"));

			projList.add(projElemList);
		}

		if (projList.size() == 1) {
			result = new Projection(result, projList.get(0));
		}
		else if (projList.size() > 1) {
			result = new MultiProjection(result, projList);
		}
		else {
			// Empty constructor
			result = new EmptySet();
		}

		return new Reduced(result);
	}

	/**
	 * Gets the set of variables that are relevant for the constructor. This
	 * method accumulates all subject, predicate and object variables from the
	 * supplied statement patterns, but ignores any context variables.
	 */
	private Set<Var> getConstructVars(Collection<StatementPattern> statementPatterns) {
		Set<Var> vars = new LinkedHashSet<Var>(statementPatterns.size() * 2);

		for (StatementPattern sp : statementPatterns) {
			vars.add(sp.getSubjectVar());
			vars.add(sp.getPredicateVar());
			vars.add(sp.getObjectVar());
		}

		return vars;
	}

	@Override
	public TupleExpr visit(ASTDescribeQuery node, Object data)
		throws VisitorException
	{
		TupleExpr tupleExpr = null;

		if (node.getWhereClause() != null) {
			// Start with building the graph pattern
			graphPattern = new GraphPattern();
			node.getWhereClause().jjtAccept(this, null);
			tupleExpr = graphPattern.buildTupleExpr();

			// Apply result ordering
			ASTOrderClause orderNode = node.getOrderClause();
			if (orderNode != null) {
				List<OrderElem> orderElemements = (List<OrderElem>)orderNode.jjtAccept(this, null);
				tupleExpr = new Order(tupleExpr, orderElemements);
			}

			// Process limit and offset clauses
			ASTLimit limitNode = node.getLimit();
			long limit = -1;
			if (limitNode != null) {
				limit = (Long)limitNode.jjtAccept(this, null);
			}

			ASTOffset offsetNode = node.getOffset();
			long offset = -1;
			if (offsetNode != null) {
				offset = (Long)offsetNode.jjtAccept(this, null);
			}

			if (offset >= 1 || limit >= 0) {
				tupleExpr = new Slice(tupleExpr, offset, limit);
			}
		}

		// Process describe clause last
		return (TupleExpr)node.getDescribe().jjtAccept(this, tupleExpr);
	}

	@Override
	public TupleExpr visit(ASTDescribe node, Object data)
		throws VisitorException
	{
		TupleExpr result = (TupleExpr)data;

		// Create a graph query that produces the statements that have the
		// requests resources as subject or object
		Var subjVar = createAnonVar("-descr-subj");
		Var predVar = createAnonVar("-descr-pred");
		Var objVar = createAnonVar("-descr-obj");
		StatementPattern sp = new StatementPattern(subjVar, predVar, objVar);

		if (result == null) {
			result = sp;
		}
		else {
			result = new Join(result, sp);
		}

		List<SameTerm> sameTerms = new ArrayList<SameTerm>(2 * node.jjtGetNumChildren());

		for (int i = 0; i < node.jjtGetNumChildren(); i++) {
			ValueExpr resource = (ValueExpr)node.jjtGetChild(i).jjtAccept(this, null);

			sameTerms.add(new SameTerm(subjVar.clone(), resource));
			sameTerms.add(new SameTerm(objVar.clone(), resource));
		}

		ValueExpr constraint = sameTerms.get(0);
		for (int i = 1; i < sameTerms.size(); i++) {
			constraint = new Or(constraint, sameTerms.get(i));
		}

		result = new Filter(result, constraint);

		ProjectionElemList projElemList = new ProjectionElemList();
		projElemList.addElement(new ProjectionElem(subjVar.getName(), "subject"));
		projElemList.addElement(new ProjectionElem(predVar.getName(), "predicate"));
		projElemList.addElement(new ProjectionElem(objVar.getName(), "object"));
		result = new Projection(result, projElemList);

		return new Reduced(result);
	}

	@Override
	public TupleExpr visit(ASTAskQuery node, Object data)
		throws VisitorException
	{
		graphPattern = new GraphPattern();

		super.visit(node, null);

		TupleExpr tupleExpr = graphPattern.buildTupleExpr();
		tupleExpr = new Slice(tupleExpr, 0, 1);

		return tupleExpr;
	}

	@Override
	public Group visit(ASTGroupClause node, Object data)
		throws VisitorException
	{
		TupleExpr tupleExpr = (TupleExpr)data;
		Group g = new Group(tupleExpr);
		int childCount = node.jjtGetNumChildren();

		List<String> groupBindingNames = new ArrayList<String>();
		for (int i = 0; i < childCount; i++) {
			String name = (String)node.jjtGetChild(i).jjtAccept(this, g);
			groupBindingNames.add(name);
		}

		g.setGroupBindingNames(groupBindingNames);

		return g;
	}

	@Override
	public String visit(ASTGroupCondition node, Object data)
		throws VisitorException
	{
		Group group = (Group)data;
		TupleExpr arg = group.getArg();

		Extension extension = null;
		if (arg instanceof Extension) {
			extension = (Extension)arg;
		}
		else {
			extension = new Extension();
		}

		String name = null;
		ValueExpr ve = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, data);
		if (ve instanceof Var) {
			name = ((Var)ve).getName();
		}
		else {
			if (node.jjtGetNumChildren() > 1) {
				Var v = (Var)node.jjtGetChild(1).jjtAccept(this, data);
				name = v.getName();
			}
			else {
				// create an alias on the spot
				name = createConstVar(null).getName();
			}

			ExtensionElem elem = new ExtensionElem(ve, name);
			extension.addElement(elem);
		}

		if (extension.getElements().size() > 0 && !(arg instanceof Extension)) {
			extension.setArg(arg);
			group.setArg(extension);
		}

		return name;
	}

	@Override
	public List<OrderElem> visit(ASTOrderClause node, Object data)
		throws VisitorException
	{
		int childCount = node.jjtGetNumChildren();
		List<OrderElem> elements = new ArrayList<OrderElem>(childCount);

		for (int i = 0; i < childCount; i++) {
			elements.add((OrderElem)node.jjtGetChild(i).jjtAccept(this, null));
		}

		return elements;
	}

	@Override
	public OrderElem visit(ASTOrderCondition node, Object data)
		throws VisitorException
	{
		ValueExpr valueExpr = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		return new OrderElem(valueExpr, node.isAscending());
	}

	@Override
	public Long visit(ASTLimit node, Object data)
		throws VisitorException
	{
		return node.getValue();
	}

	@Override
	public Long visit(ASTOffset node, Object data)
		throws VisitorException
	{
		return node.getValue();
	}

	@Override
	public Object visit(ASTGraphPatternGroup node, Object data)
		throws VisitorException
	{
		GraphPattern parentGP = graphPattern;
		graphPattern = new GraphPattern(parentGP);

		super.visit(node, null);

		// Filters are scoped to the graph pattern group and do not affect
		// bindings external to the group
		TupleExpr te = graphPattern.buildTupleExpr();

		// TODO not sure this is the cleanest way of handling this.
		if (data != null && data instanceof Exists) {
			((Exists)data).setSubQuery(te);
		}
		else {
			parentGP.addRequiredTE(te);
		}

		graphPattern = parentGP;

		return null;
	}

	@Override
	public Object visit(ASTOptionalGraphPattern node, Object data)
		throws VisitorException
	{
		GraphPattern parentGP = graphPattern;
		graphPattern = new GraphPattern(parentGP);

		super.visit(node, null);

		// Optional constraints also apply to left hand side of operator
		List<ValueExpr> constraints = graphPattern.removeAllConstraints();

		TupleExpr leftArg = parentGP.buildTupleExpr();
		TupleExpr rightArg = graphPattern.buildTupleExpr();

		LeftJoin leftJoin;

		if (constraints.isEmpty()) {
			leftJoin = new LeftJoin(leftArg, rightArg);
		}
		else {
			ValueExpr constraint = constraints.get(0);
			for (int i = 1; i < constraints.size(); i++) {
				constraint = new And(constraint, constraints.get(i));
			}

			leftJoin = new LeftJoin(leftArg, rightArg, constraint);
		}

		graphPattern = parentGP;

		graphPattern.clear();
		graphPattern.addRequiredTE(leftJoin);

		return null;
	}

	@Override
	public Object visit(ASTGraphGraphPattern node, Object data)
		throws VisitorException
	{
		Var oldContext = graphPattern.getContextVar();
		Scope oldScope = graphPattern.getStatementPatternScope();

		ValueExpr newContext = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);

		graphPattern.setContextVar(valueExpr2Var(newContext));
		graphPattern.setStatementPatternScope(Scope.NAMED_CONTEXTS);

		node.jjtGetChild(1).jjtAccept(this, null);

		graphPattern.setContextVar(oldContext);
		graphPattern.setStatementPatternScope(oldScope);

		return null;
	}

	@Override
	public Object visit(ASTUnionGraphPattern node, Object data)
		throws VisitorException
	{
		GraphPattern parentGP = graphPattern;

		graphPattern = new GraphPattern(parentGP);
		node.jjtGetChild(0).jjtAccept(this, null);
		TupleExpr leftArg = graphPattern.buildTupleExpr();

		graphPattern = new GraphPattern(parentGP);
		node.jjtGetChild(1).jjtAccept(this, null);
		TupleExpr rightArg = graphPattern.buildTupleExpr();

		parentGP.addRequiredTE(new Union(leftArg, rightArg));
		graphPattern = parentGP;

		return null;
	}

	@Override
	public Object visit(ASTMinusGraphPattern node, Object data)
		throws VisitorException
	{
		GraphPattern parentGP = graphPattern;

		TupleExpr leftArg = graphPattern.buildTupleExpr();

		graphPattern = new GraphPattern(parentGP);
		node.jjtGetChild(0).jjtAccept(this, null);
		TupleExpr rightArg = graphPattern.buildTupleExpr();

		parentGP = new GraphPattern();
		parentGP.addRequiredTE(new Difference(leftArg, rightArg));
		graphPattern = parentGP;

		return null;
	}

	@Override
	public Object visit(ASTPropertyList propListNode, Object data)
		throws VisitorException
	{
		ValueExpr subject = (ValueExpr)data;
		ValueExpr predicate = (ValueExpr)propListNode.getVerb().jjtAccept(this, null);
		@SuppressWarnings("unchecked")
		List<ValueExpr> objectList = (List<ValueExpr>)propListNode.getObjectList().jjtAccept(this, null);

		Var subjVar = valueExpr2Var(subject);
		Var predVar = valueExpr2Var(predicate);

		for (ValueExpr object : objectList) {
			Var objVar = valueExpr2Var(object);
			graphPattern.addRequiredSP(subjVar, predVar, objVar);
		}

		ASTPropertyList nextPropList = propListNode.getNextPropertyList();
		if (nextPropList != null) {
			nextPropList.jjtAccept(this, subject);
		}

		return null;
	}

	@Override
	public Object visit(ASTPathAlternative pathAltNode, Object data)
		throws VisitorException
	{

		if (pathAltNode.jjtGetNumChildren() > 1) {

			GraphPattern parentGP = graphPattern;

			graphPattern = new GraphPattern(parentGP);
			pathAltNode.jjtGetChild(0).jjtAccept(this, data);
			TupleExpr leftArg = graphPattern.buildTupleExpr();

			graphPattern = new GraphPattern(parentGP);

			pathAltNode.jjtGetChild(1).jjtAccept(this, data);
			TupleExpr rightArg = graphPattern.buildTupleExpr();
			parentGP.addRequiredTE(new Union(leftArg, rightArg));

			graphPattern = parentGP;
		}
		else {
			pathAltNode.jjtGetChild(0).jjtAccept(this, data);
		}

		return null;
	}

	@Override
	public PropertySetElem visit(ASTPathOneInPropertySet node, Object data)
		throws VisitorException
	{

		PropertySetElem result = new PropertySetElem();
		result.setInverse(node.isInverse());
		ValueConstant predicate = (ValueConstant)node.jjtGetChild(0).jjtAccept(this, data);
		result.setPredicate(predicate);

		return result;
	}

	private ASTObjectList getObjectList(Node node) {
		if (node == null) {
			return null;
		}
		if (node instanceof ASTPropertyListPath) {
			return ((ASTPropertyListPath)node).getObjectList();
		}
		else {
			return getObjectList(node.jjtGetParent());
		}
	}

	@Override
	public Object visit(ASTPathSequence pathSeqNode, Object data)
		throws VisitorException
	{
		ValueExpr subject = (ValueExpr)data;
		Var subjVar = valueExpr2Var(subject);

		@SuppressWarnings("unchecked")
		List<ValueExpr> objectList = (List<ValueExpr>)getObjectList(pathSeqNode).jjtAccept(this, null);

		List<ASTPathElt> pathElements = pathSeqNode.getPathElements();

		int pathLength = pathElements.size();

		GraphPattern pathSequencePattern = new GraphPattern(graphPattern);

		Scope scope = pathSequencePattern.getStatementPatternScope();
		Var contextVar = pathSequencePattern.getContextVar();

		for (int i = 0; i < pathLength; i++) {
			ASTPathElt pathElement = pathElements.get(i);

			ASTPathMod pathMod = pathElement.getPathMod();

			long lowerBound = Long.MIN_VALUE;
			long upperBound = Long.MIN_VALUE;

			if (pathMod != null) {
				lowerBound = pathMod.getLowerBound();
				upperBound = pathMod.getUpperBound();

				if (upperBound == Long.MIN_VALUE) {
					upperBound = lowerBound;
				}
				else if (lowerBound == Long.MIN_VALUE) {
					lowerBound = upperBound;
				}
			}

			if (pathElement.isNegatedPropertySet()) {

				// create a temporary negated property set object and set the
				// correct subject and object vars to continue
				// the path sequence.

				NegatedPropertySet nps = new NegatedPropertySet();
				nps.setScope(scope);
				nps.setSubjectVar(subjVar);
				nps.setContextVar(contextVar);

				for (Node child : pathElement.jjtGetChildren()) {
					nps.addPropertySetElem((PropertySetElem)child.jjtAccept(this, data));
				}

				if (i == pathLength - 1) {
					nps.setObjectList(objectList);
				}
				else {
					// not last element in path.
					Var nextVar = createAnonVar(subjVar.getName() + "-property-set-" + i);

					List<ValueExpr> nextVarList = new ArrayList<ValueExpr>();
					nextVarList.add(nextVar);
					nps.setObjectList(nextVarList);

					subjVar = nextVar;
				}

				// convert the NegatedPropertySet to a proper TupleExpr
				pathSequencePattern.addRequiredTE(createTupleExprForNegatedPropertySet(nps, i));

			}
			else if (pathElement.isNestedPath()) {
				GraphPattern parentGP = graphPattern;

				graphPattern = new GraphPattern();

				if (i == pathLength - 1) {
					// last element in the path
					pathElement.jjtGetChild(0).jjtAccept(this, data);

					TupleExpr te = graphPattern.buildTupleExpr();

					for (ValueExpr object : objectList) {
						Var objVar = valueExpr2Var(object);
						te = handlePathModifiers(scope, subjVar, te, objVar, contextVar, lowerBound, upperBound);
						pathSequencePattern.addRequiredTE(te);
					}
				}
				else {
					// not the last element in the path, introduce an anonymous var
					// to connect.
					Var nextVar = createAnonVar(subjVar.getName() + "-nested-" + i);

					pathElement.jjtGetChild(0).jjtAccept(this, data);

					TupleExpr te = graphPattern.buildTupleExpr();

					// replace all object list occurrences with the intermediate var.

					te = replaceVarOccurrence(te, objectList, nextVar);
					te = handlePathModifiers(scope, subjVar, te, nextVar, contextVar, lowerBound, upperBound);
					pathSequencePattern.addRequiredTE(te);

					subjVar = nextVar;
				}

				graphPattern = parentGP;
			}
			else {

				ValueExpr pred = (ValueExpr)pathElement.jjtAccept(this, data);
				Var predVar = valueExpr2Var(pred);

				TupleExpr te;
				if (i == pathLength - 1) {
					// last element in the path, connect to list of defined objects
					for (ValueExpr object : objectList) {
						Var objVar = valueExpr2Var(object);

						if (pathElement.isInverse()) {
							te = new StatementPattern(scope, objVar, predVar, subjVar, contextVar);
							te = handlePathModifiers(scope, objVar, te, subjVar, contextVar, lowerBound, upperBound);

							pathSequencePattern.addRequiredTE(te);
						}
						else {
							te = new StatementPattern(scope, subjVar, predVar, objVar, contextVar);
							te = handlePathModifiers(scope, subjVar, te, objVar, contextVar, lowerBound, upperBound);

							pathSequencePattern.addRequiredTE(te);
						}
					}
				}
				else {
					// not the last element in the path, introduce an anonymous var
					// to connect.
					Var nextVar = createAnonVar(predVar.getName() + "-" + i);
					if (pathElement.isInverse()) {

						te = new StatementPattern(scope, nextVar, predVar, subjVar, contextVar);

						te = handlePathModifiers(scope, nextVar, te, subjVar, contextVar, lowerBound, upperBound);

						pathSequencePattern.addRequiredTE(te);
					}
					else {
						te = new StatementPattern(scope, subjVar, predVar, nextVar, contextVar);
						te = handlePathModifiers(scope, subjVar, te, nextVar, contextVar, lowerBound, upperBound);

						pathSequencePattern.addRequiredTE(te);
					}

					// set the subject for the next element in the path.
					subjVar = nextVar;
				}
			}
		}

		// add the created path sequence to the graph pattern.
		for (TupleExpr te : pathSequencePattern.getRequiredTEs()) {
			graphPattern.addRequiredTE(te);
		}

		return null;
	}

	private TupleExpr createTupleExprForNegatedPropertySet(NegatedPropertySet nps, int index) {
		Var subjVar = nps.getSubjectVar();

		Var predVar = createAnonVar("nps-" + subjVar.getName() + "-" + index);
		// Var predVarInverse = createAnonVar("nps-inverse-" + subjVar.getName() +
		// "-" + index);

		ValueExpr filterCondition = null;
		ValueExpr filterConditionInverse = null;

		// build (inverted) filter conditions for each negated path element.
		for (PropertySetElem elem : nps.getPropertySetElems()) {
			ValueConstant predicate = elem.getPredicate();

			if (elem.isInverse()) {
				Compare compare = new Compare(predVar, predicate, CompareOp.NE);
				if (filterConditionInverse == null) {
					filterConditionInverse = compare;
				}
				else {
					filterConditionInverse = new And(compare, filterConditionInverse);
				}
			}
			else {
				Compare compare = new Compare(predVar, predicate, CompareOp.NE);
				if (filterCondition == null) {
					filterCondition = compare;
				}
				else {
					filterCondition = new And(compare, filterCondition);
				}
			}
		}

		TupleExpr patternMatch = null;

		// build a regular statement pattern (or a join of several patterns if the
		// object list has more than
		// one item)
		if (filterCondition != null) {
			for (ValueExpr objVar : nps.getObjectList()) {
				if (patternMatch == null) {
					patternMatch = new StatementPattern(nps.getScope(), subjVar, predVar, (Var)objVar,
							nps.getContextVar());
				}
				else {
					patternMatch = new Join(new StatementPattern(nps.getScope(), subjVar, predVar, (Var)objVar,
							nps.getContextVar()), patternMatch);
				}
			}
		}

		TupleExpr patternMatchInverse = null;

		// build a inverse statement pattern (or a join of several patterns if the
		// object list has more than
		// one item):
		if (filterConditionInverse != null) {
			for (ValueExpr objVar : nps.getObjectList()) {
				if (patternMatchInverse == null) {
					patternMatchInverse = new StatementPattern(nps.getScope(), (Var)objVar, predVar, subjVar,
							nps.getContextVar());
				}
				else {
					patternMatchInverse = new Join(new StatementPattern(nps.getScope(), (Var)objVar, predVar,
							subjVar, nps.getContextVar()), patternMatchInverse);
				}
			}
		}

		TupleExpr completeMatch = null;

		if (patternMatch != null) {
			completeMatch = new Filter(patternMatch, filterCondition);
		}

		if (patternMatchInverse != null) {
			if (completeMatch == null) {
				completeMatch = new Filter(patternMatchInverse, filterConditionInverse);
			}
			else {
				completeMatch = new Union(new Filter(patternMatchInverse, filterConditionInverse), completeMatch);
			}
		}

		return completeMatch;
	}

	private TupleExpr replaceVarOccurrence(TupleExpr te, List<ValueExpr> objectList, Var replacementVar)
		throws VisitorException
	{
		for (ValueExpr objExpr : objectList) {
			Var objVar = valueExpr2Var(objExpr);
			VarReplacer replacer = new VarReplacer(objVar, replacementVar);
			te.visit(replacer);
		}
		return te;
	}

	private TupleExpr handlePathModifiers(Scope scope, Var subjVar, TupleExpr te, Var endVar, Var contextVar,
			long lowerBound, long upperBound)
		throws VisitorException
	{

		TupleExpr result = te;

		if (lowerBound >= 0L) {

			if (lowerBound < upperBound) {

				if (upperBound < Long.MAX_VALUE) {
					// upperbound is fixed-length

					// create set of unions for all path lengths between lower
					// and upper bound.
					Union union = new Union();
					Union currentUnion = union;

					for (long length = lowerBound; length < upperBound; length++) {

						TupleExpr path = createPath(scope, subjVar, te, endVar, contextVar, length);

						currentUnion.setLeftArg(path);
						if (length == upperBound - 1) {
							path = createPath(scope, subjVar, te, endVar, contextVar, length + 1);
							currentUnion.setRightArg(path);
						}
						else {
							Union nextUnion = new Union();
							currentUnion.setRightArg(nextUnion);
							currentUnion = nextUnion;
						}
					}

					result = union;
				}
				else {
					// upperbound is abitrary-length

					result = new ArbitraryLengthPath(scope, subjVar, te, endVar, contextVar, lowerBound);
				}
			}
			else {
				// create single path of fixed length.
				TupleExpr path = createPath(scope, subjVar, te, endVar, contextVar, lowerBound);
				result = path;
			}
		}

		return result;
	}

	private TupleExpr createPath(Scope scope, Var subjVar, TupleExpr pathExpression, Var endVar,
			Var contextVar, long length)
		throws VisitorException
	{
		if (pathExpression instanceof StatementPattern) {
			Var predVar = ((StatementPattern)pathExpression).getPredicateVar();

			if (length == 0L) {
				return new ZeroLengthPath(scope, subjVar, endVar, contextVar);
			}
			else {
				GraphPattern gp = new GraphPattern();
				gp.setContextVar(contextVar);
				gp.setStatementPatternScope(scope);

				Var nextVar = null;

				for (long i = 0L; i < length; i++) {
					if (i < length - 1) {
						nextVar = createAnonVar(predVar.getValue() + "-path-" + length + "-" + i);
					}
					else {
						nextVar = endVar;
					}
					gp.addRequiredSP(subjVar, predVar, nextVar);
					subjVar = nextVar;
				}
				return gp.buildTupleExpr();
			}
		}
		else {
			if (length == 0L) {
				return new ZeroLengthPath(scope, subjVar, endVar, contextVar);
			}
			else {
				GraphPattern gp = new GraphPattern();
				gp.setContextVar(contextVar);
				gp.setStatementPatternScope(scope);

				Var nextVar = null;
				for (long i = 0L; i < length; i++) {
					if (i < length - 1L) {
						nextVar = createAnonVar(subjVar.getName() + "-expression-path-" + length + "-" + i);
					}
					else {
						nextVar = endVar;
					}

					// create a clone of the path expression.
					TupleExpr clone = pathExpression.clone();

					VarReplacer replacer = new VarReplacer(endVar, nextVar);
					clone.visit(replacer);

					gp.addRequiredTE(clone);

					subjVar = nextVar;
				}
				return gp.buildTupleExpr();
			}
		}
	}

	private class VarReplacer extends QueryModelVisitorBase<VisitorException> {

		private Var toBeReplaced;

		private Var replacement;

		public VarReplacer(Var toBeReplaced, Var replacement) {
			this.toBeReplaced = toBeReplaced;
			this.replacement = replacement;
		}

		@Override
		public void meet(Var var) {
			if (toBeReplaced.equals(var)) {
				QueryModelNode parent = var.getParentNode();
				parent.replaceChildNode(var, replacement);
				replacement.setParentNode(parent);
			}
		}
	}

	@Override
	public Object visit(ASTPropertyListPath propListNode, Object data)
		throws VisitorException
	{
		ValueExpr subject = (ValueExpr)data;
		ValueExpr verbPath = (ValueExpr)propListNode.getVerb().jjtAccept(this, data);

		if (verbPath instanceof Var) {

			@SuppressWarnings("unchecked")
			List<ValueExpr> objectList = (List<ValueExpr>)propListNode.getObjectList().jjtAccept(this, null);

			Var subjVar = valueExpr2Var(subject);

			Var predVar = valueExpr2Var(verbPath);
			for (ValueExpr object : objectList) {
				Var objVar = valueExpr2Var(object);
				graphPattern.addRequiredSP(subjVar, predVar, objVar);
			}
		}
		else {
			// path is a single IRI or a more complex path. handled by the
			// visitor.

		}

		ASTPropertyListPath nextPropList = propListNode.getNextPropertyList();
		if (nextPropList != null) {
			nextPropList.jjtAccept(this, subject);
		}

		return null;
	}

	@Override
	public List<ValueExpr> visit(ASTObjectList node, Object data)
		throws VisitorException
	{
		int childCount = node.jjtGetNumChildren();
		List<ValueExpr> result = new ArrayList<ValueExpr>(childCount);

		for (int i = 0; i < childCount; i++) {
			result.add((ValueExpr)node.jjtGetChild(i).jjtAccept(this, null));
		}

		return result;
	}

	@Override
	public Var visit(ASTBlankNodePropertyList node, Object data)
		throws VisitorException
	{
		Var bnodeVar = createAnonVar(node.getVarName());
		super.visit(node, bnodeVar);
		return bnodeVar;
	}

	@Override
	public Var visit(ASTCollection node, Object data)
		throws VisitorException
	{
		String listVarName = node.getVarName();
		Var rootListVar = createAnonVar(listVarName);

		Var listVar = rootListVar;

		int childCount = node.jjtGetNumChildren();
		for (int i = 0; i < childCount; i++) {
			ValueExpr childValue = (ValueExpr)node.jjtGetChild(i).jjtAccept(this, null);

			Var childVar = valueExpr2Var(childValue);
			graphPattern.addRequiredSP(listVar, createConstVar(RDF.FIRST), childVar);

			Var nextListVar;
			if (i == childCount - 1) {
				nextListVar = createConstVar(RDF.NIL);
			}
			else {
				nextListVar = createAnonVar(listVarName + "-" + (i + 1));
			}

			graphPattern.addRequiredSP(listVar, createConstVar(RDF.REST), nextListVar);
			listVar = nextListVar;
		}

		return rootListVar;
	}

	@Override
	public Object visit(ASTConstraint node, Object data)
		throws VisitorException
	{
		ValueExpr valueExpr = (ValueExpr)super.visit(node, null);
		graphPattern.addConstraint(valueExpr);

		return valueExpr;
	}

	@Override
	public Or visit(ASTOr node, Object data)
		throws VisitorException
	{
		ValueExpr leftArg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		ValueExpr rightArg = (ValueExpr)node.jjtGetChild(1).jjtAccept(this, null);
		return new Or(leftArg, rightArg);
	}

	@Override
	public Object visit(ASTAnd node, Object data)
		throws VisitorException
	{
		ValueExpr leftArg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		ValueExpr rightArg = (ValueExpr)node.jjtGetChild(1).jjtAccept(this, null);
		return new And(leftArg, rightArg);
	}

	@Override
	public Not visit(ASTNot node, Object data)
		throws VisitorException
	{
		ValueExpr arg = (ValueExpr)super.visit(node, null);
		return new Not(arg);
	}

	@Override
	public Coalesce visit(ASTCoalesce node, Object data)
		throws VisitorException
	{

		Coalesce coalesce = new Coalesce();
		int noOfArgs = node.jjtGetNumChildren();

		for (int i = 0; i < noOfArgs; i++) {
			ValueExpr arg = (ValueExpr)node.jjtGetChild(i).jjtAccept(this, data);
			coalesce.addArgument(arg);
		}

		return coalesce;
	}

	@Override
	public Compare visit(ASTCompare node, Object data)
		throws VisitorException
	{
		ValueExpr leftArg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		ValueExpr rightArg = (ValueExpr)node.jjtGetChild(1).jjtAccept(this, null);
		return new Compare(leftArg, rightArg, node.getOperator());
	}

	@Override
	public FunctionCall visit(ASTSubstr node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.SUBSTRING.toString(), node, 2, 3);
	}

	@Override
	public FunctionCall visit(ASTConcat node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.CONCAT.toString(), node, 1, Integer.MAX_VALUE);
	}

	@Override
	public FunctionCall visit(ASTAbs node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.NUMERIC_ABS.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTCeil node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.NUMERIC_CEIL.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTContains node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.CONTAINS.toString(), node, 2, 2);
	}
	
	@Override
	public FunctionCall visit(ASTFloor node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.NUMERIC_FLOOR.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTRound node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.NUMERIC_ROUND.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTRand node, Object data)
		throws VisitorException
	{
		return createFunctionCall("RAND", node, 0, 0);
	}

	@Override
	public SameTerm visit(ASTSameTerm node, Object data)
		throws VisitorException
	{
		ValueExpr leftArg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		ValueExpr rightArg = (ValueExpr)node.jjtGetChild(1).jjtAccept(this, null);
		return new SameTerm(leftArg, rightArg);
	}

	@Override
	public MathExpr visit(ASTMath node, Object data)
		throws VisitorException
	{
		ValueExpr leftArg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		ValueExpr rightArg = (ValueExpr)node.jjtGetChild(1).jjtAccept(this, null);
		return new MathExpr(leftArg, rightArg, node.getOperator());
	}

	@Override
	public Object visit(ASTFunctionCall node, Object data)
		throws VisitorException
	{
		ValueConstant uriNode = (ValueConstant)node.jjtGetChild(0).jjtAccept(this, null);
		URI functionURI = (URI)uriNode.getValue();

		FunctionCall functionCall = new FunctionCall(functionURI.toString());

		for (int i = 1; i < node.jjtGetNumChildren(); i++) {
			Node argNode = node.jjtGetChild(i);
			functionCall.addArg((ValueExpr)argNode.jjtAccept(this, null));
		}

		return functionCall;
	}

	@Override
	public FunctionCall visit(ASTEncodeForURI node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.ENCODE_FOR_URI.toString(), node, 1, 1);
	}

	@Override
	public Object visit(ASTStr node, Object data)
		throws VisitorException
	{
		ValueExpr arg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		return new Str(arg);
	}

	@Override
	public FunctionCall visit(ASTStrDt node, Object data)
		throws VisitorException
	{
		return createFunctionCall("STRDT", node, 2, 2);
	}

	@Override
	public FunctionCall visit(ASTStrStarts node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.STARTS_WITH.toString(), node, 2, 2);
	}

	@Override
	public FunctionCall visit(ASTStrEnds node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.ENDS_WITH.toString(), node, 2, 2);
	}

	@Override
	public FunctionCall visit(ASTStrLen node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.STRING_LENGTH.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTUpperCase node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.UPPER_CASE.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTLowerCase node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.LOWER_CASE.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTStrLang node, Object data)
		throws VisitorException
	{
		return createFunctionCall("STRLANG", node, 2, 2);
	}

	@Override
	public FunctionCall visit(ASTNow node, Object data)
		throws VisitorException
	{
		return createFunctionCall("NOW", node, 0, 0);
	}

	@Override
	public FunctionCall visit(ASTYear node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.YEAR_FROM_DATETIME.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTMonth node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.MONTH_FROM_DATETIME.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTDay node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.DAY_FROM_DATETIME.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTHours node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.HOURS_FROM_DATETIME.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTMinutes node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.MINUTES_FROM_DATETIME.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTSeconds node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.SECONDS_FROM_DATETIME.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTTimezone node, Object data)
		throws VisitorException
	{
		return createFunctionCall(FN.TIMEZONE_FROM_DATETIME.toString(), node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTTz node, Object data)
		throws VisitorException
	{
		return createFunctionCall("TZ", node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTMD5 node, Object data)
		throws VisitorException
	{
		return createFunctionCall("MD5", node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTSHA1 node, Object data)
		throws VisitorException
	{
		return createFunctionCall("SHA1", node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTSHA224 node, Object data)
		throws VisitorException
	{
		throw new VisitorException("hash function SHA-224 is currently not supported");
	}

	@Override
	public FunctionCall visit(ASTSHA256 node, Object data)
		throws VisitorException
	{
		return createFunctionCall("SHA256", node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTSHA384 node, Object data)
		throws VisitorException
	{
		return createFunctionCall("SHA384", node, 1, 1);
	}

	@Override
	public FunctionCall visit(ASTSHA512 node, Object data)
		throws VisitorException
	{
		return createFunctionCall("SHA512", node, 1, 1);
	}

	@Override
	public IRIFunction visit(ASTIRIFunc node, Object data)
		throws VisitorException
	{
		ValueExpr expr = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		return new IRIFunction(expr);
	}

	@Override
	public Lang visit(ASTLang node, Object data)
		throws VisitorException
	{
		ValueExpr arg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		return new Lang(arg);
	}

	@Override
	public Datatype visit(ASTDatatype node, Object data)
		throws VisitorException
	{
		ValueExpr arg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		return new Datatype(arg);
	}

	@Override
	public Object visit(ASTLangMatches node, Object data)
		throws VisitorException
	{
		ValueExpr leftArg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		ValueExpr rightArg = (ValueExpr)node.jjtGetChild(1).jjtAccept(this, null);
		return new LangMatches(leftArg, rightArg);
	}

	@Override
	public ValueExpr visit(ASTBound node, Object data)
		throws VisitorException
	{
		Var var = (Var)node.getArg().jjtAccept(this, null);
		return new Bound(var);
	}

	@Override
	public IsURI visit(ASTIsIRI node, Object data)
		throws VisitorException
	{
		ValueExpr arg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		return new IsURI(arg);
	}

	@Override
	public IsBNode visit(ASTIsBlank node, Object data)
		throws VisitorException
	{
		ValueExpr arg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		return new IsBNode(arg);
	}

	@Override
	public IsLiteral visit(ASTIsLiteral node, Object data)
		throws VisitorException
	{
		ValueExpr arg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		return new IsLiteral(arg);
	}

	@Override
	public IsNumeric visit(ASTIsNumeric node, Object data)
		throws VisitorException
	{
		ValueExpr arg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		return new IsNumeric(arg);
	}

	public Object visit(ASTBNodeFunc node, Object data)
		throws VisitorException
	{

		BNodeGenerator generator = new BNodeGenerator();

		if (node.jjtGetNumChildren() > 0) {
			ValueExpr nodeIdExpr = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
			generator.setNodeIdExpr(nodeIdExpr);
		}

		return generator;
	}

	@Override
	public Object visit(ASTRegexExpression node, Object data)
		throws VisitorException
	{
		ValueExpr arg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		ValueExpr pattern = (ValueExpr)node.jjtGetChild(1).jjtAccept(this, null);
		ValueExpr flags = null;
		if (node.jjtGetNumChildren() > 2) {
			flags = (ValueExpr)node.jjtGetChild(2).jjtAccept(this, null);
		}
		return new Regex(arg, pattern, flags);
	}

	@Override
	public Exists visit(ASTExistsFunc node, Object data)
		throws VisitorException
	{
		Exists e = new Exists();

		node.jjtGetChild(0).jjtAccept(this, e);
		return e;
	}

	@Override
	public Not visit(ASTNotExistsFunc node, Object data)
		throws VisitorException
	{
		Exists e = new Exists();
		node.jjtGetChild(0).jjtAccept(this, e);
		return new Not(e);
	}

	public If visit(ASTIf node, Object data)
		throws VisitorException
	{
		If result = null;

		if (node.jjtGetNumChildren() < 3) {
			throw new VisitorException("IF construction missing required number of arguments");
		}

		ValueExpr condition = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);
		ValueExpr resultExpr = (ValueExpr)node.jjtGetChild(1).jjtAccept(this, null);
		ValueExpr alternative = (ValueExpr)node.jjtGetChild(2).jjtAccept(this, null);

		result = new If(condition, resultExpr, alternative);

		return result;
	}

	public ValueExpr visit(ASTIn node, Object data)
		throws VisitorException
	{
		ValueExpr result = null;
		ValueExpr leftArg = (ValueExpr)node.jjtGetParent().jjtGetChild(0).jjtAccept(this, null);

		int listItemCount = node.jjtGetNumChildren();

		if (listItemCount == 0) {
			result = new ValueConstant(BooleanLiteralImpl.FALSE);
		}
		else if (listItemCount == 1) {
			ValueExpr arg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);

			result = new Compare(leftArg, arg, CompareOp.EQ);
		}
		else {
			// create a set of disjunctive comparisons to represent the IN
			// operator: X IN (a, b, c) -> X = a || X = b || X = c.
			Or or = new Or();
			Or currentOr = or;
			for (int i = 0; i < listItemCount - 1; i++) {
				ValueExpr arg = (ValueExpr)node.jjtGetChild(i).jjtAccept(this, null);

				currentOr.setLeftArg(new Compare(leftArg, arg, CompareOp.EQ));

				if (i == listItemCount - 2) { // second-to-last item
					arg = (ValueExpr)node.jjtGetChild(i + 1).jjtAccept(this, null);
					currentOr.setRightArg(new Compare(leftArg, arg, CompareOp.EQ));
				}
				else {
					Or newOr = new Or();
					currentOr.setRightArg(newOr);
					currentOr = newOr;
				}
			}
			result = or;
		}

		return result;
	}

	public ValueExpr visit(ASTNotIn node, Object data)
		throws VisitorException
	{
		ValueExpr result = null;
		ValueExpr leftArg = (ValueExpr)node.jjtGetParent().jjtGetChild(0).jjtAccept(this, null);

		int listItemCount = node.jjtGetNumChildren();

		if (listItemCount == 0) {
			result = new ValueConstant(BooleanLiteralImpl.TRUE);
		}
		else if (listItemCount == 1) {
			ValueExpr arg = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, null);

			result = new Compare(leftArg, arg, CompareOp.NE);
		}
		else {
			// create a set of conjunctive comparisons to represent the NOT IN
			// operator: X NOT IN (a, b, c) -> X != a && X != b && X != c.
			And and = new And();
			And currentAnd = and;
			for (int i = 0; i < listItemCount - 1; i++) {
				ValueExpr arg = (ValueExpr)node.jjtGetChild(i).jjtAccept(this, null);

				currentAnd.setLeftArg(new Compare(leftArg, arg, CompareOp.NE));

				if (i == listItemCount - 2) { // second-to-last item
					arg = (ValueExpr)node.jjtGetChild(i + 1).jjtAccept(this, null);
					currentAnd.setRightArg(new Compare(leftArg, arg, CompareOp.NE));
				}
				else {
					And newAnd = new And();
					currentAnd.setRightArg(newAnd);
					currentAnd = newAnd;
				}
			}
			result = and;
		}

		return result;
	}

	@Override
	public Var visit(ASTVar node, Object data)
		throws VisitorException
	{
		Var var = new Var(node.getName());
		var.setAnonymous(node.isAnonymous());
		return var;
	}

	@Override
	public ValueConstant visit(ASTIRI node, Object data)
		throws VisitorException
	{
		URI uri;
		try {
			uri = valueFactory.createURI(node.getValue());
		}
		catch (IllegalArgumentException e) {
			// invalid URI
			throw new VisitorException(e.getMessage());
		}

		return new ValueConstant(uri);
	}

	@Override
	public Object visit(ASTQName node, Object data)
		throws VisitorException
	{
		throw new VisitorException("QNames must be resolved before building the query model");
	}

	@Override
	public Object visit(ASTBind node, Object data)
		throws VisitorException
	{
		ValueExpr ve = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, data);

		Node aliasNode = node.jjtGetChild(1);
		String alias = ((ASTVar)aliasNode).getName();

		Extension extension = new Extension();
		extension.addElement(new ExtensionElem(ve, alias));

		graphPattern.addBindingAssignment(extension);

		return extension;
	}

	@Override
	public Object visit(ASTBlankNode node, Object data)
		throws VisitorException
	{
		throw new VisitorException(
				"Blank nodes must be replaced with variables before building the query model");
	}

	@Override
	public ValueConstant visit(ASTRDFLiteral node, Object data)
		throws VisitorException
	{
		String label = (String)node.getLabel().jjtAccept(this, null);
		String lang = node.getLang();
		ASTIRI datatypeNode = node.getDatatype();

		Literal literal;
		if (datatypeNode != null) {
			URI datatype;
			try {
				datatype = valueFactory.createURI(datatypeNode.getValue());
			}
			catch (IllegalArgumentException e) {
				// invalid URI
				throw new VisitorException(e.getMessage());
			}
			literal = valueFactory.createLiteral(label, datatype);
		}
		else if (lang != null) {
			literal = valueFactory.createLiteral(label, lang);
		}
		else {
			literal = valueFactory.createLiteral(label);
		}

		return new ValueConstant(literal);
	}

	@Override
	public ValueConstant visit(ASTNumericLiteral node, Object data)
		throws VisitorException
	{
		Literal literal = valueFactory.createLiteral(node.getValue(), node.getDatatype());
		return new ValueConstant(literal);
	}

	@Override
	public ValueConstant visit(ASTTrue node, Object data)
		throws VisitorException
	{
		return new ValueConstant(valueFactory.createLiteral(true));
	}

	@Override
	public ValueConstant visit(ASTFalse node, Object data)
		throws VisitorException
	{
		return new ValueConstant(valueFactory.createLiteral(false));
	}

	@Override
	public String visit(ASTString node, Object data)
		throws VisitorException
	{
		return node.getValue();
	}

	@Override
	public Object visit(ASTCount node, Object data)
		throws VisitorException
	{
		ValueExpr ve = null;
		if (node.jjtGetNumChildren() > 0) {
			ve = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, data);
		}
		return new Count(ve, node.isDistinct());
	}

	@Override
	public Object visit(ASTGroupConcat node, Object data)
		throws VisitorException
	{
		ValueExpr ve = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, data);

		GroupConcat gc = new GroupConcat(ve, node.isDistinct());
		
		if (node.jjtGetNumChildren() > 1) {
			ValueExpr separator = (ValueExpr)node.jjtGetChild(1).jjtAccept(this, data);
			gc.setSeparator(separator);
		}

		return gc;
	}

	@Override
	public Object visit(ASTMax node, Object data)
		throws VisitorException
	{
		ValueExpr ve = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, data);

		return new Max(ve, node.isDistinct());
	}

	@Override
	public Object visit(ASTMin node, Object data)
		throws VisitorException
	{
		ValueExpr ve = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, data);

		return new Min(ve, node.isDistinct());
	}

	@Override
	public Object visit(ASTSum node, Object data)
		throws VisitorException
	{
		ValueExpr ve = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, data);

		return new Sum(ve, node.isDistinct());
	}

	@Override
	public Object visit(ASTAvg node, Object data)
		throws VisitorException
	{
		ValueExpr ve = (ValueExpr)node.jjtGetChild(0).jjtAccept(this, data);

		return new Avg(ve, node.isDistinct());
	}

	static class AggregateCollector extends QueryModelVisitorBase<VisitorException> {

		private Collection<AggregateOperator> operators = new ArrayList<AggregateOperator>();

		public Collection<AggregateOperator> getOperators() {
			return operators;
		}

		@Override
		public void meet(Avg node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		@Override
		public void meet(Count node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		@Override
		public void meet(GroupConcat node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		@Override
		public void meet(Max node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		@Override
		public void meet(Min node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		@Override
		public void meet(Sample node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		@Override
		public void meet(Sum node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		private void meetAggregate(AggregateOperator node) {
			operators.add(node);
		}
	}

	static class AggregateOperatorReplacer extends QueryModelVisitorBase<VisitorException> {

		private Var replacement;

		private AggregateOperator operator;

		public AggregateOperatorReplacer(AggregateOperator operator, Var replacement) {
			this.operator = operator;
			this.replacement = replacement;
		}

		@Override
		public void meet(Avg node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		@Override
		public void meet(Count node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		@Override
		public void meet(GroupConcat node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		@Override
		public void meet(Max node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		@Override
		public void meet(Min node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		@Override
		public void meet(Sample node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		@Override
		public void meet(Sum node)
			throws VisitorException
		{
			super.meet(node);
			meetAggregate(node);
		}

		private void meetAggregate(AggregateOperator node) {
			if (node.equals(operator)) {
				node.getParentNode().replaceChildNode(node, replacement);
			}
		}
	}
}
