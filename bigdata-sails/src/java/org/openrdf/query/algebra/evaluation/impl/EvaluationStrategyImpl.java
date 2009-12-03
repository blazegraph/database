/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.algebra.evaluation.impl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;
import info.aduna.iteration.DelayedIteration;
import info.aduna.iteration.DistinctIteration;
import info.aduna.iteration.EmptyIteration;
import info.aduna.iteration.FilterIteration;
import info.aduna.iteration.IntersectIteration;
import info.aduna.iteration.Iteration;
import info.aduna.iteration.LimitIteration;
import info.aduna.iteration.MinusIteration;
import info.aduna.iteration.OffsetIteration;
import info.aduna.iteration.SingletonIteration;
import info.aduna.iteration.UnionIteration;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.model.impl.DecimalLiteralImpl;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.model.impl.NumericLiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.BNodeGenerator;
import org.openrdf.query.algebra.BinaryTupleOperator;
import org.openrdf.query.algebra.Bound;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.CompareAll;
import org.openrdf.query.algebra.CompareAny;
import org.openrdf.query.algebra.Datatype;
import org.openrdf.query.algebra.Difference;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.EmptySet;
import org.openrdf.query.algebra.Exists;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.Group;
import org.openrdf.query.algebra.In;
import org.openrdf.query.algebra.Intersection;
import org.openrdf.query.algebra.IsBNode;
import org.openrdf.query.algebra.IsLiteral;
import org.openrdf.query.algebra.IsResource;
import org.openrdf.query.algebra.IsURI;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Label;
import org.openrdf.query.algebra.Lang;
import org.openrdf.query.algebra.LangMatches;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Like;
import org.openrdf.query.algebra.LocalName;
import org.openrdf.query.algebra.MathExpr;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Namespace;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.algebra.Or;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.Reduced;
import org.openrdf.query.algebra.Regex;
import org.openrdf.query.algebra.SameTerm;
import org.openrdf.query.algebra.SingletonSet;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Str;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.MathExpr.MathOp;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;
import org.openrdf.query.algebra.evaluation.function.FunctionRegistry;
import org.openrdf.query.algebra.evaluation.iterator.BadlyDesignedLeftJoinIterator;
import org.openrdf.query.algebra.evaluation.iterator.ExtensionIterator;
import org.openrdf.query.algebra.evaluation.iterator.FilterIterator;
import org.openrdf.query.algebra.evaluation.iterator.GroupIterator;
import org.openrdf.query.algebra.evaluation.iterator.JoinIterator;
import org.openrdf.query.algebra.evaluation.iterator.LeftJoinIterator;
import org.openrdf.query.algebra.evaluation.iterator.MultiProjectionIterator;
import org.openrdf.query.algebra.evaluation.iterator.OrderIterator;
import org.openrdf.query.algebra.evaluation.iterator.ProjectionIterator;
import org.openrdf.query.algebra.evaluation.util.OrderComparator;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.openrdf.query.algebra.evaluation.util.ValueComparator;

/**
 * Evaluates the TupleExpr and ValueExpr using Iterators and common tripleSource
 * API.
 * 
 * @author James Leigh
 * @author Arjohn Kampman
 * @author David Huynh
 */
public class EvaluationStrategyImpl implements EvaluationStrategy {

	/*-----------*
	 * Constants *
	 *-----------*/

	protected final TripleSource tripleSource;

	protected final Dataset dataset;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public EvaluationStrategyImpl(TripleSource tripleSource) {
		this(tripleSource, null);
	}

	public EvaluationStrategyImpl(TripleSource tripleSource, Dataset dataset) {
		this.tripleSource = tripleSource;
		this.dataset = dataset;
	}

	/*---------*
	 * Methods *
	 *---------*/

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr,
			BindingSet bindings)
		throws QueryEvaluationException
	{
		if (expr instanceof StatementPattern) {
			return evaluate((StatementPattern)expr, bindings);
		}
		else if (expr instanceof UnaryTupleOperator) {
			return evaluate((UnaryTupleOperator)expr, bindings);
		}
		else if (expr instanceof BinaryTupleOperator) {
			return evaluate((BinaryTupleOperator)expr, bindings);
		}
		else if (expr instanceof SingletonSet) {
			return evaluate((SingletonSet)expr, bindings);
		}
		else if (expr instanceof EmptySet) {
			return evaluate((EmptySet)expr, bindings);
		}
		else if (expr == null) {
			throw new IllegalArgumentException("expr must not be null");
		}
		else {
			throw new QueryEvaluationException("Unsupported tuple expr type: " + expr.getClass());
		}
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(StatementPattern sp,
			final BindingSet bindings)
		throws QueryEvaluationException
	{
		final Var subjVar = sp.getSubjectVar();
		final Var predVar = sp.getPredicateVar();
		final Var objVar = sp.getObjectVar();
		final Var conVar = sp.getContextVar();

		Value subjValue = getVarValue(subjVar, bindings);
		Value predValue = getVarValue(predVar, bindings);
		Value objValue = getVarValue(objVar, bindings);
		Value contextValue = getVarValue(conVar, bindings);

		CloseableIteration<? extends Statement, QueryEvaluationException> stIter = null;

		try {
			Resource[] contexts;

			if (dataset != null) {
				Set<URI> graphs;
				if (sp.getScope() == Scope.DEFAULT_CONTEXTS) {
					graphs = dataset.getDefaultGraphs();
				}
				else {
					graphs = dataset.getNamedGraphs();
				}

				if (graphs.isEmpty()) {
					// Search zero contexts
					return new EmptyIteration<BindingSet, QueryEvaluationException>();
				}
				else if (contextValue != null) {
					if (graphs.contains(contextValue)) {
						contexts = new Resource[] { (Resource)contextValue };
					}
					else {
						// Statement pattern specifies a context that is not part of
						// the dataset
						return new EmptyIteration<BindingSet, QueryEvaluationException>();
					}
				}
				else {
					contexts = graphs.toArray(new Resource[graphs.size()]);
				}
			}
			else if (contextValue != null) {
				contexts = new Resource[] { (Resource)contextValue };
			}
			else {
				contexts = new Resource[0];
			}

			stIter = tripleSource.getStatements((Resource)subjValue, (URI)predValue, objValue, contexts);

			if (contexts.length == 0 && sp.getScope() == Scope.NAMED_CONTEXTS) {
				// Named contexts are matched by retrieving all statements from
				// the store and filtering out the statements that do not have a
				// context.
				stIter = new FilterIteration<Statement, QueryEvaluationException>(stIter) {

					@Override
					protected boolean accept(Statement st) {
						return st.getContext() != null;
					}

				}; // end anonymous class
			}
		}
		catch (ClassCastException e) {
			// Invalid value type for subject, predicate and/or context
			return new EmptyIteration<BindingSet, QueryEvaluationException>();
		}

		// The same variable might have been used multiple times in this
		// StatementPattern, verify value equality in those cases.
		stIter = new FilterIteration<Statement, QueryEvaluationException>(stIter) {

			@Override
			protected boolean accept(Statement st) {
				Resource subj = st.getSubject();
				URI pred = st.getPredicate();
				Value obj = st.getObject();
				Resource context = st.getContext();

				if (subjVar != null) {
					if (subjVar.equals(predVar) && !subj.equals(pred)) {
						return false;
					}
					if (subjVar.equals(objVar) && !subj.equals(obj)) {
						return false;
					}
					if (subjVar.equals(conVar) && !subj.equals(context)) {
						return false;
					}
				}

				if (predVar != null) {
					if (predVar.equals(objVar) && !pred.equals(obj)) {
						return false;
					}
					if (predVar.equals(conVar) && !pred.equals(context)) {
						return false;
					}
				}

				if (objVar != null) {
					if (objVar.equals(conVar) && !obj.equals(context)) {
						return false;
					}
				}

				return true;
			}
		};

		// Return an iterator that converts the statements to var bindings
		return new ConvertingIteration<Statement, BindingSet, QueryEvaluationException>(stIter) {

			@Override
			protected BindingSet convert(Statement st) {
				QueryBindingSet result = new QueryBindingSet(bindings);

				if (subjVar != null && !result.hasBinding(subjVar.getName())) {
					result.addBinding(subjVar.getName(), st.getSubject());
				}
				if (predVar != null && !result.hasBinding(predVar.getName())) {
					result.addBinding(predVar.getName(), st.getPredicate());
				}
				if (objVar != null && !result.hasBinding(objVar.getName())) {
					result.addBinding(objVar.getName(), st.getObject());
				}
				if (conVar != null && !result.hasBinding(conVar.getName()) && st.getContext() != null) {
					result.addBinding(conVar.getName(), st.getContext());
				}

				return result;
			}
		};
	}

	protected Value getVarValue(Var var, BindingSet bindings) {
		if (var == null) {
			return null;
		}
		else if (var.hasValue()) {
			return var.getValue();
		}
		else {
			return bindings.getValue(var.getName());
		}
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(UnaryTupleOperator expr,
			BindingSet bindings)
		throws QueryEvaluationException
	{
		if (expr instanceof Projection) {
			return evaluate((Projection)expr, bindings);
		}
		else if (expr instanceof MultiProjection) {
			return evaluate((MultiProjection)expr, bindings);
		}
		else if (expr instanceof Filter) {
			return evaluate((Filter)expr, bindings);
		}
		else if (expr instanceof Slice) {
			return evaluate((Slice)expr, bindings);
		}
		else if (expr instanceof Extension) {
			return evaluate((Extension)expr, bindings);
		}
		else if (expr instanceof Distinct) {
			return evaluate((Distinct)expr, bindings);
		}
		else if (expr instanceof Reduced) {
			return evaluate((Reduced)expr, bindings);
		}
		else if (expr instanceof Group) {
			return evaluate((Group)expr, bindings);
		}
		else if (expr instanceof Order) {
			return evaluate((Order)expr, bindings);
		}
		else if (expr instanceof QueryRoot) {
			return evaluate(((QueryRoot)expr).getArg(), bindings);
		}
		else if (expr == null) {
			throw new IllegalArgumentException("expr must not be null");
		}
		else {
			throw new QueryEvaluationException("Unknown unary tuple operator type: " + expr.getClass());
		}
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Projection projection,
			BindingSet bindings)
		throws QueryEvaluationException
	{
		CloseableIteration<BindingSet, QueryEvaluationException> result;
		result = this.evaluate(projection.getArg(), bindings);
		result = new ProjectionIterator(projection, result, bindings);
		return result;
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(MultiProjection multiProjection,
			BindingSet bindings)
		throws QueryEvaluationException
	{
		CloseableIteration<BindingSet, QueryEvaluationException> result;
		result = this.evaluate(multiProjection.getArg(), bindings);
		result = new MultiProjectionIterator(multiProjection, result, bindings);
		return result;
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Filter filter, BindingSet bindings)
		throws QueryEvaluationException
	{
		CloseableIteration<BindingSet, QueryEvaluationException> result;
		result = this.evaluate(filter.getArg(), bindings);
		result = new FilterIterator(filter, result, this);
		return result;
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Slice slice, BindingSet bindings)
		throws QueryEvaluationException
	{
		CloseableIteration<BindingSet, QueryEvaluationException> result = evaluate(slice.getArg(), bindings);

		if (slice.hasOffset()) {
			result = new OffsetIteration<BindingSet, QueryEvaluationException>(result, slice.getOffset());
		}

		if (slice.hasLimit()) {
			result = new LimitIteration<BindingSet, QueryEvaluationException>(result, slice.getLimit());
		}

		return result;
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Extension extension,
			BindingSet bindings)
		throws QueryEvaluationException
	{
		CloseableIteration<BindingSet, QueryEvaluationException> result;
		result = this.evaluate(extension.getArg(), bindings);
		result = new ExtensionIterator(extension, result, this);
		return result;
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Distinct distinct,
			BindingSet bindings)
		throws QueryEvaluationException
	{
		return new DistinctIteration<BindingSet, QueryEvaluationException>(
				evaluate(distinct.getArg(), bindings));
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Reduced reduced,
			BindingSet bindings)
		throws QueryEvaluationException
	{
		return evaluate(reduced.getArg(), bindings);
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Group node, BindingSet bindings)
		throws QueryEvaluationException
	{
		return new GroupIterator(this, node, bindings);
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Order node, BindingSet bindings)
		throws QueryEvaluationException
	{
		ValueComparator vcmp = new ValueComparator();
		OrderComparator cmp = new OrderComparator(this, node, vcmp);
		return new OrderIterator(evaluate(node.getArg(), bindings), cmp);
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BinaryTupleOperator expr,
			BindingSet bindings)
		throws QueryEvaluationException
	{
		if (expr instanceof Join) {
			return evaluate((Join)expr, bindings);
		}
		else if (expr instanceof LeftJoin) {
			return evaluate((LeftJoin)expr, bindings);
		}
		else if (expr instanceof Union) {
			return evaluate((Union)expr, bindings);
		}
		else if (expr instanceof Intersection) {
			return evaluate((Intersection)expr, bindings);
		}
		else if (expr instanceof Difference) {
			return evaluate((Difference)expr, bindings);
		}
		else if (expr == null) {
			throw new IllegalArgumentException("expr must not be null");
		}
		else {
			throw new QueryEvaluationException("Unsupported binary tuple operator type: " + expr.getClass());
		}
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Join join, BindingSet bindings)
		throws QueryEvaluationException
	{
		return new JoinIterator(this, join, bindings);
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(LeftJoin leftJoin,
			final BindingSet bindings)
		throws QueryEvaluationException
	{
		// Check whether optional join is "well designed" as defined in section
		// 4.2 of "Semantics and Complexity of SPARQL", 2006, Jorge P�rez et al.
		Set<String> boundVars = bindings.getBindingNames();
		Set<String> leftVars = leftJoin.getLeftArg().getBindingNames();
		Set<String> optionalVars = leftJoin.getRightArg().getBindingNames();

		final Set<String> problemVars = new HashSet<String>(boundVars);
		problemVars.retainAll(optionalVars);
		problemVars.removeAll(leftVars);

		if (problemVars.isEmpty()) {
			// left join is "well designed"
			return new LeftJoinIterator(this, leftJoin, bindings);
		}
		else {
			return new BadlyDesignedLeftJoinIterator(this, leftJoin, bindings, problemVars);
		}
	}

	@SuppressWarnings("unchecked")
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final Union union,
			final BindingSet bindings)
		throws QueryEvaluationException
	{
		Iteration<BindingSet, QueryEvaluationException> leftArg, rightArg;

		leftArg = new DelayedIteration<BindingSet, QueryEvaluationException>() {

			@Override
			protected Iteration<BindingSet, QueryEvaluationException> createIteration()
				throws QueryEvaluationException
			{
				return evaluate(union.getLeftArg(), bindings);
			}
		};

		rightArg = new DelayedIteration<BindingSet, QueryEvaluationException>() {

			@Override
			protected Iteration<BindingSet, QueryEvaluationException> createIteration()
				throws QueryEvaluationException
			{
				return evaluate(union.getRightArg(), bindings);
			}
		};

		return new UnionIteration<BindingSet, QueryEvaluationException>(leftArg, rightArg);
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final Intersection intersection,
			final BindingSet bindings)
		throws QueryEvaluationException
	{
		Iteration<BindingSet, QueryEvaluationException> leftArg, rightArg;

		leftArg = new DelayedIteration<BindingSet, QueryEvaluationException>() {

			@Override
			protected Iteration<BindingSet, QueryEvaluationException> createIteration()
				throws QueryEvaluationException
			{
				return evaluate(intersection.getLeftArg(), bindings);
			}
		};

		rightArg = new DelayedIteration<BindingSet, QueryEvaluationException>() {

			@Override
			protected Iteration<BindingSet, QueryEvaluationException> createIteration()
				throws QueryEvaluationException
			{
				return evaluate(intersection.getRightArg(), bindings);
			}
		};

		return new IntersectIteration<BindingSet, QueryEvaluationException>(leftArg, rightArg);
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final Difference difference,
			final BindingSet bindings)
		throws QueryEvaluationException
	{
		Iteration<BindingSet, QueryEvaluationException> leftArg, rightArg;

		leftArg = new DelayedIteration<BindingSet, QueryEvaluationException>() {

			@Override
			protected Iteration<BindingSet, QueryEvaluationException> createIteration()
				throws QueryEvaluationException
			{
				return evaluate(difference.getLeftArg(), bindings);
			}
		};

		rightArg = new DelayedIteration<BindingSet, QueryEvaluationException>() {

			@Override
			protected Iteration<BindingSet, QueryEvaluationException> createIteration()
				throws QueryEvaluationException
			{
				return evaluate(difference.getRightArg(), bindings);
			}
		};

		return new MinusIteration<BindingSet, QueryEvaluationException>(leftArg, rightArg);
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(SingletonSet singletonSet,
			BindingSet bindings)
		throws QueryEvaluationException
	{
		return new SingletonIteration<BindingSet, QueryEvaluationException>(bindings);
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(EmptySet emptySet,
			BindingSet bindings)
		throws QueryEvaluationException
	{
		return new EmptyIteration<BindingSet, QueryEvaluationException>();
	}

	public Value evaluate(ValueExpr expr, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		if (expr instanceof Var) {
			return evaluate((Var)expr, bindings);
		}
		else if (expr instanceof ValueConstant) {
			return evaluate((ValueConstant)expr, bindings);
		}
		else if (expr instanceof BNodeGenerator) {
			return evaluate((BNodeGenerator)expr, bindings);
		}
		else if (expr instanceof Bound) {
			return evaluate((Bound)expr, bindings);
		}
		else if (expr instanceof Str) {
			return evaluate((Str)expr, bindings);
		}
		else if (expr instanceof Label) {
			return evaluate((Label)expr, bindings);
		}
		else if (expr instanceof Lang) {
			return evaluate((Lang)expr, bindings);
		}
		else if (expr instanceof LangMatches) {
			return evaluate((LangMatches)expr, bindings);
		}
		else if (expr instanceof Datatype) {
			return evaluate((Datatype)expr, bindings);
		}
		else if (expr instanceof Namespace) {
			return evaluate((Namespace)expr, bindings);
		}
		else if (expr instanceof LocalName) {
			return evaluate((LocalName)expr, bindings);
		}
		else if (expr instanceof IsResource) {
			return evaluate((IsResource)expr, bindings);
		}
		else if (expr instanceof IsURI) {
			return evaluate((IsURI)expr, bindings);
		}
		else if (expr instanceof IsBNode) {
			return evaluate((IsBNode)expr, bindings);
		}
		else if (expr instanceof IsLiteral) {
			return evaluate((IsLiteral)expr, bindings);
		}
		else if (expr instanceof Regex) {
			return evaluate((Regex)expr, bindings);
		}
		else if (expr instanceof Like) {
			return evaluate((Like)expr, bindings);
		}
		else if (expr instanceof FunctionCall) {
			return evaluate((FunctionCall)expr, bindings);
		}
		else if (expr instanceof And) {
			return evaluate((And)expr, bindings);
		}
		else if (expr instanceof Or) {
			return evaluate((Or)expr, bindings);
		}
		else if (expr instanceof Not) {
			return evaluate((Not)expr, bindings);
		}
		else if (expr instanceof SameTerm) {
			return evaluate((SameTerm)expr, bindings);
		}
		else if (expr instanceof Compare) {
			return evaluate((Compare)expr, bindings);
		}
		else if (expr instanceof MathExpr) {
			return evaluate((MathExpr)expr, bindings);
		}
		else if (expr instanceof In) {
			return evaluate((In)expr, bindings);
		}
		else if (expr instanceof CompareAny) {
			return evaluate((CompareAny)expr, bindings);
		}
		else if (expr instanceof CompareAll) {
			return evaluate((CompareAll)expr, bindings);
		}
		else if (expr instanceof Exists) {
			return evaluate((Exists)expr, bindings);
		}
		else if (expr == null) {
			throw new IllegalArgumentException("expr must not be null");
		}
		else {
			throw new QueryEvaluationException("Unsupported value expr type: " + expr.getClass());
		}
	}

	public Value evaluate(Var var, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value value = var.getValue();

		if (value == null) {
			value = bindings.getValue(var.getName());
		}

		if (value == null) {
			throw new ValueExprEvaluationException();
		}

		return value;
	}

	public Value evaluate(ValueConstant valueConstant, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		return valueConstant.getValue();
	}

	public Value evaluate(BNodeGenerator node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		return tripleSource.getValueFactory().createBNode();
	}

	public Value evaluate(Bound node, BindingSet bindings)
		throws QueryEvaluationException
	{
		try {
			Value argValue = evaluate(node.getArg(), bindings);
			return BooleanLiteralImpl.valueOf(argValue != null);
		}
		catch (ValueExprEvaluationException e) {
			return BooleanLiteralImpl.FALSE;
		}
	}

	public Value evaluate(Str node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value argValue = evaluate(node.getArg(), bindings);

		if (argValue instanceof URI) {
			return tripleSource.getValueFactory().createLiteral(argValue.toString());
		}
		else if (argValue instanceof Literal) {
			Literal literal = (Literal)argValue;

			if (QueryEvaluationUtil.isSimpleLiteral(literal)) {
				return literal;
			}
			else {
				return tripleSource.getValueFactory().createLiteral(literal.getLabel());
			}
		}
		else {
			throw new ValueExprEvaluationException();
		}
	}

	public Value evaluate(Label node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		// FIXME: deprecate Label in favour of Str(?)
		Value argValue = evaluate(node.getArg(), bindings);

		if (argValue instanceof Literal) {
			Literal literal = (Literal)argValue;

			if (QueryEvaluationUtil.isSimpleLiteral(literal)) {
				return literal;
			}
			else {
				return tripleSource.getValueFactory().createLiteral(literal.getLabel());
			}
		}
		else {
			throw new ValueExprEvaluationException();
		}
	}

	public Value evaluate(Lang node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value argValue = evaluate(node.getArg(), bindings);

		if (argValue instanceof Literal) {
			Literal literal = (Literal)argValue;

			String langTag = literal.getLanguage();
			if (langTag == null) {
				langTag = "";
			}

			return tripleSource.getValueFactory().createLiteral(langTag);
		}

		throw new ValueExprEvaluationException();
	}

	public Value evaluate(Datatype node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value v = evaluate(node.getArg(), bindings);

		if (v instanceof Literal) {
			Literal literal = (Literal)v;

			if (literal.getDatatype() != null) {
				// literal with datatype
				return literal.getDatatype();
			}
			else if (literal.getLanguage() == null) {
				// simple literal
				return XMLSchema.STRING;
			}
		}

		throw new ValueExprEvaluationException();
	}

	public Value evaluate(Namespace node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value argValue = evaluate(node.getArg(), bindings);

		if (argValue instanceof URI) {
			URI uri = (URI)argValue;
			return tripleSource.getValueFactory().createURI(uri.getNamespace());
		}
		else {
			throw new ValueExprEvaluationException();
		}
	}

	public Value evaluate(LocalName node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value argValue = evaluate(node.getArg(), bindings);

		if (argValue instanceof URI) {
			URI uri = (URI)argValue;
			return tripleSource.getValueFactory().createLiteral(uri.getLocalName());
		}
		else {
			throw new ValueExprEvaluationException();
		}
	}

	/**
	 * Determines whether the operand (a variable) contains a Resource.
	 * 
	 * @return <tt>true</tt> if the operand contains a Resource, <tt>false</tt>
	 *         otherwise.
	 */
	public Value evaluate(IsResource node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value argValue = evaluate(node.getArg(), bindings);
		return BooleanLiteralImpl.valueOf(argValue instanceof Resource);
	}

	/**
	 * Determines whether the operand (a variable) contains a URI.
	 * 
	 * @return <tt>true</tt> if the operand contains a URI, <tt>false</tt>
	 *         otherwise.
	 */
	public Value evaluate(IsURI node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value argValue = evaluate(node.getArg(), bindings);
		return BooleanLiteralImpl.valueOf(argValue instanceof URI);
	}

	/**
	 * Determines whether the operand (a variable) contains a BNode.
	 * 
	 * @return <tt>true</tt> if the operand contains a BNode, <tt>false</tt>
	 *         otherwise.
	 */
	public Value evaluate(IsBNode node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value argValue = evaluate(node.getArg(), bindings);
		return BooleanLiteralImpl.valueOf(argValue instanceof BNode);
	}

	/**
	 * Determines whether the operand (a variable) contains a Literal.
	 * 
	 * @return <tt>true</tt> if the operand contains a Literal, <tt>false</tt>
	 *         otherwise.
	 */
	public Value evaluate(IsLiteral node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value argValue = evaluate(node.getArg(), bindings);
		return BooleanLiteralImpl.valueOf(argValue instanceof Literal);
	}

	/**
	 * Determines whether the two operands match according to the
	 * <code>regex</code> operator.
	 * 
	 * @return <tt>true</tt> if the operands match according to the
	 *         <tt>regex</tt> operator, <tt>false</tt> otherwise.
	 */
	public Value evaluate(Regex node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value arg = evaluate(node.getArg(), bindings);
		Value parg = evaluate(node.getPatternArg(), bindings);
		Value farg = null;
		ValueExpr flagsArg = node.getFlagsArg();
		if (flagsArg != null) {
			farg = evaluate(flagsArg, bindings);
		}

		if (QueryEvaluationUtil.isSimpleLiteral(arg) && QueryEvaluationUtil.isSimpleLiteral(parg)
				&& (farg == null || QueryEvaluationUtil.isSimpleLiteral(farg)))
		{
			String text = ((Literal)arg).getLabel();
			String ptn = ((Literal)parg).getLabel();
			String flags = "";
			if (farg != null) {
				flags = ((Literal)farg).getLabel();
			}
			// TODO should this Pattern be cached?
			int f = 0;
			for (char c : flags.toCharArray()) {
				switch (c) {
					case 's':
						f |= Pattern.DOTALL;
						break;
					case 'm':
						f |= Pattern.MULTILINE;
						break;
					case 'i':
						f |= Pattern.CASE_INSENSITIVE;
						break;
					case 'x':
						f |= Pattern.COMMENTS;
						break;
					case 'd':
						f |= Pattern.UNIX_LINES;
						break;
					case 'u':
						f |= Pattern.UNICODE_CASE;
						break;
					default:
						throw new ValueExprEvaluationException(flags);
				}
			}
			Pattern pattern = Pattern.compile(ptn, f);
			boolean result = pattern.matcher(text).find();
			return BooleanLiteralImpl.valueOf(result);
		}

		throw new ValueExprEvaluationException();
	}

	public Value evaluate(LangMatches node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value langTagValue = evaluate(node.getLeftArg(), bindings);
		Value langRangeValue = evaluate(node.getRightArg(), bindings);

		if (QueryEvaluationUtil.isSimpleLiteral(langTagValue)
				&& QueryEvaluationUtil.isSimpleLiteral(langRangeValue))
		{
			String langTag = ((Literal)langTagValue).getLabel();
			String langRange = ((Literal)langRangeValue).getLabel();

			boolean result = false;
			if (langRange.equals("*")) {
				result = langTag.length() > 0;
			}
			else if (langTag.length() == langRange.length()) {
				result = langTag.equalsIgnoreCase(langRange);
			}
			else if (langTag.length() > langRange.length()) {
				// check if the range is a prefix of the tag
				String prefix = langTag.substring(0, langRange.length());
				result = prefix.equalsIgnoreCase(langRange) && langTag.charAt(langRange.length()) == '-';
			}

			return BooleanLiteralImpl.valueOf(result);
		}

		throw new ValueExprEvaluationException();

	}

	/**
	 * Determines whether the two operands match according to the
	 * <code>like</code> operator. The operator is defined as a string comparison
	 * with the possible use of an asterisk (*) at the end and/or the start of
	 * the second operand to indicate substring matching.
	 * 
	 * @return <tt>true</tt> if the operands match according to the <tt>like</tt>
	 *         operator, <tt>false</tt> otherwise.
	 */
	public Value evaluate(Like node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value val = evaluate(node.getArg(), bindings);
		String strVal = null;

		if (val instanceof URI) {
			strVal = ((URI)val).toString();
		}
		else if (val instanceof Literal) {
			strVal = ((Literal)val).getLabel();
		}

		if (strVal == null) {
			throw new ValueExprEvaluationException();
		}

		if (!node.isCaseSensitive()) {
			// Convert strVal to lower case, just like the pattern has been done
			strVal = strVal.toLowerCase();
		}

		int valIndex = 0;
		int prevPatternIndex = -1;
		int patternIndex = node.getOpPattern().indexOf('*');

		if (patternIndex == -1) {
			// No wildcards
			return BooleanLiteralImpl.valueOf(node.getOpPattern().equals(strVal));
		}

		String snippet;

		if (patternIndex > 0) {
			// Pattern does not start with a wildcard, first part must match
			snippet = node.getOpPattern().substring(0, patternIndex);
			if (!strVal.startsWith(snippet)) {
				return BooleanLiteralImpl.FALSE;
			}

			valIndex += snippet.length();
			prevPatternIndex = patternIndex;
			patternIndex = node.getOpPattern().indexOf('*', patternIndex + 1);
		}

		while (patternIndex != -1) {
			// Get snippet between previous wildcard and this wildcard
			snippet = node.getOpPattern().substring(prevPatternIndex + 1, patternIndex);

			// Search for the snippet in the value
			valIndex = strVal.indexOf(snippet, valIndex);
			if (valIndex == -1) {
				return BooleanLiteralImpl.FALSE;
			}

			valIndex += snippet.length();
			prevPatternIndex = patternIndex;
			patternIndex = node.getOpPattern().indexOf('*', patternIndex + 1);
		}

		// Part after last wildcard
		snippet = node.getOpPattern().substring(prevPatternIndex + 1);

		if (snippet.length() > 0) {
			// Pattern does not end with a wildcard.

			// Search last occurence of the snippet.
			valIndex = strVal.indexOf(snippet, valIndex);
			int i;
			while ((i = strVal.indexOf(snippet, valIndex + 1)) != -1) {
				// A later occurence was found.
				valIndex = i;
			}

			if (valIndex == -1) {
				return BooleanLiteralImpl.FALSE;
			}

			valIndex += snippet.length();

			if (valIndex < strVal.length()) {
				// Some characters were not matched
				return BooleanLiteralImpl.FALSE;
			}
		}

		return BooleanLiteralImpl.TRUE;
	}

	/**
	 * Evaluates a function.
	 */
	public Value evaluate(FunctionCall node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Function function = FunctionRegistry.getInstance().get(node.getURI());

		if (function == null) {
			throw new QueryEvaluationException("Unknown function '" + node.getURI() + "'");
		}

		List<ValueExpr> args = node.getArgs();

		Value[] argValues = new Value[args.size()];

		for (int i = 0; i < args.size(); i++) {
			argValues[i] = evaluate(args.get(i), bindings);
		}

		return function.evaluate(tripleSource.getValueFactory(), argValues);
	}

	public Value evaluate(And node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		try {
			Value leftValue = evaluate(node.getLeftArg(), bindings);
			if (QueryEvaluationUtil.getEffectiveBooleanValue(leftValue) == false) {
				// Left argument evaluates to false, we don't need to look any
				// further
				return BooleanLiteralImpl.FALSE;
			}
		}
		catch (ValueExprEvaluationException e) {
			// Failed to evaluate the left argument. Result is 'false' when
			// the right argument evaluates to 'false', failure otherwise.
			Value rightValue = evaluate(node.getRightArg(), bindings);
			if (QueryEvaluationUtil.getEffectiveBooleanValue(rightValue) == false) {
				return BooleanLiteralImpl.FALSE;
			}
			else {
				throw new ValueExprEvaluationException();
			}
		}

		// Left argument evaluated to 'true', result is determined
		// by the evaluation of the right argument.
		Value rightValue = evaluate(node.getRightArg(), bindings);
		return BooleanLiteralImpl.valueOf(QueryEvaluationUtil.getEffectiveBooleanValue(rightValue));
	}

	public Value evaluate(Or node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		try {
			Value leftValue = evaluate(node.getLeftArg(), bindings);
			if (QueryEvaluationUtil.getEffectiveBooleanValue(leftValue) == true) {
				// Left argument evaluates to true, we don't need to look any
				// further
				return BooleanLiteralImpl.TRUE;
			}
		}
		catch (ValueExprEvaluationException e) {
			// Failed to evaluate the left argument. Result is 'true' when
			// the right argument evaluates to 'true', failure otherwise.
			Value rightValue = evaluate(node.getRightArg(), bindings);
			if (QueryEvaluationUtil.getEffectiveBooleanValue(rightValue) == true) {
				return BooleanLiteralImpl.TRUE;
			}
			else {
				throw new ValueExprEvaluationException();
			}
		}

		// Left argument evaluated to 'false', result is determined
		// by the evaluation of the right argument.
		Value rightValue = evaluate(node.getRightArg(), bindings);
		return BooleanLiteralImpl.valueOf(QueryEvaluationUtil.getEffectiveBooleanValue(rightValue));
	}

	public Value evaluate(Not node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value argValue = evaluate(node.getArg(), bindings);
		boolean argBoolean = QueryEvaluationUtil.getEffectiveBooleanValue(argValue);
		return BooleanLiteralImpl.valueOf(!argBoolean);
	}

	public Value evaluate(SameTerm node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value leftVal = evaluate(node.getLeftArg(), bindings);
		Value rightVal = evaluate(node.getRightArg(), bindings);

		return BooleanLiteralImpl.valueOf(leftVal != null && leftVal.equals(rightVal));
	}

	public Value evaluate(Compare node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value leftVal = evaluate(node.getLeftArg(), bindings);
		Value rightVal = evaluate(node.getRightArg(), bindings);

		return BooleanLiteralImpl.valueOf(QueryEvaluationUtil.compare(leftVal, rightVal, node.getOperator()));
	}

	public Value evaluate(MathExpr node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		// Do the math
		Value leftVal = evaluate(node.getLeftArg(), bindings);
		Value rightVal = evaluate(node.getRightArg(), bindings);

		if (leftVal instanceof Literal && rightVal instanceof Literal) {
			return getValue((Literal)leftVal, (Literal)rightVal, node.getOperator());
		}

		return null;
	}

	public Value evaluate(In node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value leftValue = evaluate(node.getArg(), bindings);

		// Result is false until a match has been found
		boolean result = false;

		// Use first binding name from tuple expr to compare values
		String bindingName = node.getSubQuery().getBindingNames().iterator().next();

		CloseableIteration<BindingSet, QueryEvaluationException> iter = evaluate(node.getSubQuery(), bindings);
		try {
			while (result == false && iter.hasNext()) {
				BindingSet bindingSet = iter.next();

				Value rightValue = bindingSet.getValue(bindingName);

				result = leftValue == null && rightValue == null || leftValue != null
						&& leftValue.equals(rightValue);
			}
		}
		finally {
			iter.close();
		}

		return BooleanLiteralImpl.valueOf(result);
	}

	public Value evaluate(CompareAny node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value leftValue = evaluate(node.getArg(), bindings);

		// Result is false until a match has been found
		boolean result = false;

		// Use first binding name from tuple expr to compare values
		String bindingName = node.getSubQuery().getBindingNames().iterator().next();

		CloseableIteration<BindingSet, QueryEvaluationException> iter = evaluate(node.getSubQuery(), bindings);
		try {
			while (result == false && iter.hasNext()) {
				BindingSet bindingSet = iter.next();

				Value rightValue = bindingSet.getValue(bindingName);

				try {
					result = QueryEvaluationUtil.compare(leftValue, rightValue, node.getOperator());
				}
				catch (ValueExprEvaluationException e) {
					// ignore, maybe next value will match
				}
			}
		}
		finally {
			iter.close();
		}

		return BooleanLiteralImpl.valueOf(result);
	}

	public Value evaluate(CompareAll node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		Value leftValue = evaluate(node.getArg(), bindings);

		// Result is true until a mismatch has been found
		boolean result = true;

		// Use first binding name from tuple expr to compare values
		String bindingName = node.getSubQuery().getBindingNames().iterator().next();

		CloseableIteration<BindingSet, QueryEvaluationException> iter = evaluate(node.getSubQuery(), bindings);
		try {
			while (result == true && iter.hasNext()) {
				BindingSet bindingSet = iter.next();

				Value rightValue = bindingSet.getValue(bindingName);

				try {
					result = QueryEvaluationUtil.compare(leftValue, rightValue, node.getOperator());
				}
				catch (ValueExprEvaluationException e) {
					// Exception thrown by ValueCompare.isTrue(...)
					result = false;
				}
			}
		}
		finally {
			iter.close();
		}

		return BooleanLiteralImpl.valueOf(result);
	}

	public Value evaluate(Exists node, BindingSet bindings)
		throws ValueExprEvaluationException, QueryEvaluationException
	{
		CloseableIteration<BindingSet, QueryEvaluationException> iter = evaluate(node.getSubQuery(), bindings);
		try {
			return BooleanLiteralImpl.valueOf(iter.hasNext());
		}
		finally {
			iter.close();
		}
	}

	public boolean isTrue(ValueExpr expr, BindingSet bindings)
		throws QueryEvaluationException
	{
		try {
			Value value = evaluate(expr, bindings);
			return QueryEvaluationUtil.getEffectiveBooleanValue(value);
		}
		catch (ValueExprEvaluationException e) {
			return false;
		}
	}

	private static Literal getValue(Literal leftLit, Literal rightLit, MathOp op) {
		URI leftDatatype = leftLit.getDatatype();
		URI rightDatatype = rightLit.getDatatype();

		// Only numeric value can be used in math expressions
		if (leftDatatype != null && rightDatatype != null && XMLDatatypeUtil.isNumericDatatype(leftDatatype)
				&& XMLDatatypeUtil.isNumericDatatype(rightDatatype))
		{
			// Determine most specific datatype that the arguments have in common,
			// choosing from xsd:integer, xsd:decimal, xsd:float and xsd:double as
			// per the SPARQL/XPATH spec
			URI commonDatatype;

			if (leftDatatype.equals(XMLSchema.DOUBLE) || rightDatatype.equals(XMLSchema.DOUBLE)) {
				commonDatatype = XMLSchema.DOUBLE;
			}
			else if (leftDatatype.equals(XMLSchema.FLOAT) || rightDatatype.equals(XMLSchema.FLOAT)) {
				commonDatatype = XMLSchema.FLOAT;
			}
			else if (leftDatatype.equals(XMLSchema.DECIMAL) || rightDatatype.equals(XMLSchema.DECIMAL)) {
				commonDatatype = XMLSchema.DECIMAL;
			}
			else if (op == MathOp.DIVIDE) {
				// Result of integer divide is decimal and requires the arguments to
				// be handled as such, see for details:
				// http://www.w3.org/TR/xpath-functions/#func-numeric-divide
				commonDatatype = XMLSchema.DECIMAL;
			}
			else {
				commonDatatype = XMLSchema.INTEGER;
			}

			// Note: Java already handles cases like divide-by-zero appropriately
			// for floats and doubles, see:
			// http://www.particle.kth.se/~lindsey/JavaCourse/Book/Part1/Tech/
			// Chapter02/floatingPt2.html

			try {
				if (commonDatatype.equals(XMLSchema.DOUBLE)) {
					double left = leftLit.doubleValue();
					double right = rightLit.doubleValue();

					switch (op) {
						case PLUS:
							return new NumericLiteralImpl(left + right);
						case MINUS:
							return new NumericLiteralImpl(left - right);
						case MULTIPLY:
							return new NumericLiteralImpl(left * right);
						case DIVIDE:
							return new NumericLiteralImpl(left / right);
						default:
							throw new IllegalArgumentException("Unknown operator: " + op);
					}
				}
				else if (commonDatatype.equals(XMLSchema.FLOAT)) {
					float left = leftLit.floatValue();
					float right = rightLit.floatValue();

					switch (op) {
						case PLUS:
							return new NumericLiteralImpl(left + right);
						case MINUS:
							return new NumericLiteralImpl(left - right);
						case MULTIPLY:
							return new NumericLiteralImpl(left * right);
						case DIVIDE:
							return new NumericLiteralImpl(left / right);
						default:
							throw new IllegalArgumentException("Unknown operator: " + op);
					}
				}
				else if (commonDatatype.equals(XMLSchema.DECIMAL)) {
					BigDecimal left = leftLit.decimalValue();
					BigDecimal right = rightLit.decimalValue();

					switch (op) {
						case PLUS:
							return new DecimalLiteralImpl(left.add(right));
						case MINUS:
							return new DecimalLiteralImpl(left.subtract(right));
						case MULTIPLY:
							return new DecimalLiteralImpl(left.multiply(right));
						case DIVIDE:
							// Divide by zero handled through NumberFormatException
							return new DecimalLiteralImpl(left.divide(right, RoundingMode.HALF_UP));
						default:
							throw new IllegalArgumentException("Unknown operator: " + op);
					}
				}
				else { // XMLSchema.INTEGER
					BigInteger left = leftLit.integerValue();
					BigInteger right = rightLit.integerValue();

					switch (op) {
						case PLUS:
							return new IntegerLiteralImpl(left.add(right));
						case MINUS:
							return new IntegerLiteralImpl(left.subtract(right));
						case MULTIPLY:
							return new IntegerLiteralImpl(left.multiply(right));
						case DIVIDE:
							throw new RuntimeException("Integer divisions should be processed as decimal divisions");
						default:
							throw new IllegalArgumentException("Unknown operator: " + op);
					}
				}
			}
			catch (NumberFormatException e) {
				return null;
			}
			catch (ArithmeticException e) {
				return null;
			}
		}

		return null;
	}
}
