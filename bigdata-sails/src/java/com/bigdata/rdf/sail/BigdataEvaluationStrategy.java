package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Properties;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;

public interface BigdataEvaluationStrategy extends EvaluationStrategy {

	CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			final TupleExpr tupleExpr, final BindingSet bs, 
			final Properties queryHints) throws QueryEvaluationException;
	
}
