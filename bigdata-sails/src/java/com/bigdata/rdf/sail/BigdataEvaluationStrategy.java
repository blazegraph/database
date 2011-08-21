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
 * Created on Sep 16, 2009
 */
package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Properties;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;

/**
 * Extended interface.
 * 
 * @author <a href="mailto:mrpersoncik@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public interface BigdataEvaluationStrategy extends EvaluationStrategy {

    /**
     * Top-level method invoked for bigdata.
     * 
     * @param tupleExpr
     *            The {@link QueryRoot}.
     * @param bs
     *            The input {@link BindingSet}.
     * @param queryHints
     *            The {@link QueryHints}.
     * 
     * @return An iteration from which the solutions may be drained.
     * 
     * @throws QueryEvaluationException
     */
	CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			final TupleExpr tupleExpr, final BindingSet bs, 
			final Properties queryHints) throws QueryEvaluationException;
	
}
