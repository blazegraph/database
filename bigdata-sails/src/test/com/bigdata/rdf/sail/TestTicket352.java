/**
Copyright (C) SYSTAP, LLC 2011.  All rights reserved.

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

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;

/**
 * Unit test template for use in submission of bugs.
 * <p>
 * This test case will delegate to an underlying backing store. You can specify
 * this store via a JVM property as follows:
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithQuads</code>
 * <p>
 * There are three possible configurations for the testClass:
 * <ul>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithQuads (quads mode)</li>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithoutSids (triples mode)</li>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithSids (SIDs mode)</li>
 * </ul>
 * <p>
 * The default for triples and SIDs mode is for inference with truth maintenance
 * to be on. If you would like to turn off inference, make sure to do so in
 * {@link #getProperties()}.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestTicket352 extends QuadsTestCase {

	public TestTicket352() {
	}

	public TestTicket352(String arg0) {
		super(arg0);
	}

	public void testEvaluate() throws Exception {
		final BigdataSail sail = getSail();
		try {
			sail.initialize();
			final BigdataSailConnection conn = sail.getConnection();
			try {
				conn.addStatement(new URIImpl("s:1"), new URIImpl("p:1"),
						new LiteralImpl("l1"));
				conn.addStatement(new URIImpl("s:2"), new URIImpl("p:2"),
						new URIImpl("o:2"));
				conn.addStatement(new URIImpl("s:3"), new URIImpl("p:3"),
						new LiteralImpl("l3"));
				if(log.isInfoEnabled())
					log.info("try query with BindingSet evaluate:");
				query(conn, false);
				if(log.isInfoEnabled())
					log.info("try query with BindingSet stream evaluate:");
				query(conn, true);
			} finally {
				conn.close();
			}
		} finally {
			sail.shutDown();
			getSail().__tearDownUnitTest();
		}
	}

	private void query(final BigdataSailConnection conn,
			final boolean useIteration) throws SailException,
			QueryEvaluationException {

		final ProjectionElemList elemList = new ProjectionElemList(
				new ProjectionElem("z"));
		
		final TupleExpr query = new Projection(new StatementPattern(
				new Var("s"), new Var("p"), new Var("o")), elemList);
		
		final QueryBindingSet bindings = mb("z", "u:is_not_in_database");
		
		final CloseableIteration<? extends BindingSet, QueryEvaluationException> results;
		
		if (useIteration) {

			// the caller is providing a set of solutions as inputs.
			final QueryBindingSet emptyQueryBindingSet = new QueryBindingSet();
			
			results = conn.evaluate(query, null, emptyQueryBindingSet,
					new Iter(bindings), false, null);
		} else {

			// w/o the caller providing a set of solutions as inputs.
			results = conn.evaluate(query, null, bindings, false);
		
		}
		
		while (results.hasNext()) {
		
			final BindingSet bset = results.next();
			
			if (log.isInfoEnabled())
				log.info(bset.toString());

		}
		
		results.close();

	}

	/**
	 * Makes a binding set by taking each pair of values and using the first
	 * value as name and the second as value. Creates an URI for a value with a
	 * ':' in it, or a Literal for a value without a ':'.
	 */
	private QueryBindingSet mb(final String... nameValuePairs) {
		
		final QueryBindingSet bs = new QueryBindingSet();
		
		for (int i = 0; i < nameValuePairs.length; i += 2)
			bs.addBinding(nameValuePairs[i],
					nameValuePairs[i + 1].indexOf(':') > 0 ? new URIImpl(
							nameValuePairs[i + 1]) : new LiteralImpl(
							nameValuePairs[i + 1]));
		
		return bs;

	}
	
	/**
	 * Iterates over the given bindings.
	 */
	private static class Iter implements
			CloseableIteration<BindingSet, QueryEvaluationException> {

		final private Iterator<BindingSet> iter;

		private Iter(Collection<BindingSet> bindings) {
			this.iter = bindings.iterator();
		}

		private Iter(BindingSet... bindings) {
			this(Arrays.asList(bindings));
		}

		public boolean hasNext() throws QueryEvaluationException {
			return iter.hasNext();
		}

		public BindingSet next() throws QueryEvaluationException {
			return iter.next();
		}

		public void remove() throws QueryEvaluationException {
			iter.remove();
		}

		public void close() throws QueryEvaluationException {
		}
	}

}