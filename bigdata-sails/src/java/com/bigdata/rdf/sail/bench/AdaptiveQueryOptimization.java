package com.bigdata.rdf.sail.bench;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.controller.JoinGraph.JGraph;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Hard codes LUBM UQ.
 * 
 * <pre>
 * [query2]
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?x ?y ?z
 * WHERE{
 * 	?x a ub:GraduateStudent .
 * 	?y a ub:University .
 * 	?z a ub:Department .
 * 	?x ub:memberOf ?z .
 * 	?z ub:subOrganizationOf ?y .
 * 	?x ub:undergraduateDegreeFrom ?y
 * }
 * </pre>
 * 
 * Re-ordered joins to cluster by shared variables. This makes a nicer graph if
 * you draw it. 
 * 
 * <pre>
 * v2	?z a ub:Department .
 * v3	?x ub:memberOf ?z .
 * v4	?z ub:subOrganizationOf ?y .
 * v1	?y a ub:University .
 * v5	?x ub:undergraduateDegreeFrom ?y
 * v0	?x a ub:GraduateStudent .
 * </pre>
 * 
 * <pre>
 * http://www.w3.org/1999/02/22-rdf-syntax-ns#type (TermId(8U))
 * 
 * http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent (TermId(324U))
 * </pre>
 */
public class AdaptiveQueryOptimization {

	public static void main(String[] args) throws Exception {

		final String namespace = "LUBM_U50";
		final String propertyFile = "/root/workspace/bigdata-quads-query-branch/bigdata-perf/lubm/ant-build/bin/WORMStore.properties";
		final String journalFile = "/data/lubm/U50/bigdata-lubm.WORM.jnl";

		final Properties properties = new Properties();
		{
			// Read the properties from the file.
			final InputStream is = new BufferedInputStream(new FileInputStream(
					propertyFile));
			try {
				properties.load(is);
			} finally {
				is.close();
			}
			if (System.getProperty(BigdataSail.Options.FILE) != null) {
				// Override/set from the environment.
				properties.setProperty(BigdataSail.Options.FILE, System
						.getProperty(BigdataSail.Options.FILE));
			}
			if (properties.getProperty(BigdataSail.Options.FILE) == null) {
				properties.setProperty(BigdataSail.Options.FILE, journalFile);
			}
		}

		final Journal jnl = new Journal(properties);
		try {

			final AbstractTripleStore database = (AbstractTripleStore) jnl
					.getResourceLocator().locate(namespace,
							jnl.getLastCommitTime());

			if (database == null)
				throw new RuntimeException("Not found: " + namespace);

			/*
			 * Resolve terms against the lexicon.
			 */
			final BigdataValueFactory f = database.getLexiconRelation()
					.getValueFactory();

			final BigdataURI rdfType = f
					.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

			final BigdataURI graduateStudent = f
					.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

			final BigdataURI university = f
					.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");

			final BigdataURI department = f
					.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

			final BigdataURI memberOf = f
					.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

			final BigdataURI subOrganizationOf = f
					.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");

			final BigdataURI undergraduateDegreeFrom = f
					.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

			final BigdataValue[] terms = new BigdataValue[] { rdfType,
					graduateStudent, university, department, memberOf,
					subOrganizationOf, undergraduateDegreeFrom };

			// resolve terms.
			database.getLexiconRelation()
					.addTerms(terms, terms.length, true/* readOnly */);

			{
				for (BigdataValue tmp : terms) {
					System.out.println(tmp + " : " + tmp.getIV());
					if (tmp.getIV() == null)
						throw new RuntimeException("Not defined: " + tmp);
				}
			}

			final IVariable<?> x = Var.var("x");
			final IVariable<?> y = Var.var("y");
			final IVariable<?> z = Var.var("z");

			// The name space for the SPO relation.
			final String[] relation = new String[] {namespace + ".spo"};

			final long timestamp = jnl.getLastCommitTime();
			
			int nextId = 0;
			
			// ?x a ub:GraduateStudent .
			final IPredicate p0 = new SPOPredicate(new BOp[] { x,
					new Constant(rdfType.getIV()), new Constant(graduateStudent.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(IPredicate.Annotations.TIMESTAMP,timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?y a ub:University .
			final IPredicate p1 = new SPOPredicate(new BOp[] { y,
					new Constant(rdfType.getIV()), new Constant(university.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(IPredicate.Annotations.TIMESTAMP,timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?z a ub:Department .
			final IPredicate p2 = new SPOPredicate(new BOp[] { z,
					new Constant(rdfType.getIV()), new Constant(department.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(IPredicate.Annotations.TIMESTAMP,timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?x ub:memberOf ?z .
			final IPredicate p3 = new SPOPredicate(new BOp[] { x,
					new Constant(memberOf.getIV()), z },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(IPredicate.Annotations.TIMESTAMP,timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?z ub:subOrganizationOf ?y .
			final IPredicate p4 = new SPOPredicate(new BOp[] { z,
					new Constant(subOrganizationOf.getIV()), y },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(IPredicate.Annotations.TIMESTAMP,timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?x ub:undergraduateDegreeFrom ?y
			final IPredicate p5 = new SPOPredicate(new BOp[] { x,
					new Constant(undergraduateDegreeFrom.getIV()), y },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(IPredicate.Annotations.TIMESTAMP,timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// the vertices of the join graph (the predicates).
			final IPredicate[] preds = new IPredicate[] { p0, p1, p2, p3, p4,
					p5 };

//			final JoinGraph op = new JoinGraph(//
//					new NV(JoinGraph.Annotations.VERTICES, preds),// 
//					new NV(JoinGraph.Annotations.SAMPLE_SIZE, 100) //
//			);
			
			final JGraph g = new JGraph(preds);

			final int limit = 100;

			final QueryEngine queryEngine = QueryEngineFactory
					.getQueryController(jnl/* indexManager */);
			
			final BOpContextBase context = new BOpContextBase(queryEngine);
			
			System.err.println("joinGraph=" + g.toString());

			/*
			 * Sample the vertices.
			 * 
			 * @todo Sampling for scale-out not yet finished.
			 * 
			 * @todo Re-sampling might always produce the same sample depending
			 * on the sample operator impl (it should be random, but it is not).
			 */
			g.sampleVertices(context, limit);
			
			System.err.println("joinGraph=" + g.toString());

			/*
			 * Estimate the cardinality and weights for each edge.
			 * 
			 * @todo It would be very interesting to see the variety and/or
			 * distribution of the values bound when the edge is sampled. This
			 * can be easily done using a hash map with a counter. That could
			 * tell us a lot about the cardinality of the next join path
			 * (sampling the join path also tells us a lot, but it does not
			 * explain it as much as seeing the histogram of the bound values).
			 * I believe that there are some interesting online algorithms for
			 * computing the N most frequent observations and the like which
			 * could be used here.
			 */
			g.estimateEdgeWeights(queryEngine, limit);

			System.err.println("joinGraph=" + g.toString());

			/*
			 * @todo choose starting vertex (most selective). see if there are
			 * any paths which are fully determined based on static optimization
			 * (doubtful).
			 */

			/*
			 * @todo iteratively chain sample to choose best path, then execute
			 * that path. this is where most of the complex bits are.
			 * constraints must be applied to appropriate joins, variables must
			 * be filtered when no longer required, edges which are must be
			 * dropped from paths in which they have become redundant, etc.,
			 * etc.
			 * 
			 * @todo a simpler starting place is just to explore the cost of the
			 * query under different join orderings. e.g., Choose(N), where N is
			 * the #of predicates (full search). Or dynamic programming (also
			 * full search, just a little smarter).
			 */
//			g.run();
			
			
//			/*
//			 * Run the index scan without materializing anything from the
//			 * lexicon.
//			 */
//			if (true) {
//				System.out.println("Running SPO only access path.");
//				final long begin = System.currentTimeMillis();
//				final IAccessPath<ISPO> accessPath = database.getAccessPath(
//						null/* s */, rdfType, undergraduateStudent);
//				final IChunkedOrderedIterator<ISPO> itr = accessPath.iterator();
//				try {
//					while (itr.hasNext()) {
//						itr.next();
//					}
//				} finally {
//					itr.close();
//				}
//				final long elapsed = System.currentTimeMillis() - begin;
//				System.err.println("Materialize SPOs      : elapsed=" + elapsed
//						+ "ms");
//			}

//			/*
//			 * Open the sail and run Q14.
//			 * 
//			 * @todo It would be interesting to run this using a lexicon join.
//			 * Also, given the changes in the various defaults which were
//			 * recently made, it is worth while to again explore the parameter
//			 * space for this query.
//			 */
//			if (true) {
//				final BigdataSail sail = new BigdataSail(database);
//				sail.initialize();
//				final BigdataSailConnection conn = sail.getReadOnlyConnection();
//				try {
//					System.out.println("Materializing statements.");
//					final long begin = System.currentTimeMillis();
//					final CloseableIteration<? extends Statement, SailException> itr = conn
//							.getStatements(null/* s */, rdfType,
//									undergraduateStudent, true/* includeInferred */);
//					try {
//						while (itr.hasNext()) {
//							itr.next();
//						}
//					} finally {
//						itr.close();
//					}
//					final long elapsed = System.currentTimeMillis() - begin;
//					System.err.println("Materialize statements: elapsed="
//							+ elapsed + "ms");
//				} finally {
//					conn.close();
//				}
//				sail.shutDown();
//			}
			
		} finally {
			jnl.close();
		}

	}

}
