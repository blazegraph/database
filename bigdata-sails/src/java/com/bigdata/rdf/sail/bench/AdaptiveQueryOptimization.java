package com.bigdata.rdf.sail.bench;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.controller.JoinGraph.Edge;
import com.bigdata.bop.controller.JoinGraph.JGraph;
import com.bigdata.bop.controller.JoinGraph.Path;
import com.bigdata.bop.controller.JoinGraph.Vertex;
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
 * Hard codes LUBM Q2.
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
			final String[] relation = new String[] { namespace + ".spo" };

			final long timestamp = jnl.getLastCommitTime();

			int nextId = 0;

			// ?x a ub:GraduateStudent .
			final IPredicate p0 = new SPOPredicate(new BOp[] { x,
					new Constant(rdfType.getIV()),
					new Constant(graduateStudent.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(IPredicate.Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?y a ub:University .
			final IPredicate p1 = new SPOPredicate(new BOp[] { y,
					new Constant(rdfType.getIV()),
					new Constant(university.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(IPredicate.Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?z a ub:Department .
			final IPredicate p2 = new SPOPredicate(new BOp[] { z,
					new Constant(rdfType.getIV()),
					new Constant(department.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(IPredicate.Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?x ub:memberOf ?z .
			final IPredicate p3 = new SPOPredicate(new BOp[] { x,
					new Constant(memberOf.getIV()), z },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(IPredicate.Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?z ub:subOrganizationOf ?y .
			final IPredicate p4 = new SPOPredicate(new BOp[] { z,
					new Constant(subOrganizationOf.getIV()), y },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(IPredicate.Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?x ub:undergraduateDegreeFrom ?y
			final IPredicate p5 = new SPOPredicate(new BOp[] { x,
					new Constant(undergraduateDegreeFrom.getIV()), y },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(IPredicate.Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// the vertices of the join graph (the predicates).
			final IPredicate[] preds = new IPredicate[] { p0, p1, p2, p3, p4,
					p5 };

			// final JoinGraph op = new JoinGraph(//
			// new NV(JoinGraph.Annotations.VERTICES, preds),//
			// new NV(JoinGraph.Annotations.SAMPLE_SIZE, 100) //
			// );

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
			 * Estimate the cardinality and weights for each edge, obtaining the
			 * Edge with the minimum estimated cardinality. This will be the
			 * starting point for the join graph evaluation.
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
			 * 
			 * @todo We should be saving the materialized intermediate results,
			 * if not for the initialization, then for the chain sampling.
			 * 
			 * FIXME ROX is choosing the starting edge based on the minimum
			 * estimated cardinality. However, it is possible for there to be
			 * more than one edge with an estimated cardinality which is
			 * substantially to the minimum estimated cardinality. It would be
			 * best to start from multiple vertices so we can explore join paths
			 * which begin with those alternative starting vertices as well.
			 * (LUBM Q2 is an example of such a query).
			 */
			g.estimateEdgeWeights(queryEngine, limit);

			System.err.println("joinGraph=" + g.toString());

			/*
			 * The starting vertex for chain sampling is the vertex in the edge
			 * with the minimum estimated cardinality whose range count is
			 * smallest. Beginning with that vertex, create a set of paths for
			 * each edge branching from that vertex. Since those edges were
			 * already sampled, we can immediately form a set of join paths with
			 * their estimated cardinality and hit ratios (aka scale factors).
			 * 
			 * The initial set of paths consists of a single path whose sole
			 * edge is the edge with the minimum estimated cardinality.
			 */
			final Edge startEdge = g
					.getMinimumCardinalityEdge(null/* visited */);
			
			if (startEdge == null)
				throw new RuntimeException("No weighted edges.");

//			/*
//			 * Generate a set of paths by extending that starting vertex in one
//			 * step in each possible direction. For the initial one-step
//			 * extension of the starting vertex we can reuse the estimated
//			 * cardinality of each edge in the join graph, which was already
//			 * computed above.
//			 */
//			final Path[] paths;
//			{
//
//				System.err.println("startEdge="+startEdge);
//
//				// The starting vertex is the one with the minimum est.
//				// cardinality.
//				final Vertex startVertex = startEdge
//						.getMinimumCardinalityVertex();
//
//				System.err.println("startVertex=" + startVertex);
//
//				// Find the set of edges branching from the starting vertex.
//				final List<Edge> branches = g
//						.getEdges(startVertex, null/* visited */);
//
//				if (branches.isEmpty()) {
//
//					// No vertices remain to be explored so we should just execute something.
//					throw new RuntimeException("Paths can not be extended");
//					
//				} else if (branches.size() == 1) {
//
//					final Edge e = branches.get(0);
//
//					final Path path = new Path(e);
//
//					// The initial sample is just the sample for that edge.
//					path.sample = e.sample;
//
//					System.err.println("path=" + path);
//
//					paths = new Path[] { path };
//
//				} else {
//
//					final List<Path> list = new LinkedList<Path>();
//
//					// Create one path for each of those branches.
//					for (Edge e : branches) {
//
//						if (e.v1 != startVertex && e.v2 != startVertex)
//							continue;
//
//						// Create a one step path.
//						final Path path = new Path(e);
//
//						// The initial sample is just the sample for that edge.
//						path.sample = e.sample;
//
//						System.err
//								.println("path[" + list.size() + "]: " + path);
//
//						list.add(path);
//
//					}
//
//					paths = list.toArray(new Path[list.size()]);
//
//				}
//
//				System.err.println("selectedJoinPath: "
//						+ g.getSelectedJoinPath(paths));
//				
//			}

			/*
			 * FIXME Now extend the initial paths some more and explore the
			 * termination criteria and how they handle paths which are extended
			 * from places other than the "stopping vertex". (We might need a
			 * different termination criteria if we allow the paths to be
			 * extended from any vertex in the path which branches to a vertex
			 * not yet in the path and not yet executed).
			 */
			
			/*
			 * Examine the vertices for the un-executed edge with the minimum
			 * estimated cardinality.
			 * 
			 * If none of those vertices are branching, then the edge is
			 * executed.
			 * 
			 * Otherwise, If there are unexplored branching vertices in
			 * 
			 * Extend the current set of paths by one breadth-first step. For
			 * each path in the path[] which has an unexplored branching vertex,
			 * created one new path per unexplored branch of that vertex. For
			 * paths which do not have a branching vertex, the path is simply
			 * extended by one step.
			 */

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
			// g.run();

			// /*
			// * Run the index scan without materializing anything from the
			// * lexicon.
			// */
			// if (true) {
			// System.out.println("Running SPO only access path.");
			// final long begin = System.currentTimeMillis();
			// final IAccessPath<ISPO> accessPath = database.getAccessPath(
			// null/* s */, rdfType, undergraduateStudent);
			// final IChunkedOrderedIterator<ISPO> itr = accessPath.iterator();
			// try {
			// while (itr.hasNext()) {
			// itr.next();
			// }
			// } finally {
			// itr.close();
			// }
			// final long elapsed = System.currentTimeMillis() - begin;
			// System.err.println("Materialize SPOs      : elapsed=" + elapsed
			// + "ms");
			// }

			// /*
			// * Open the sail and run Q14.
			// *
			// * @todo It would be interesting to run this using a lexicon join.
			// * Also, given the changes in the various defaults which were
			// * recently made, it is worth while to again explore the parameter
			// * space for this query.
			// */
			// if (true) {
			// final BigdataSail sail = new BigdataSail(database);
			// sail.initialize();
			// final BigdataSailConnection conn = sail.getReadOnlyConnection();
			// try {
			// System.out.println("Materializing statements.");
			// final long begin = System.currentTimeMillis();
			// final CloseableIteration<? extends Statement, SailException> itr
			// = conn
			// .getStatements(null/* s */, rdfType,
			// undergraduateStudent, true/* includeInferred */);
			// try {
			// while (itr.hasNext()) {
			// itr.next();
			// }
			// } finally {
			// itr.close();
			// }
			// final long elapsed = System.currentTimeMillis() - begin;
			// System.err.println("Materialize statements: elapsed="
			// + elapsed + "ms");
			// } finally {
			// conn.close();
			// }
			// sail.shutDown();
			// }

		} finally {
			jnl.close();
		}

	}

}
