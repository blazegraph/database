package com.bigdata.blueprints;

import java.util.Properties;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.openrdf.query.QueryLanguage;

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.tinkerpop.blueprints.Edge;

import cutthecrap.utils.striterators.ICloseableIterator;

public class TestPathConstraints extends TestCase {

    private static final transient Logger log = 
            Logger.getLogger(TestPathConstraints.class);
    
    public void testPathLength() throws Exception {
    
        final Properties props = new Properties();
        props.setProperty(BigdataGraph.Options.LAX_EDGES, "true");
        
        final BigdataGraphEmbedded graph = (BigdataGraphEmbedded)
                BigdataGraphFactory.create(
                        SimpleBlueprintsValueFactory.INSTANCE, props);
        
        try {
            
            for (int i = 0; i < 5; i++) {
                graph.addVertex(""+i);
                if (i > 0) {
                    final PartialVertex from = new PartialVertex(""+(i-1));
                    final PartialVertex to = new PartialVertex(""+i);
                    final PartialVertex edge = new PartialVertex((i-1)+""+i);
                    final PartialVertex foo = new PartialVertex("test:foo");
                    final Edge e = graph.addEdge((i-1)+""+i, from, to, "foo");
                    graph.addEdge("test:foo", from, to, null);
                    graph.addEdge(SimpleBlueprintsValueFactory.TYPE.toString(), edge, foo, null);
                    e.setProperty("some:prop", "someVal");
                }
            }
            graph.commit();
            
            if (log.isDebugEnabled()) {
                log.debug("\n"+graph.dumpStore());
            }
            
            { // path limits and constraints
                
                final String queryStr =
                        "select * " +
                        "where { " +
                        "  service bd:alp { " +
                        "    <id:0> ?eid ?to . " +
                        "    hint:Prior hint:alp.pathExpr \"true\" . " +
                        "    ?eid <bigdata:type> <test:foo> . " +
                        "    ?eid <some:prop> \"someVal\" . " +
                        "    hint:Group hint:alp.lowerBound 1 . " +
                        "    hint:Group hint:alp.upperBound ?upper . " + 
                        "  } " +
                        "}";
                
//                final BigdataSailRepositoryConnection cxn = graph
//                        .getReadConnection();
//                try {
//                    final BigdataSailTupleQuery query = (BigdataSailTupleQuery) cxn
//                            .prepareQuery(QueryLanguage.SPARQL, queryStr);
//                    final QueryRoot optimized = query.optimize();
//                    if (log.isDebugEnabled()) {
//                        log.debug("optimized:\n" + optimized);
//                    }
//                } finally {
//                    cxn.close();
//                }
    
                for (int i = 1; i < 5; i++) {
                    final ICloseableIterator<BigdataBindingSet> selection = 
                            graph.select(queryStr.replace("?upper", ""+i));
                    try {
                        int n = 0;
                        while (selection.hasNext()) {
                            final BigdataBindingSet bbs = selection.next();
                            if (log.isDebugEnabled()) {
                                log.debug(bbs);
                            }
                            n++;
                        }
                        assertTrue(n == i);
                    } finally {
                        selection.close();
                    }
                }
                
            }
            
            { // go bi-directional one-hop from v2 with a ZLP
                
                final String queryStr =
                        "select * " +
                        "where { " +
                        "  service bd:alp { " +
                        "    <id:2> <test:foo> ?to . " +
                        "    hint:Prior hint:alp.pathExpr true . " +
                        "    hint:Group hint:alp.lowerBound 0 . " +
                        "    hint:Group hint:alp.upperBound 1 . " + 
                        "    hint:Group hint:alp.bidirectional true . " + 
                        "  } " +
                        "}";
                
//                final BigdataSailRepositoryConnection cxn = graph
//                        .getReadConnection();
//                try {
//                    final BigdataSailTupleQuery query = (BigdataSailTupleQuery) cxn
//                            .prepareQuery(QueryLanguage.SPARQL, queryStr);
//                    final QueryRoot optimized = query.optimize();
//                    if (log.isDebugEnabled()) {
//                        log.debug("optimized:\n" + optimized);
//                    }
//                } finally {
//                    cxn.close();
//                }
    
                final ICloseableIterator<BigdataBindingSet> selection = graph.select(queryStr);
                try {
                    int n = 0;
                    while (selection.hasNext()) {
                        final BigdataBindingSet bbs = selection.next();
                        if (log.isDebugEnabled()) {
                            log.debug(bbs);
                        }
                        n++;
                    }
                    assertTrue(n == 3);
                } finally {
                    selection.close();
                }
                
            }
            
            
        } finally {
            graph.shutdown();
        }
        
    }

    public void testPathFilters() throws Exception {
        
        final Properties props = new Properties();
        props.setProperty(BigdataGraph.Options.LAX_EDGES, "true");
        
        final BigdataGraphEmbedded graph = (BigdataGraphEmbedded)
                BigdataGraphFactory.create(
                        SimpleBlueprintsValueFactory.INSTANCE, props);
        
        try {
            
            for (int i = 0; i < 5; i++) {
                graph.addVertex(""+i);
                if (i > 0) {
                    final PartialVertex from = new PartialVertex(""+(i-1));
                    final PartialVertex to = new PartialVertex(""+i);
                    final PartialVertex edge = new PartialVertex((i-1)+""+i);
                    final PartialVertex foo = new PartialVertex("test:foo");
                    final Edge e = graph.addEdge((i-1)+""+i, from, to, "foo");
                    graph.addEdge("test:foo", from, to, null);
                    graph.addEdge(SimpleBlueprintsValueFactory.TYPE.toString(), edge, foo, null);
                    e.setProperty("some:prop", i);
                }
            }
            graph.commit();
            
            if (log.isDebugEnabled()) {
                log.debug("\n"+graph.dumpStore());
            }
            
            { // path filter
                
                final String queryStr =
                        "select * " +
                        "where { " +
                        "  service bd:alp { " +
                        "    <id:0> ?eid ?to . " +
                        "    hint:Prior hint:alp.pathExpr \"true\" . " +
                        "    ?eid <bigdata:type> <test:foo> . " +
                        "    ?eid <some:prop> ?val . " +
                        "    filter(?val < 3) . " +
                        "    hint:Group hint:alp.lowerBound 0 . " +
                        "    hint:Group hint:alp.upperBound 100 . " + 
                        "  } " +
                        "}";

//                showOptimizedAST(graph, queryStr);
                
                final ICloseableIterator<BigdataBindingSet> selection = graph.select(queryStr);
                try {
                    int n = 0;
                    while (selection.hasNext()) {
                        final BigdataBindingSet bbs = selection.next();
                        if (log.isDebugEnabled()) {
                            log.debug(bbs);
                        }
                        n++;
                    }
                    assertTrue(n == 3);
                } finally {
                    selection.close();
                }
                
            }
            
            { // path filter + incoming bindings
                
                /*
                 * This tests whether vars used by the service are projected
                 * in correctly.
                 */
                final String queryStr =
                        "select * " +
                        "where { " +
                        "  service bd:alp { " +
                        "    <id:0> ?eid ?to . " +
                        "    hint:Prior hint:alp.pathExpr \"true\" . " +
                        "    ?eid <bigdata:type> <test:foo> . " +
                        "    ?eid <some:prop> ?val . " +
                        "    filter(?val < ?max) . " +
                        "    hint:Group hint:alp.lowerBound 0 . " +
                        "    hint:Group hint:alp.upperBound 100 . " + 
                        "  } " +
                        "  values (?max) { " +
                        "    (3) " + 
                        "  } " +
                        "}";
                
//              showOptimizedAST(graph, queryStr);
                
                final ICloseableIterator<BigdataBindingSet> selection = graph.select(queryStr);
                try {
                    int n = 0;
                    while (selection.hasNext()) {
                        final BigdataBindingSet bbs = selection.next();
                        if (log.isDebugEnabled()) {
                            log.debug(bbs);
                        }
                        n++;
                    }
                    assertTrue(n == 3);
                } finally {
                    selection.close();
                }
                
            }
            
        } finally {
            graph.shutdown();
        }
        
    }
    
    private void showOptimizedAST(final BigdataGraphEmbedded graph, 
            final String queryStr) throws Exception {
        
        final BigdataSailRepositoryConnection cxn = graph.getReadConnection();
        try {
            final BigdataSailTupleQuery query = (BigdataSailTupleQuery) cxn
                    .prepareQuery(QueryLanguage.SPARQL, queryStr);
            final QueryRoot optimized = query.optimize();
            if (log.isDebugEnabled()) {
                log.debug("optimized:\n" + optimized);
            }
        } finally {
            cxn.close();
        }

    }

}
