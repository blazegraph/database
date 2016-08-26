package com.bigdata.blueprints;

import java.util.Properties;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.openrdf.query.QueryLanguage;

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.store.BDS;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

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
                        "  values (?max) { " +
                        "    (3) " + 
                        "  } " +
                        "  service bd:alp { " +
                        "    <id:0> ?eid ?to . " +
                        "    hint:Prior hint:alp.pathExpr \"true\" . " +
                        "    ?eid <bigdata:type> <test:foo> . " +
                        "    ?eid <some:prop> ?val . " +
                        "    filter(?val < ?max) . " +
                        "    hint:Group hint:alp.lowerBound 0 . " +
                        "    hint:Group hint:alp.upperBound 100 . " + 
                        "  } " +
                        "}";
                
                showOptimizedAST(graph, queryStr);
                
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
    
    public void testPred() throws Exception {
    
        final Properties props = new Properties();
        props.setProperty(BigdataGraph.Options.LAX_EDGES, "true");
        
        final BigdataGraphEmbedded graph = (BigdataGraphEmbedded)
                BigdataGraphFactory.create(
                        SimpleBlueprintsValueFactory.INSTANCE, props);
        
        try {
            
//            for (int i = 0; i < 5; i++) {
//                graph.addVertex("v"+i);
//                if (i > 0) {
//                    final PartialVertex from = new PartialVertex("v"+(i-1));
//                    final PartialVertex to = new PartialVertex("v"+i);
//                    final Edge e = graph.addEdge("e"+(i-1)+""+i, from, to, "edge");
//                }
//            }
            /*
             * Create more than one path to v4 and see if the ALP op can
             * handle it.
             */
            final Vertex v0 = graph.addVertex("v0");
            final Vertex v1 = graph.addVertex("v1");
            final Vertex v2 = graph.addVertex("v2");
            final Vertex v3a = graph.addVertex("v3a");
            final Vertex v3b = graph.addVertex("v3b");
            final Vertex v4 = graph.addVertex("v4");
            graph.addEdge("e01", v0, v1, "edge");
            graph.addEdge("e12", v1, v2, "edge");
            graph.addEdge("e23a", v2, v3a, "edge");
            graph.addEdge("e23b", v2, v3b, "edge");
            graph.addEdge("e3a4", v3a, v4, "edge");
            graph.addEdge("e3b4", v3b, v4, "edge");
            graph.commit();
            
            if (log.isDebugEnabled()) {
                log.debug("\n"+graph.dumpStore());
            }
            
            { // path predecessor
                
                final String queryStr =
                        "select ?from ?edge ?to " +
                        "where { " +
                        "  service bd:alp { " +
                        "    <id:v0> ?e ?to . " +
                        "    hint:Prior hint:alp.pathExpr \"true\" . " +
                        "    filter(?e != <bigdata:type>) . " +
                        "    hint:Group hint:alp.lowerBound 0 . " +
                        "    hint:Group hint:alp.upperBound 10 . " + 
                        "    hint:Group hint:alp.edgeVar ?edge . " + 
                        "  } " +
//                        "  ?from ?edge ?to . " +
//                        "  filter(?to = <id:v4>) . " +
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
    
                final ICloseableIterator<BigdataBindingSet> selection = 
                        graph.select(queryStr);
                try {
                    int n = 0;
                    while (selection.hasNext()) {
                        final BigdataBindingSet bbs = selection.next();
                        if (log.isDebugEnabled()) {
                            log.debug(bbs);
                        }
                        n++;
                    }
//                    assertTrue(n == 2);
                } finally {
                    selection.close();
                }
                
            }
            
            if (false) { // node and optionally any edges using ZLP
                
                final String queryStr =
                        "select ?from ?edge ?to " +
                        "where { " +
                        "  service bd:alp { " +
                        "    ?id ?e ?to . " +
                        "    hint:Prior hint:alp.pathExpr \"true\" . " +
                        "    filter(?e != <bigdata:type>) . " +
                        "    hint:Group hint:alp.lowerBound 0 . " +
                        "    hint:Group hint:alp.upperBound 1 . " + 
                        "    hint:Group hint:alp.edgeVar ?edge . " + 
                        "  } " +
//                        "  optional { ?from ?edge ?to . } " +
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

                { // v0 has an edge
                    
                    final ICloseableIterator<BigdataBindingSet> selection = 
                            graph.select(queryStr.replace("?id", "<id:v0>"));
                    try {
                        int n = 0;
                        while (selection.hasNext()) {
                            final BigdataBindingSet bbs = selection.next();
                            if (log.isDebugEnabled()) {
                                log.debug(bbs);
                            }
                            n++;
                        }
                        assertTrue(n == 2);
                    } finally {
                        selection.close();
                    }
                    
                }
                
                { // v4 has no edge
                    
                    final ICloseableIterator<BigdataBindingSet> selection = 
                            graph.select(queryStr.replace("?id", "<id:v4>"));
                    try {
                        assertTrue(selection.hasNext());
                        final BigdataBindingSet bbs = selection.next();
                        if (log.isDebugEnabled()) {
                            log.debug(bbs);
                        }
                        assertFalse(selection.hasNext());
                        assertFalse(bbs.isBound("edge"));
                    } finally {
                        selection.close();
                    }
                    
                }
                
            }
            
        } finally {
            graph.shutdown();
        }
        
    }
    
    public void testJoinOrder() throws Exception {
        
        final Properties props = new Properties();
        props.setProperty(BigdataGraph.Options.LAX_EDGES, "true");
        
        final BigdataGraphEmbedded graph = (BigdataGraphEmbedded)
                BigdataGraphFactory.create(
                        SimpleBlueprintsValueFactory.INSTANCE, props);
        
        try {
            
            for (int i = 0; i < 5; i++) {
                final Vertex v = graph.addVertex("v"+i);
                if (i > 0) {
                    final PartialVertex from = new PartialVertex("v"+(i-1));
                    final PartialVertex to = new PartialVertex("v"+i);
                    graph.addEdge("e"+(i-1)+""+i, from, to, "edge");
                } else {
                    v.setProperty("foo", "bar");
                }
            }
            graph.commit();
            
            if (log.isDebugEnabled()) {
                log.debug("\n"+graph.dumpStore());
            }
            
            { // join ordering
                
                final String queryStr =
                        "select ?from ?edge ?to " +
                        "where { " +
                        "  ?id <"+BDS.SEARCH+"> \"bar\" . " +
                        "  service bd:alp { " +
                        "    ?id ?e ?to . " +
                        "    hint:Prior hint:alp.pathExpr \"true\" . " +
                        "    filter(?e != <bigdata:type>) . " +
                        "    hint:Group hint:alp.lowerBound 0 . " +
                        "    hint:Group hint:alp.upperBound 10 . " + 
                        "    hint:Group hint:alp.edgeVar ?edge . " + 
                        "  } " +
                        "  ?from ?edge ?to . " +
                        "  ?from <bigdata:type> <bigdata:Vertex> . " +
                        "}";
                
                final BigdataSailRepositoryConnection cxn = graph.cxn();
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
    
//                final ICloseableIterator<BigdataBindingSet> selection = 
//                        graph.select(queryStr);
//                try {
//                    int n = 0;
//                    while (selection.hasNext()) {
//                        final BigdataBindingSet bbs = selection.next();
//                        if (log.isDebugEnabled()) {
//                            log.debug(bbs);
//                        }
//                        n++;
//                    }
////                    assertTrue(n == 2);
//                } finally {
//                    selection.close();
//                }
                
            }
            
        } finally {
            graph.shutdown();
        }
        
    }
    
    private void showOptimizedAST(final BigdataGraph graph, 
            final String queryStr) throws Exception {
        
        final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) graph.cxn();
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
