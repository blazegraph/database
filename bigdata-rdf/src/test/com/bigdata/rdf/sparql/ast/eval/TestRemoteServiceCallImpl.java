/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.RemoteServiceCallImpl;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Data driven test suite for SPARQL 1.1 Federated Query against an openrdf
 * aware SERVICE implementation running in the same JVM.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRemoteServiceCallImpl extends AbstractDataDrivenSPARQLTestCase {

    private static final Logger log = Logger
            .getLogger(TestRemoteServiceCallImpl.class);
    
    /**
     * 
     */
    public TestRemoteServiceCallImpl() {
    }

    /**
     * @param name
     */
    public TestRemoteServiceCallImpl(String name) {
        super(name);
    }

    /**
     * A mocked up test to validate the SPARQL query generation logic.
     */
    public void test_service_001() throws Exception {
        
        final URI serviceURI = new URIImpl("http://www.bigdata.com/myService");

        final GraphPatternGroup<IGroupMemberNode> groupNode = new JoinGroupNode();
        {
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));
        }
        
        final String exprImage = "SERVICE <" + serviceURI + "> { ?s ?p ?o }";
        
        final Map<String,String> prefixDecls = new LinkedHashMap<String, String>();
        {
            prefixDecls.put("foo", "http://www.bigdata.com/foo");
        }

        final RemoteServiceCallImpl fixture = new RemoteServiceCallImpl(store,
                groupNode, serviceURI, exprImage, prefixDecls);

        final List<BindingSet> bindingSets = new LinkedList<BindingSet>();
        
        final String queryStr = fixture.getSparqlQuery(bindingSets
                .toArray(new BindingSet[bindingSets.size()]));
        
        if (log.isInfoEnabled())
            log.info(queryStr);
        
        final String baseURI = null;
        
        // Verify that the query is syntactically valid.
        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        if (log.isInfoEnabled())
            log.info(astContainer);

    }
        
    /**
     * A variant test in which there are some BINDINGS to be passed through. The
     * set of bindings covers the different types of RDF {@link Value} and also
     * exercises the prefix declarations.
     */
    public void test_service_002() throws Exception {
        
        final URI serviceURI = new URIImpl("http://www.bigdata.com/myService");

        final GraphPatternGroup<IGroupMemberNode> groupNode = new JoinGroupNode();
        {
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("book")));
        }
        
        final String exprImage = "SERVICE <" + serviceURI + "> { ?s ?p ?book }";
        
        final Map<String,String> prefixDecls = new LinkedHashMap<String, String>();
        {
            prefixDecls.put("foo", "http://www.bigdata.com/foo#");
        }

        final RemoteServiceCallImpl fixture = new RemoteServiceCallImpl(store,
                groupNode, serviceURI, exprImage, prefixDecls);

        final List<BindingSet> bindingSets = new LinkedList<BindingSet>();
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book",
                    new URIImpl("http://example.org/book/book1"));
            bindingSets.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book",
                    new URIImpl("http://www.bigdata.com/foo#book2"));
            bindingSets.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book", new LiteralImpl("Semantic Web Primer"));
            bindingSets.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book",
                    new LiteralImpl("Semantic Web Primer", "DE"));
            bindingSets.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book", new LiteralImpl("12", XSD.INT));
            bindingSets.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book", new LiteralImpl("true", XSD.BOOLEAN));
            bindingSets.add(bset);
        }
        /*
         * Note: Blank nodes are not permitting in the BINDINGS clause (per the
         * SPARQL 1.1 grammar).
         */
//        {
//            final MapBindingSet bset = new MapBindingSet();
//            bset.addBinding("book", new BNodeImpl("abc"));
//            bindingSets.add(bset);
//        }
        
        final String queryStr = fixture.getSparqlQuery(bindingSets
                .toArray(new BindingSet[bindingSets.size()]));
        
        if (log.isInfoEnabled())
            log.info(queryStr);
        
        final String baseURI = null;
        
        // Verify that the query is syntactically valid.
        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        if (log.isInfoEnabled())
            log.info(astContainer);
        
    }

}
