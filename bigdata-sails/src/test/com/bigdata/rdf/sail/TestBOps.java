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

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestBOps extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(TestBOps.class);
    
    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();
        
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    /**
     * 
     */
    public TestBOps() {
    }

    /**
     * @param arg0
     */
    public TestBOps(String arg0) {
        super(arg0);
    }

    public void testSimpleJoin() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            URI mike = new URIImpl(ns+"Mike");
            URI bryan = new URIImpl(ns+"Bryan");
            URI person = new URIImpl(ns+"Person");
            URI likes = new URIImpl(ns+"likes");
            URI rdf = new URIImpl(ns+"RDF");
            Literal l1 = new LiteralImpl("Mike");
            Literal l2 = new LiteralImpl("Bryan");
/**/
            cxn.setNamespace("ns", ns);
            
            cxn.add(mike, RDF.TYPE, person);
            cxn.add(mike, likes, rdf);
            cxn.add(mike, RDFS.LABEL, l1);
            cxn.add(bryan, RDF.TYPE, person);
            cxn.add(bryan, likes, rdf);
            cxn.add(bryan, RDFS.LABEL, l2);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

            {
                
                String query = 
                    "PREFIX rdf: <"+RDF.NAMESPACE+"> " +
                    "PREFIX rdfs: <"+RDFS.NAMESPACE+"> " +
                    "PREFIX ns: <"+ns+"> " +
                    
                    "select * " +
                    "WHERE { " +
                    "  ?s rdf:type ns:Person . " +
                    "  ?s ns:likes ?likes . " +
                    "  ?s rdfs:label ?label . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();
 
                Collection<BindingSet> solution = new LinkedList<BindingSet>();
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", mike),
                    new BindingImpl("likes", rdf),
                    new BindingImpl("label", l1)
                }));
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", bryan),
                    new BindingImpl("likes", rdf),
                    new BindingImpl("label", l2)
                }));
                
                compare(result, solution);
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
}
