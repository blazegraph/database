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
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.memory.MemoryStore;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestSingleTailRule extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(TestSingleTailRule.class);
    
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
    public TestSingleTailRule() {
    }

    /**
     * @param arg0
     */
    public TestSingleTailRule(String arg0) {
        super(arg0);
    }

    public void testMultiGraphs() throws Exception {

        final BigdataSail sail = getSail();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        repo.initialize();
        final BigdataSailRepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            URI mike = vf.createURI(ns+"Mike");
            URI likes = vf.createURI(ns+"likes");
            URI rdf = vf.createURI(ns+"RDF");
/**/
            cxn.setNamespace("ns", ns);
            
            testValueRoundTrip(cxn.getSailConnection(), mike, likes, rdf);
            
        } finally {
            cxn.close();
            if (sail instanceof BigdataSail)
                ((BigdataSail)sail).__tearDownUnitTest();
        }

    }
    
    private void testValueRoundTrip(final SailConnection con, 
            final Resource subj, final URI pred, final Value obj)
        throws Exception
    {
        con.addStatement(subj, pred, obj);
        con.commit();
    
        CloseableIteration<? extends Statement, SailException> stIter = 
            con.getStatements(null, null, null, false);
    
        try {
            assertTrue(stIter.hasNext());
    
            Statement st = stIter.next();
            assertEquals(subj, st.getSubject());
            assertEquals(pred, st.getPredicate());
            assertEquals(obj, st.getObject());
            assertTrue(!stIter.hasNext());
        }
        finally {
            stIter.close();
        }
    
//        ParsedTupleQuery tupleQuery = QueryParserUtil.parseTupleQuery(QueryLanguage.SERQL,
//                "SELECT S, P, O FROM {S} P {O} WHERE P = <" + pred.stringValue() + ">", null);
        ParsedTupleQuery tupleQuery = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL,
                "SELECT ?S ?P ?O " +
                "WHERE { " +
                "  ?S ?P ?O . " +
                "  FILTER( ?P = <" + pred.stringValue() + "> ) " +
                "}", null);
    
        CloseableIteration<? extends BindingSet, QueryEvaluationException> iter;
        iter = con.evaluate(tupleQuery.getTupleExpr(), null, EmptyBindingSet.getInstance(), false);
    
        try {
            assertTrue(iter.hasNext());
    
            BindingSet bindings = iter.next();
            assertEquals(subj, bindings.getValue("S"));
            assertEquals(pred, bindings.getValue("P"));
            assertEquals(obj, bindings.getValue("O"));
            assertTrue(!iter.hasNext());
        }
        finally {
            iter.close();
        }
    }


    
}
