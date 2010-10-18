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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class DavidsTestBOps extends ProxyBigdataSailTestCase {

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
    public DavidsTestBOps() {
    }

    /**
     * @param arg0
     */
    public DavidsTestBOps(String arg0) {
        super(arg0);
    }
    
    public void testDefaultGraph ()
        throws Exception
    {
        BigdataSail sail = getTheSail () ;
        final ValueFactory vf = sail.getValueFactory();
        RepositoryConnection cxn = getRepositoryConnection ( sail ) ;

        String ns = "http://xyz.com/test#" ;
        String kb = String.format ( "<%ss> <%sp> <%so> .", ns, ns, ns ) ;
        String qs = String.format ( "select ?p ?o where { <%ss> ?p ?o .}", ns ) ;

        Resource graphs [] = new Resource [] { vf.createURI ( String.format ( "%sg1", ns ) ), vf.createURI ( String.format ( "%sg2", ns ) ) } ;

        Collection<BindingSet> expected = getExpected ( createBindingSet ( new BindingImpl ( "p", new URIImpl ( String.format ( "%sp", ns ) ) )
                                                                         , new BindingImpl ( "o", new URIImpl ( String.format ( "%so", ns ) ) )
                                                                         )
                                                      ) ;
        run ( sail, cxn, kb, graphs, qs, expected ) ;
    }

    private BigdataSail getTheSail ()
        throws SailException
    {
        BigdataSail sail = getSail () ;
        sail.initialize () ;
        return sail ;
    }

    private RepositoryConnection getRepositoryConnection ( BigdataSail sail )
        throws RepositoryException
    {
        BigdataSailRepository repo = new BigdataSailRepository ( sail ) ;
        BigdataSailRepositoryConnection cxn = ( BigdataSailRepositoryConnection )repo.getConnection () ;
        cxn.setAutoCommit ( false ) ;
        return cxn ;
    }

    private void run ( BigdataSail sail, RepositoryConnection rc, String kb, Resource graphs [], String qs, Collection<BindingSet> expected )
    {
        try
        {
            for ( Resource g : graphs )
                load ( rc, kb, g ) ;
            compare ( query ( rc, qs ), expected ) ;
        }
        catch ( Exception e )
        {
            e.printStackTrace () ;
        }
        finally
        {
            try { if ( null != rc ) rc.close () ; }
            catch ( Exception e ) {}
            if ( null != sail ) sail.__tearDownUnitTest () ;
        }
    }

    private void load ( RepositoryConnection rc, String kb, Resource g )
        throws RepositoryException, RDFParseException, IOException
    {
        rc.add ( new ByteArrayInputStream ( kb.toString ().getBytes ( "UTF-8" ) )
               , "http://xyz.com/test"
               , RDFFormat.TURTLE
               , g
               ) ;
        rc.commit () ;
    }

    private TupleQueryResult query ( RepositoryConnection rc, String qs )
        throws RepositoryException, MalformedQueryException, QueryEvaluationException
    {
        return rc.prepareTupleQuery ( QueryLanguage.SPARQL, qs ).evaluate () ;
    }

    private Collection<BindingSet> getExpected ( BindingSet... bindingSets )
    {
        Collection<BindingSet> expected = new LinkedList<BindingSet> () ;
        for ( BindingSet bs : bindingSets )
            expected.add ( bs ) ;
        return expected ;
    }
}