/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
package com.bigdata.rdf.sail.tck;

import java.util.Properties;
import java.util.UUID;

import junit.framework.Test;

import org.apache.log4j.Logger;
import org.openrdf.query.Dataset;
import org.openrdf.query.parser.sparql.ManifestTest;
import org.openrdf.query.parser.sparql.SPARQLQueryTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.dataset.DatasetRepository;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;

/**
 * @author <a href="mailto:dmacgbr@users.sourceforge.net">David MacMillan</a>
 * @version $Id$
 */
public class BigdataFederationSparqlTest extends SPARQLQueryTest
{
    public static Test suite ()
        throws Exception
    {
        return ManifestTest.suite
        (
            new Factory ()
            {
                public SPARQLQueryTest createSPARQLQueryTest ( String URI, String name, String query, String results, Dataset dataSet, boolean laxCardinality)
                {
                    return new BigdataFederationSparqlTest ( URI, name, query, results, dataSet, laxCardinality ) ;
                }
            }
        ) ;
    }

    public BigdataFederationSparqlTest ( String URI, String name, String query, String results, Dataset dataSet, boolean laxCardinality )
    {
        super ( URI, name, query, results, dataSet, laxCardinality ) ;
    }
   
    @Override public void tearDown ()
        throws Exception
    {
        super.tearDown () ;
        _ts.destroy () ;
        _ts = null ;
    }

    @Override protected Repository newRepository ()
        throws Exception
    {
        return new DatasetRepository ( new BigdataSailRepository ( new BigdataSail ( newTripleStore () ) ) ) ;
    }

    private ScaleOutTripleStore newTripleStore ()
        throws Exception
    {
        _ts = new ScaleOutTripleStore ( getFederation (), newNamespace (), ITx.UNISOLATED, getProperties () ) ;
        _ts.create () ;
        return _ts ;
    }

    private JiniFederation<Object> getFederation ()
        throws Exception
    {
        if ( null == _fed )
        {
            JiniClient<Object> jc = new JiniClient<Object> ( new String [] { getConfiguration () } ) ;
            _fed = jc.connect () ;
        }
        return _fed ;
    }

    private String getConfiguration ()
        throws Exception
    {
        String c = System.getProperty ( CONFIG_PROPERTY ) ;
        if ( null == c )
            throw new Exception ( String.format ( "Configuration property not set. Specify as: -D%s=<filename or URL>", CONFIG_PROPERTY ) ) ;
        return c ;
    }

    private String newNamespace ()
    {
        return "SPARQLTest_" + UUID.randomUUID ().toString () ;
    }

    private Properties getProperties ()
    {
        if ( null == _properties )
        {
            //
            // TODO What do we really need here? Don't some of these entail others?
            //
            _properties = new Properties () ;
            _properties.put ( BigdataSail.Options.QUADS_MODE, "true" ) ;
            _properties.put ( BigdataSail.Options.TRUTH_MAINTENANCE, "false" ) ;
            _properties.put ( BigdataSail.Options.NATIVE_JOINS, "true" ) ;
            _properties.put ( BigdataSail.Options.QUERY_TIME_EXPANDER, "true" ) ;
            _properties.put ( BigdataSail.Options.ALLOW_AUTO_COMMIT, "true" ) ;
            _properties.put ( BigdataSail.Options.ISOLATABLE_INDICES, "false" ) ;
            _properties.put ( BigdataSail.Options.STAR_JOINS, "false" ) ;
            _properties.put ( BigdataSail.Options.TEXT_INDEX, "false" ) ;
        }
        return _properties ;
    }

    public static final String CONFIG_PROPERTY = "bigdata.configuration" ;

    private static final Logger _logger = Logger.getLogger ( BigdataFederationSparqlTest.class ) ;

    private JiniFederation<Object> _fed = null ;
    private ScaleOutTripleStore _ts = null ;
    private Properties _properties = null ;
}