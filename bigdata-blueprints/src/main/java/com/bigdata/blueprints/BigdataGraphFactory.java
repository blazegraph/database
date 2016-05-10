/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.blueprints;

import java.util.Properties;

import com.bigdata.BigdataStatics;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.remote.BigdataSailFactory;


/**
 * Helper class to create BigdataGraph instances.
 * 
 * @author mikepersonick
 *
 */
public class BigdataGraphFactory  {

//    private static final transient Logger log = Logger.getLogger(BigdataGraphFactory.class);

   /**
    * Connect to a remote bigdata instance.
    * 
    * FIXME This does not parameterize the value of the ContextPath. See
    * {@link BigdataStatics#getContextPath()}.
    * 
    * @deprecated
    * As of version 1.5.2, you should use one of the connect methods with a sparqlEndpointURL.
    * See {@linkplain http://wiki.blazegraph.com/wiki/index.php/NanoSparqlServer#Active_URLs} 
    */
   public static BigdataGraph connect(final String host, final int port) {

	  //Assume the default KB to make the SPARQL Endpoint
	  //FIXME:  the /bigdata reference should be parameterized
      return connect("http://" + host + ":" + port + "/bigdata" + "/sparql");

    }
    
    /**
     * Connect to a remote bigdata instance.
     * 
     * @param sparqlEndpointURL
     *            The URL of the SPARQL end point. This will be used to read and
     *            write on the graph using the blueprints API.
     */

    public static BigdataGraph connect(final String sparqlEndpointURL) {

    	//Ticket #1182:  centralize rewriting in the SAIL factory.
       
       return new BigdataGraphClient(BigdataSailFactory.connect(sparqlEndpointURL));
       
    }

    /**
     * Open an existing persistent local bigdata instance.  If a journal does
     * not exist at the specified location and the boolean create flag is true
     * a journal will be created at that location.
     */
    public static BigdataGraph open(final String file, final boolean create) throws Exception {
        final BigdataSail sail = (BigdataSail) BigdataSailFactory.openSail(file, create);
        sail.initialize();
        return new BigdataGraphEmbedded(sail);
    }

    /**
	 * Create a persistent local bigdata instance. If a journal does not exist
	 * at the specified location, then a journal will be created at that
	 * location.
	 */
    public static BigdataGraph create(final String file) throws Exception {
        final BigdataSail sail = (BigdataSail) BigdataSailFactory.openSail(file, true);
        sail.initialize();
        return new BigdataGraphEmbedded(sail);
    }

    /**
     * Create a new local in-memory bigdata instance.
     */
    public static BigdataGraph create() throws Exception {
        return create(BigdataRDFFactory.INSTANCE);
    }
    
    /**
     * Create a new local in-memory bigdata instance with the supplied value
     * factory.
     */
    public static BigdataGraph create(final BlueprintsValueFactory vf) 
            throws Exception {
        return create(vf, new Properties());
    }
    
    /**
     * Create a new local in-memory bigdata instance with the supplied value
     * factory.
     */
    public static BigdataGraph create(final BlueprintsValueFactory vf, 
            final Properties props) throws Exception {
        final BigdataSail sail = (BigdataSail) BigdataSailFactory.createSail();
        sail.initialize();
        return new BigdataGraphEmbedded(sail, vf, props);
    }
    
//    /**
//     * Create a new persistent local bigdata instance.
//     */
//    public static BigdataGraph create(final String file) 
//            throws Exception {
//        final BigdataSail sail = BigdataSailFactory.createSail(file);
//        sail.initialize();
//        return new BigdataGraphEmbedded(sail);
//    }
    
}
