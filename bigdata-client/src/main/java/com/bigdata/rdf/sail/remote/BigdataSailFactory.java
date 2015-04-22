/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.bigdata.rdf.sail.remote;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.axioms.OwlAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

/**
 * Helper class to create a bigdata instance.
 * 
 * @author mikepersonick
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1152" > BigdataSailFactory must be
 *      moved to the client package </a>
 */
public class BigdataSailFactory {

    /**
     * A handy list of common Options you might want to specify when creating
     * your bigdata instance.
     * 
     * @author mikepersonick
     *
     */
    public static enum Option {
        
        /**
         * Inference on or off.  Off by default.
         */
        Inference,
        
        /**
         * Quads on or off.  Off by default.
         */
        Quads,
        
        /**
         * RDR (statement identifiers) on or off.  Off by default.
         */
        RDR,
        
        /**
         * Text index on or off.  Off by default.
         */
        TextIndex,
        
//        /**
//         * Create an in-memory instance.
//         */
//        InMemory,
//        
//        /**
//         * Create a persistent instance backed by a file.  You must specify
//         * the file.
//         */
//        Persistent
        
    }
    
    /**
    * Connect to a remote bigdata instance.
    * 
    * FIXME This does not parameterize the value of the ContextPath. See
    * {@link BigdataStatics#getContextPath()}.
    */
   public static BigdataSailRemoteRepository connect(final String host,
         final int port) {
     
       return connect("http://" + host + ":" + port + "/bigdata"); 
        
    }
    
    /**
	 * Connect to a remote bigdata instance.
	 * 
     * @param sparqlEndpointURL
     *            The URL of the SPARQL end point.
	 * 
	 * FIXME This does not support the HA load balancer pattern. See #1148.
	 * 
	 * FIXME This does not parameterize the value of the ContextPath. See
	 * {@link BigdataStatics#getContextPath()}.
	 * 
	 * FIXME This MIGHT leak HttpClient or Executor resources.
	 */
   public static BigdataSailRemoteRepository connect(
			final String sparqlEndpointURL) {

      return new RemoteRepositoryManager().getRepositoryForURL(
            sparqlEndpointURL).getBigdataSailRemoteRepository();

	}
	
   /**
    * Convenience method to allow the testing of the URL normalization
    * functionality.
    * 
    * @see <a href="http://trac.blazegraph.com/ticket/1139">
    *      BigdataSailFactory.connect() </a>
    */
   @Deprecated // We are getting rid of this, right?
	public static String testServiceEndpointUrl(final String serviceEndpoint)
	{
		return normalizeEndpoint(serviceEndpoint);
	}

	/**
	 * Massage the service endpoint to ensure that it ends with
	 * </code>/bigdata</code>
	 */
   @Deprecated // We are getting rid of this, right?
    static private String normalizeEndpoint(final String serviceEndpoint) {

        if (serviceEndpoint.endsWith("/sparql")) {
            
        	return serviceEndpoint.substring(0,
        				serviceEndpoint.length()-"/sparql".length());
        	
        } if (serviceEndpoint.endsWith("/sparql/")) {
            
        	return serviceEndpoint.substring(0,
        				serviceEndpoint.length()-"/sparql/".length());
        	
        } else if (serviceEndpoint.endsWith("/bigdata/")) {
            
        	return serviceEndpoint.substring(0, 
        				serviceEndpoint.length()-1) ;
        	
        } else if (serviceEndpoint.endsWith("/bigdata")) {
            
        	return serviceEndpoint;
        	
		} else if (serviceEndpoint.contains("/bigdata")
				&& serviceEndpoint.endsWith("/")) {
			// This is the case of /bigdata/namespace/NAMESPACE/

			return serviceEndpoint.substring(0, serviceEndpoint.length() - 1);

		} else if (serviceEndpoint.contains("/bigdata")) {
			// This is the case of /bigdata/namespace/NAMESPACE

			return serviceEndpoint;

		} else if (serviceEndpoint.endsWith("/")) {

			return serviceEndpoint + "bigdata";

		} else {
            
        	return serviceEndpoint + "/bigdata";
        	
        }
        
    }

    /**
     * Open an existing persistent bigdata instance. If a journal does
     * not exist at the specified location then an exception will be thrown.
     */
    public static BigdataSailRepository openRepository(final String file) {
        
        return new BigdataSailRepository(openSail(file, false));
        
    }
        
    /**
     * Open an existing persistent bigdata instance. If a journal does
     * not exist at the specified location and the boolean create flag is true
     * a journal will be created at that location with the default set of
     * options.
     */
    public static BigdataSailRepository openRepository(final String file, 
            final boolean create) {
        
        return new BigdataSailRepository(openSail(file, create));
        
    }
        
    /**
     * Open an existing persistent bigdata instance. If a journal does
     * not exist at the specified location then an exception will be thrown.
     */
    public static BigdataSail openSail(final String file) {
        
        return openSail(file, false);
        
    }
        
    /**
     * Open an existing persistent bigdata instance. If a journal does
     * not exist at the specified location and the boolean create flag is true
     * a journal will be created at that location with the default set of
     * options.
     */
    public static BigdataSail openSail(final String file, final boolean create) {
        
        if (!new File(file).exists()) {
            
            if (!create) {
                throw new IllegalArgumentException("journal does not exist at specified location");
            } else {
                return createSail(file);
            }
            
        } else {
        
            final Properties props = new Properties();
            props.setProperty(BigdataSail.Options.FILE, file);
            
            final BigdataSail sail = new BigdataSail(props);
            
            return sail;
            
        }
        
    }
    
    /**
     * Create a new bigdata instance using the specified options.  Since no
     * journal file is specified this must be an in-memory instance.
     */
    public static BigdataSailRepository createRepository(final Option... args) {
        
        return createRepository(new Properties(), null, args);
        
    }
    
    /**
     * Create a new bigdata instance using the specified options.  Since no
     * journal file is specified this must be an in-memory instance.
     */
    public static BigdataSailRepository createRepository(final Properties props,
            final Option... args) {
        
        return createRepository(props, null, args);
        
    }
    
    /**
     * Create a new bigdata instance using the specified options.
     */
    public static BigdataSailRepository createRepository(final String file,
            final Option... args) {
        
        return createRepository(new Properties(), file, args);
        
    }
    
    /**
     * Create a new bigdata instance using the specified options.  Since no
     * journal file is specified this must be an in-memory instance.
     */
    public static BigdataSailRepository createRepository(final Properties props,
            final String file, final Option... args) {
        
        return new BigdataSailRepository(createSail(props, file, args));
        
    }
    
    /**
     * Create a new bigdata instance using the specified options.  Since no
     * journal file is specified this must be an in-memory instance.
     */
    public static BigdataSail createSail(final Option... args) {
        
        return createSail(new Properties(), null, args);
        
    }
    
    /**
     * Create a new bigdata instance using the specified options and filename.  
     */
    public static BigdataSail createSail(final String file, 
            final Option... args) {
    
    	//Ticket #1185: BigdataGraphFactory create not working. 
    	
        return createSail(new Properties(), file, args);
        
    }
    
    /**
     * Create a new bigdata instance using the specified options.
     */
    public static BigdataSail createSail(final Properties props,
            final String file, final Option... args) {
        
        final List<Option> options = args != null ? 
                Arrays.asList(args) : new LinkedList<Option>();

        checkArgs(file, options);
                
//        final Properties props = new Properties();
        
        if (file != null) {
            props.setProperty(BigdataSail.Options.FILE, file);
            props.setProperty(Journal.Options.BUFFER_MODE, BufferMode.DiskRW.toString());
        } else {
            props.setProperty(Journal.Options.BUFFER_MODE, BufferMode.MemStore.toString());
        }
        
        if (options.contains(Option.Inference)) {
            props.setProperty(BigdataSail.Options.AXIOMS_CLASS, OwlAxioms.class.getName());
            props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "true");
            props.setProperty(BigdataSail.Options.JUSTIFY, "true");
        } else {
            props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
            props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
            props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        }
        
        props.setProperty(BigdataSail.Options.TEXT_INDEX, 
                String.valueOf(options.contains(Option.TextIndex)));
        
        props.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, 
                String.valueOf(options.contains(Option.RDR)));
        
        props.setProperty(BigdataSail.Options.QUADS, 
                String.valueOf(options.contains(Option.Quads)));
        
        // Setup for the RWStore recycler rather than session protection.
        props.setProperty("com.bigdata.service.AbstractTransactionService.minReleaseAge","1");
        props.setProperty("com.bigdata.btree.writeRetentionQueue.capacity","4000");
        props.setProperty("com.bigdata.btree.BTree.branchingFactor","128");
        // Bump up the branching factor for the lexicon indices on the default kb.
        props.setProperty("com.bigdata.namespace.kb.lex.com.bigdata.btree.BTree.branchingFactor","400");
        // Bump up the branching factor for the statement indices on the default kb.
        props.setProperty("com.bigdata.namespace.kb.spo.com.bigdata.btree.BTree.branchingFactor","1024");
        
        final BigdataSail sail = new BigdataSail(props);
        
        return sail;
        
    }
    
    protected static void checkArgs(final String file, final List<Option> options) {
        
        if (options.contains(Option.Inference) && options.contains(Option.Quads)) {
            throw new IllegalArgumentException();
        }
        
        if (options.contains(Option.RDR) && options.contains(Option.Quads)) {
            throw new IllegalArgumentException();
        }
        
//        if (options.contains(Option.InMemory) && options.contains(Option.Persistent)) {
//            throw new IllegalArgumentException();
//        }
//        
//        if (options.contains(Option.InMemory) && file != null) {
//            throw new IllegalArgumentException();
//        }
//        
//        if (options.contains(Option.Persistent) && file == null) {
//            throw new IllegalArgumentException();
//        }
        
    }
    
    
}
