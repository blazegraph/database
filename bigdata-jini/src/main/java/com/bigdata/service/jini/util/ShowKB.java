/*

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
/*
 * Created on Jan 10, 2009
 */

package com.bigdata.service.jini.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import net.jini.config.ConfigurationException;

import org.openrdf.model.vocabulary.RDF;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.config.Configuration;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.BytesUtil;

/**
 * Utility to create a scale-out KB instance.  You must specify an appropriate
 * security policy. For example:
 * <pre>
 * -Djava.security.policy=policy.all
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: CreateKB.java 4970 2011-07-26 15:11:55Z thompsonbry $
 */
public class ShowKB {
    
//    private static final Logger log = Logger.getLogger(ShowKB.class);

	/**
	 * The name of the component in the jini configuration file for this class.
	 */
    protected static final String COMPONENT = ShowKB.class.getName();

	/**
	 * Configuration options understood by this utility. 
	 */
    public interface ConfigurationOptions {

		/**
		 * The KB namespace. This option must be specified for the
		 * {@value ShowKB#COMPONENT} in the {@link Configuration}.
		 */
		String NAMESPACE = "namespace";

    }
    
    private final JiniFederation<?> fed;
    
    private ShowKB(final JiniFederation<?> fed) {
    	
    	if(fed == null)
    		throw new IllegalArgumentException();
    	
    	this.fed = fed;
    	
    }

	/**
	 * Show somes stuff about a KB instance.
	 * <p>
	 * Configuration options use {@link #COMPONENT} as their namespace. The
	 * following options are defined:
	 * <dl>
	 * 
	 * <dt>{@value ConfigurationOptions#NAMESPACE}</dt>
	 * <dd>The namespace of the KB instance.</dd>
	 * 
	 * </dl>
	 * 
	 * @param args
	 *            Configuration file and optional overrides.
	 * 
	 * @see ConfigurationOptions
	 * 
	 * @throws ConfigurationException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
    public static void main(final String[] args) throws InterruptedException,
            ConfigurationException, IOException, ExecutionException {

    	System.err.println("Start");
        final JiniFederation<?> fed = JiniClient.newInstance(args).connect();
    	System.err.println("Connected");

        /*
         * Install a shutdown hook (normal kill will trigger this hook).
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {

                fed.shutdownNow();

            }

        });
        
        try {

            final String namespace = (String) fed
                    .getClient()
                    .getConfiguration()
                    .getEntry(COMPONENT, ConfigurationOptions.NAMESPACE,
                            String.class);

            System.out.println("KB namespace=" + namespace);

            final long ts = fed.getLastCommitTime();
//            final long ts = ITx.UNISOLATED;
            
            final ScaleOutTripleStore tripleStore = (ScaleOutTripleStore) fed
                    .getResourceLocator().locate(namespace, ts);

            if (tripleStore == null) {

                System.err.println("Does not exist: " + namespace);

                System.exit(1);

            }

            // show #of statements in the newly create triple store (e.g., any axioms).
            System.out.println("axiomCount=" + tripleStore.getStatementCount());
            
            {
                final KeyBuilder keyBuilder = new KeyBuilder();

                @SuppressWarnings({ "unchecked", "rawtypes" })
                final Iterator<BigdataValue> itr = (Iterator) tripleStore
                        .getVocabulary().values();
                
                while(itr.hasNext()) {
                    
                    final BigdataValue v = itr.next();
                    
                    final IV<?,?> iv = v.getIV();

                    final String key = iv == null ? "N/A" : BytesUtil
                            .toString(IVUtility.encode(keyBuilder.reset(), iv)
                                    .getKey());

                    System.out.println(v+"\t"+iv+"\t"+key);
                    
                }

                // Range count rdf:type.
                {
                	final IAccessPath<ISPO> ap = tripleStore.getAccessPath(null/*s*/,RDF.TYPE,null/*o*/);
                	System.out.println("have ap: "+ap);
                	final long rangeCount = ap.rangeCount(false/*exact*/);
                	System.out.println("rangeCount="+rangeCount);
                }
                
            }

        } finally {

            fed.shutdown();

        }

    }

}
