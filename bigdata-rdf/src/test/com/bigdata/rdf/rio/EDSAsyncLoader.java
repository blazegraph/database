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
package com.bigdata.rdf.rio;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.openrdf.rio.RDFFormat;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.lexicon.LexiconKeyOrder;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.EmbeddedClient;
import com.bigdata.service.EmbeddedFederation;

/**
 * <pre>
 * -server -Xmx1000m
 * </pre>
 * <pre>
 * bigdata-rdf/src/test/com/bigdata/rdf/rio/EDSAsyncLoader.properties kb "..\rdf-data\lehigh\U1"
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EDSAsyncLoader {

    /**
     * Harness may be used to load data into a {@link EmbeddedFederation}
     * described by a property file.
     * 
     * @param args
     *            <i>propertyFile namespace fileOrDirectory</i><br>
     *            where <i>propertyFile</i> is a file containing configuration
     *            properties; <i>namespace</i> is the KB namespace, and the file
     *            or directory are the RDF data to be loaded.
     * 
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {

        if (args.length != 3) {

            System.err
                    .println("usage: propertyFile  namespace fileOrDirectory");

            System.exit(1);

        }

        // the property file to read.
        final File propertyFile = new File(args[0]);

        // the kb namespace.
        final String namespace = args[1];

        // the file or directory to be loaded.
        final File resource = new File(args[2]);

        if (!resource.exists())
            throw new FileNotFoundException(resource.toString());

        final Properties properties = new Properties();
        {
            final InputStream is = new FileInputStream(propertyFile);
            try {
                properties.load(is);
                properties.list(System.out);
            } finally {
                is.close();
            }
        }

        if(false){
            final String pname = com.bigdata.config.Configuration
                    .getOverrideProperty(namespace + "."
                            + LexiconRelation.NAME_LEXICON_RELATION + "."
                            + LexiconKeyOrder.TERM2ID,
                            IndexMetadata.Options.SINK_IDLE_TIMEOUT_NANOS);

            final String pval = "" + TimeUnit.SECONDS.toNanos(1);

            System.out.println("Override: " + pname + "=" + pval);

            // Put an idle timeout on the sink of 1s.
            properties.setProperty(pname, pval);
        }

        final int producerChunkSize = 20000;
        final int valuesInitialCapacity = 10000;
        final int bnodesInitialCapacity = 16;
        final long unbufferedStatementThreshold = 5000L;// Long.MAX_VALUE;
        final long rejectedExecutionDelay = 250L; // milliseconds.
        final int poolSize = 5; // @todo try 1, 5, 20.

        final EmbeddedClient<?> client = new EmbeddedClient(properties);

        final EmbeddedFederation<?> fed = client.connect();

        try {

            System.out.println("Opening KB: namespace=" + namespace);

            ScaleOutTripleStore tripleStore = (ScaleOutTripleStore) fed
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);

            if (tripleStore == null) {

                System.out.println("Creating new KB: namespace=" + namespace);

                tripleStore = new ScaleOutTripleStore(fed, namespace,
                        ITx.UNISOLATED, properties);

                tripleStore.create();

            }

            final AsynchronousStatementBufferFactory<BigdataStatement, File> statementBufferFactory = new AsynchronousStatementBufferFactory<BigdataStatement, File>(
                    (ScaleOutTripleStore) tripleStore,//
                    producerChunkSize, //
                    valuesInitialCapacity,//
                    bnodesInitialCapacity,//
                    RDFFormat.RDFXML, // defaultFormat
                    false, // verifyData
                    false, // deleteAfter
                    poolSize, // parserPoolSize,
                    20, // parserQueueCapacity
                    poolSize, // term2IdWriterPoolSize,
                    poolSize, // otherWriterPoolSize
                    poolSize, // notifyPoolSize
                    unbufferedStatementThreshold);

            try {
                
                System.out.println("Loading: "+resource);

                // tasks to load the resource or file(s)
                if (resource.isDirectory()) {

                    statementBufferFactory.submitAll(resource,
                            new com.bigdata.rdf.load.RDFFilenameFilter(),
                            rejectedExecutionDelay);

                } else {

                    statementBufferFactory.submitOne(resource);

                }

                System.out.println("Awaiting completion.");
                
                // wait for the async writes to complete.
                statementBufferFactory.awaitAll();

                // dump write statistics for indices used by kb.
                System.err.println(fed.getServiceCounterSet()
                        .getPath("Indices").toString());

                // dump factory specific counters.
                System.err.println(statementBufferFactory.getCounters()
                        .toString());
                
                System.out.println("Done.");

            } catch (Throwable t) {

                statementBufferFactory
                        .cancelAll(true/* mayInterruptIfRunning */);

                // rethrow
                throw new RuntimeException(t);

            }
            
        } finally {

            client.disconnect(true/* immediateShutdown */);

        }

        System.exit(0);
        
    }

}
