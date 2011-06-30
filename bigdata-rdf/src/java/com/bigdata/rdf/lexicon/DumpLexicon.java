package com.bigdata.rdf.lexicon;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Properties;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.JiniClient;

/**
 * Utility class to dump the TERMS index of a triple store.
 * 
 * @author thompsonbry
 */
public class DumpLexicon {

	private DumpLexicon() {
	}

	private static void usage() {
        
        System.err.println("usage: (-tuples) <namespace> <filename>");

	}

    /**
     * Open the {@link IIndexManager} identified by the property file.
     * 
     * @param propertyFile
     *            The property file (for a standalone bigdata instance) or the
     *            jini configuration file (for a bigdata federation). The file
     *            must end with either ".properties" or ".config".
     *            
     * @return The {@link IIndexManager}.
     */
    private static IIndexManager openIndexManager(final String propertyFile) {

        final File file = new File(propertyFile);

        if (!file.exists()) {

            throw new RuntimeException("Could not find file: " + file);

        }

        boolean isJini = false;
        if (propertyFile.endsWith(".config")) {
            // scale-out.
            isJini = true;
        } else if (propertyFile.endsWith(".properties")) {
            // local journal.
            isJini = false;
        } else {
            /*
             * Note: This is a hack, but we are recognizing the jini
             * configuration file with a .config extension and the journal
             * properties file with a .properties extension.
             */
            throw new RuntimeException(
                    "File must have '.config' or '.properties' extension: "
                            + file);
        }

        final IIndexManager indexManager;
        try {

            if (isJini) {

                /*
                 * A bigdata federation.
                 */

				final JiniClient jiniClient = new JiniClient(
						new String[] { propertyFile });

                indexManager = jiniClient.connect();

            } else {

                /*
                 * Note: we only need to specify the FILE when re-opening a
                 * journal containing a pre-existing KB.
                 */
                final Properties properties = new Properties();
                {
                    // Read the properties from the file.
                    final InputStream is = new BufferedInputStream(
                            new FileInputStream(propertyFile));
                    try {
                        properties.load(is);
                    } finally {
                        is.close();
                    }
                    if (System.getProperty(BigdataSail.Options.FILE) != null) {
                        // Override/set from the environment.
                        properties.setProperty(BigdataSail.Options.FILE, System
                                .getProperty(BigdataSail.Options.FILE));
                    }
                }

                final Journal jnl = new Journal(properties);
                
                indexManager = jnl;

            }

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return indexManager;
        
    }

    /**
	 * @param args
	 *            <code>(-tuples) &lt;namespace&gt; &lt;filename&gt;</code> <br/>
	 *            where <i>namespace</i> is the namespace of the
	 *            {@link LexiconRelation}. Use <code>kb.lex</code> if you have
	 *            not overridden the namespace of the
	 *            {@link AbstractTripleStore}. <br/>
	 *            where <i>filename</i> is the name of the properties or
	 *            configuration file to be used.
	 */
	public static void main(final String[] args) {

		if (args.length < 2) {
			usage();
	        System.exit(1);
        }

        boolean showTuples = false;

        int i = 0;
        
        for(; i<args.length; i++) {
            
            String arg = args[i];
            
            if( ! arg.startsWith("-")) {
                
                // End of options.
                break;
                
            }
            
            if(arg.equals("-tuples")) {
                
                showTuples = true;
                
            }
            
			else
				throw new RuntimeException("Unknown argument: " + arg);
            
        }

		if (i + 2 != args.length) {
			usage();
			System.exit(1);
		}

		final String namespace = args[i++];

		final String propertyFile = args[i++];

		PrintWriter w = new PrintWriter(System.out);

		IIndexManager indexManager = null;
		try {

			w.println("namespace: " + namespace);

			w.println("filename : " + propertyFile);
	        
			indexManager = openIndexManager(propertyFile);

			final long timestamp = ITx.READ_COMMITTED;

			final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
					.getResourceLocator().locate(namespace, timestamp);

			if (tripleStore == null) {

				throw new RuntimeException("Not found: namespace=" + namespace
						+ ", timestamp=" + TimestampUtility.toString(timestamp));

			}

			final LexiconRelation lex = tripleStore.getLexiconRelation();

			w.println(lex.getLexiconConfiguration());

			w.flush();
			
			new BlobsIndexHelper().dump(w, showTuples, namespace, lex
					.getBlobsIndex());

		} catch (RuntimeException ex) {

			ex.printStackTrace();

			System.err.println("Error: " + ex + " on file: " + propertyFile);

			System.exit(2);
			
		} finally {

			w.flush();
			
			w.close();
			
			if(indexManager != null) {

				if(indexManager instanceof IJournal) {

					if(((IJournal)indexManager).isOpen()) {
						
						((IJournal) indexManager).shutdownNow();
						
					}

				} else {

					IBigdataClient<?> client = null;

					try {
					
						client = ((IBigdataFederation<?>) indexManager)
								.getClient();
						
					} catch (IllegalStateException ex) {
						// Ignore.
					}
					
					if (client != null)
						client.disconnect(true/* immediateShutdown */);

				}
				
			}
			
		}

	}

//  /**
//  * Dumps the lexicon in a variety of ways (test suites only).
//  */
// public StringBuilder dumpTerms() {
//
//     final StringBuilder sb = new StringBuilder(Bytes.kilobyte32 * 4);
//
//     /**
//      * Dumps the terms in term order.
//      */
//     sb.append("---- terms in term order ----\n");
//     for( Iterator<Value> itr = termIterator(); itr.hasNext(); ) {
//         
//         final Value val = itr.next();
//         
//         if (val == null) {
//             sb.append("NullIV");
//         } else {
//             sb.append(val.toString());
//         }
//         
//         sb.append("\n");
//         
//     }
//     
//     return sb;
//     
// }
    
//    /**
//     * Dumps the lexicon in a variety of ways.
//     * 
//     * @param store
//     */
//    protected void dumpTerms(final AbstractTripleStore store) {

        /*
         * Note: it is no longer true that all terms are stored in the reverse
         * index (BNodes are not). Also, statement identifiers are stored in the
         * forward index, so we can't really write the following assertion
         * anymore.
         */
//        // Same #of terms in the forward and reverse indices.
//        assertEquals("#terms", store.getIdTermIndex().rangeCount(null, null),
//                store.getTermIdIndex().rangeCount(null, null));
        
//        /**
//         * Dumps the forward mapping.
//         */
//        {
//
//            System.err.println("terms index (forward mapping).");
//
//            final IIndex ndx = store.getLexiconRelation().getTerm2IdIndex();
//
//            final ITupleIterator itr = ndx.rangeIterator(null, null);
//
//            while (itr.hasNext()) {
//
//                final ITuple tuple = itr.next();
//                
////                // the term identifier.
////                Object val = itr.next();
//
//                /*
//                 * The sort key for the term. This is not readily decodable. See
//                 * RdfKeyBuilder for specifics.
//                 */
//                final byte[] key = tuple.getKey();
//
//                /*
//                 * deserialize the term identifier (packed long integer).
//                 */
//                final long id;
//                
//                try {
//
//                    id = tuple.getValueStream().readLong();
////                    id = tuple.getValueStream().unpackLong();
//
//                } catch (IOException ex) {
//
//                    throw new RuntimeException(ex);
//
//                }
//
//                System.err.println(BytesUtil.toString(key) + ":" + id);
//
//            }
//
//        }

//        /**
//         * Dumps the reverse mapping.
//         */
//        {
//
//            System.err.println("ids index (reverse mapping).");
//
//            final IIndex ndx = store.getLexiconRelation().getId2TermIndex();
//
//            final ITupleIterator<BigdataValue> itr = ndx.rangeIterator(null, null);
//
//            while (itr.hasNext()) {
//
//                final ITuple<BigdataValue> tuple = itr.next();
//                
//                final BigdataValue term = tuple.getObject();
//                
//                System.err.println(term.getIV()+ ":" + term);
//
//            }
//
//        }
        
//        /**
//         * Dumps the term:id index.
//         */
//        for( Iterator<BlobIV> itr = store.getLexiconRelation().termsIndexScan(); itr.hasNext(); ) {
//            
//            System.err.println("term->id : "+itr.next());
//            
//        }

//        /**
//         * Dumps the id:term index.
//         */
//        for( Iterator<Value> itr = store.getLexiconRelation().idTermIndexScan(); itr.hasNext(); ) {
//            
//            System.err.println("id->term : "+itr.next());
//            
//        }

//        /**
//         * Dumps the terms in term order.
//         */
//        for( Iterator<Value> itr = store.getLexiconRelation().termIterator(); itr.hasNext(); ) {
//            
//            System.err.println("termOrder : "+itr.next());
//            
//        }
//        
//    }
    
}
