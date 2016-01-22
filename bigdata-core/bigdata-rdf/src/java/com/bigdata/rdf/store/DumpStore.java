package com.bigdata.rdf.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;

/**
 * Utility class may be used to dump out a <em>small</em> database.
 */
public class DumpStore {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {

        // default namespace.
        String namespace = "kb";
        boolean explicit = false;
        boolean inferred = false;
        boolean axioms = false;
        boolean justifications = false;

        int i = 0;

        while (i < args.length) {
            
            final String arg = args[i];

            if (arg.startsWith("-")) {

                if (arg.equals("-namespace")) {

                    namespace = args[++i];

                } else if (arg.equals("-explicit")) {

                    explicit = true;
                    
                } else if (arg.equals("-inferred")) {

                    inferred = true;

                } else if (arg.equals("-axioms")) {

                    axioms = true;
                    
                } else if (arg.equals("-justifications")) {

                    justifications = true;

//                } else {
//
//                    System.err.println("Unknown argument: " + arg);
//
//                    usage();
                    
                }
                
            } else {
                
                break;

            }
            
            i++;
            
        }
        
//        final int remaining = args.length - i;
//
//        if (remaining < 1/*allow run w/o any named files or directories*/) {
//
//            System.err.println("Not enough arguments.");
//
//            usage();
//
//        }

        final File propertyFile = new File(args[i++]);

        if (!propertyFile.exists()) {

            throw new FileNotFoundException(propertyFile.toString());

        }

        final Properties properties = new Properties();
        {
            System.out.println("Reading properties: "+propertyFile);
            final InputStream is = new FileInputStream(propertyFile);
            try {
                properties.load(is);
            } finally {
                if (is != null) {
                    is.close();
                }
            }
        }

        /*
         * Allow override of select options.
         */
        {
            final String[] overrides = new String[] {
                    // Journal options.
                    com.bigdata.journal.Options.FILE,
//                    // RDFParserOptions.
//                    RDFParserOptions.Options.DATATYPE_HANDLING,
//                    RDFParserOptions.Options.PRESERVE_BNODE_IDS,
//                    RDFParserOptions.Options.STOP_AT_FIRST_ERROR,
//                    RDFParserOptions.Options.VERIFY_DATA,
//                    // DataLoader options.
//                    DataLoader.Options.BUFFER_CAPACITY,
//                    DataLoader.Options.CLOSURE,
//                    DataLoader.Options.COMMIT,
//                    DataLoader.Options.FLUSH,
            };
            for (String s : overrides) {
                if (System.getProperty(s) != null) {
                    // Override/set from the environment.
                    final String v = System.getProperty(s);
                    System.out.println("Using: " + s + "=" + v);
                    properties.setProperty(s, v);
                }
            }
        }
        
        Journal jnl = null;
        try {

            jnl = new Journal(properties);

            System.out.println("Journal file: "+jnl.getFile());

            AbstractTripleStore kb = (AbstractTripleStore) jnl
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);

            if (kb == null) {

                throw new RuntimeException("No such namespace: "+namespace);
                
            }
			
			System.out.println(kb.dumpStore(kb/* resolveTerms */, explicit,
					inferred, axioms, justifications));

	    } finally {

            if (jnl != null) {

                jnl.close();

            }
            
        }

	}

}
