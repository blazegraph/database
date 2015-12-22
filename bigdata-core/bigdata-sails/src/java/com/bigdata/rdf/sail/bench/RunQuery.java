package com.bigdata.rdf.sail.bench;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.sail.SailTupleQuery;

import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;

/**
 * Open a journal and run a query against it - pretty much the only reason to
 * use this class is if you want to run the query under a debugger against a
 * local {@link Journal}
 * 
 * @author thompsonbry
 */
public class RunQuery {

	private static final transient Logger log = Logger.getLogger(RunQuery.class);

	private static void usage() {

		System.err.println("(-namespace namespace|-f file) propertyFile");
		
		System.exit(1);
        
    }

	/**
	 * Read a query from a file or stdin and run it against a journal. Use
	 * 
	 * <pre>
	 * -Dcom.bigdata.journal.AbstractJournal.file=...
	 * </pre>
	 * 
	 * to specify the name of the journal file if it is not specified in the
	 * propertyFile.
	 * 
	 * @param args
	 *            (-namespace namespace|-f file) propertyFile
	 */
	public static void main(String[] args) {
		try {
			
			String namespace = null;
			String query = null;

			int i = 0;
			while (i < args.length) {
				final String arg = args[i];
				if (arg.startsWith("-")) {
					if (arg.equals("-namespace")) {
						namespace = args[++i];
					} else if(arg.equals("-f")) {
						query = readFromFile(new File(args[++i]));
					} else {
						System.err.println("Unknown argument: " + arg);
						usage();
					}
				} else {
					break;
				}
				i++;
			}

			final File propertyFile = new File(args[i++]);		
			final Properties properties = new Properties();
	        {
	            
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
//	                    // RDFParserOptions.
//	                    RDFParserOptions.Options.DATATYPE_HANDLING,
//	                    RDFParserOptions.Options.PRESERVE_BNODE_IDS,
//	                    RDFParserOptions.Options.STOP_AT_FIRST_ERROR,
//	                    RDFParserOptions.Options.VERIFY_DATA,
//	                    // DataLoader options.
//	                    DataLoader.Options.BUFFER_CAPACITY,
//	                    DataLoader.Options.CLOSURE,
//	                    DataLoader.Options.COMMIT,
//	                    DataLoader.Options.FLUSH,
	            };
	            for (String s : overrides) {
	                if (System.getProperty(s) != null) {
	                    // Override/set from the environment.
	                    String v = System.getProperty(s);
	                    if (s.equalsIgnoreCase(com.bigdata.journal.Options.FILE))
	                    	v = new File(v).getAbsolutePath();
	                    System.out.println("Using: " + s + "=" + v);
	                    
	                    properties.setProperty(s, v);
	                }
	            }
	        }

	        // override the namespace.
	        if (namespace != null)
	        	properties.setProperty(BigdataSail.Options.NAMESPACE,namespace);
	      
			if (query == null) {
				
				System.err.println("Reading from stdin:");
				
				query = readFromStdin();
				
			}
			
			if (log.isInfoEnabled()) {

				log.info("Query:\n" + query);
				
			}
			
			final BigdataSail sail = new BigdataSail(properties);
			final BigdataSailRepository repository = new BigdataSailRepository(sail);
			repository.initialize();
			
			try {
				
				final BigdataSailRepositoryConnection cxn = 
					(BigdataSailRepositoryConnection) repository.getReadOnlyConnection();

				try {

					final SailTupleQuery tupleQuery = (SailTupleQuery)

					cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);

					tupleQuery.setIncludeInferred(true/* includeInferred */);

					final TupleQueryResult result = tupleQuery.evaluate();

					long nsolutions = 0;

					try {

						while (result.hasNext()) {

							final BindingSet bset = result.next();

							if (log.isInfoEnabled())
								log.info(bset);

							nsolutions++;

						}

					} finally {

						result.close();

					}
					
					System.err.println("Done: #solutions="+nsolutions);

				} finally {
					cxn.close();
				}

			} finally {

				sail.shutDown();

			}

		} catch (Exception e) {
			e.printStackTrace();

			usage();

		}

	}

	/**
	 * Read the contents of a file.
	 * <p>
	 * Note: This makes default platform assumptions about the encoding of the
	 * file.
	 * 
	 * @param file
	 *            The file.
	 * @return The file's contents.
	 * 
	 * @throws IOException
	 */
	static private String readFromFile(final File file) throws IOException {

		final LineNumberReader r = new LineNumberReader(new FileReader(file));

        try {

            final StringBuilder sb = new StringBuilder();

            String s;
            while ((s = r.readLine()) != null) {

                if (r.getLineNumber() > 1)
                    sb.append("\n");

                sb.append(s);

            }

            return sb.toString();

        } finally {

            r.close();

        }

    }

    /**
     * Read from stdin.
     * <p>
     * Note: This makes default platform assumptions about the encoding of the
     * data being read.
     * 
     * @return The data read.
     * 
     * @throws IOException
     */
    static private String readFromStdin() throws IOException {

        final LineNumberReader r = new LineNumberReader(new InputStreamReader(System.in));

        try {

            final StringBuilder sb = new StringBuilder();

            String s;
            while ((s = r.readLine()) != null) {

                if (r.getLineNumber() > 1)
                    sb.append("\n");

                sb.append(s);

            }

            return sb.toString();

        } finally {

            r.close();

        }

    }
    	
}
