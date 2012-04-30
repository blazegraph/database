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
/*
 * Created on Apr 30, 2012
 */
package com.bigdata.rdf.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.bigdata.rdf.ServiceProviderHook;
import com.bigdata.rdf.rio.RDFParserOptions;
import com.bigdata.rdf.vocab.VocabularyDecl;

/**
 * Utility class scans some RDF data, builds up a distribution over the distinct
 * predicates, and generates a {@link VocabularyDecl} for the source data. This
 * can be used to optimize the data density over source data sources.
 * 
 * TODO This could generate one decl per file and then wrap them into a
 * Vocabulary.
 * 
 * @author bryan
 */
public class VocabBuilder {

	private static final Logger log = Logger.getLogger(VocabBuilder.class);
	
    private final RDFParserOptions parserOptions;
	
	private final Map<URI, P> preds = new LinkedHashMap<URI, P>();

	private VocabBuilder() {

		parserOptions = new RDFParserOptions();
		
		parserOptions.setStopAtFirstError(false);
		
		parserOptions.setVerifyData(false);
		
	}
	
    /**
     * 
     * @param source
     * @param baseURL
     * @param rdfFormat
     * 
     * @throws IOException 
     * @throws RDFHandlerException 
     * @throws RDFParseException 
     */
//	private void parse(final Reader source, final String baseURL,
//			final RDFFormat fmt) throws RDFParseException, RDFHandlerException,
//			IOException {
//
//        final RDFParserFactory rdfParserFactory = RDFParserRegistry
//                .getInstance().get(fmt);
//
//        if (rdfParserFactory == null) {
//
//            throw new RuntimeException(
//                    "Parser factory not found: source=" + uriStr
//                            + ", fmt=" + fmt);
//
//        }
//
//        final RDFParser rdfParser = rdfParserFactory.getParser();
////        rdfParser.setValueFactory(database.getValueFactory());
//        rdfParser.setVerifyData(parserOptions.getVerifyData());
//        rdfParser.setStopAtFirstError(parserOptions.getStopAtFirstError());
//        rdfParser.setDatatypeHandling(parserOptions.getDatatypeHandling());
//        rdfParser.setPreserveBNodeIDs(parserOptions.getPreserveBNodeIDs());
//
//        rdfParser.setRDFHandler(new AddStatementHandler());
//
//        /*
//         * Run the parser, which will cause statements to be inserted.
//         */
//
//        rdfParser.parse(source, baseURL);
//
//    }

	protected void loadFiles(final int depth, final File file,
			final String baseURI, final RDFFormat rdfFormat,
			final FilenameFilter filter) throws IOException {

        if (file.isDirectory()) {

            if (log.isDebugEnabled())
                log.debug("loading directory: " + file);

//            final LoadStats loadStats = new LoadStats();

            final File[] files = (filter != null ? file.listFiles(filter)
                    : file.listFiles());

            for (int i = 0; i < files.length; i++) {

                final File f = files[i];

//                final RDFFormat fmt = RDFFormat.forFileName(f.toString(),
//                        rdfFormat);

                loadFiles(depth + 1, f, baseURI, rdfFormat, filter);
                
            }
            
            return;
            
        }
        
        final String n = file.getName();
        
        RDFFormat fmt = RDFFormat.forFileName(n);

        if (fmt == null && n.endsWith(".zip")) {
            fmt = RDFFormat.forFileName(n.substring(0, n.length() - 4));
        }

        if (fmt == null && n.endsWith(".gz")) {
            fmt = RDFFormat.forFileName(n.substring(0, n.length() - 3));
        }

        if (fmt == null) // fallback
            fmt = rdfFormat;

		final RDFParserFactory rdfParserFactory = RDFParserRegistry
				.getInstance().get(fmt);

		if (rdfParserFactory == null) {

			throw new RuntimeException("Parser factory not found: source="
					+ file + ", fmt=" + fmt);

		}

		final RDFParser rdfParser = rdfParserFactory.getParser();
		// rdfParser.setValueFactory(database.getValueFactory());
		rdfParser.setVerifyData(parserOptions.getVerifyData());
		rdfParser.setStopAtFirstError(parserOptions.getStopAtFirstError());
		rdfParser.setDatatypeHandling(parserOptions.getDatatypeHandling());
		rdfParser.setPreserveBNodeIDs(parserOptions.getPreserveBNodeIDs());

		rdfParser.setRDFHandler(new AddStatementHandler());

		InputStream is = null;

        try {


            is = new FileInputStream(file);

            if (n.endsWith(".gz")) {

                is = new GZIPInputStream(is);

            } else if (n.endsWith(".zip")) {

                is = new ZipInputStream(is);

            }

            /*
             * Obtain a buffered reader on the input stream.
             */

			final Reader reader = new BufferedReader(new InputStreamReader(is));

			try {

				// baseURI for this file.
				final String s = baseURI != null ? baseURI : file.toURI()
						.toString();

				rdfParser.parse(reader, s);

				return;

            } catch (Exception ex) {

                throw new RuntimeException("While loading: " + file, ex);

            } finally {

                reader.close();

            }

        } finally {
            
            if (is != null)
                is.close();

        }

    }

//    /**
//	 * Loads data from the <i>source</i>. The caller is responsible for closing
//	 * the <i>source</i> if there is an error.
//	 * 
//	 * @param totals
//	 *            Used to report out the total {@link LoadStats}.
//	 * @param source
//	 *            A {@link Reader} or {@link InputStream}.
//	 * @param baseURL
//	 *            The baseURI (optional, when not specified the name of the each
//	 *            file load is converted to a URL and used as the baseURI for
//	 *            that file).
//	 * @param rdfFormat
//	 *            The format of the file (optional, when not specified the
//	 *            format is deduced for each file in turn using the
//	 *            {@link RDFFormat} static methods).
//	 * @param defaultGraph
//	 *            The value that will be used for the graph/context co-ordinate
//	 *            when loading data represented in a triple format into a quad
//	 *            store.
//	 * @param endOfBatch
//	 *            Signal indicates the end of a batch.
//	 */
//	public void loadData3(final Object source, final String baseURL,
//			final RDFFormat rdfFormat) throws IOException {
//
//		final long begin = System.currentTimeMillis();
//
//        try {
//            
//          /*
//           * Run the parser, which will cause statements to be inserted.
//           */
//
//
//          if(source instanceof Reader) {
//
//				loader.loadRdf((Reader) source, baseURL, rdfFormat,
//						parserOptions);
//
//			} else if (source instanceof InputStream) {
//
//				loader.loadRdf((InputStream) source, baseURL, rdfFormat,
//						parserOptions);
//
//            } else
//                throw new AssertionError();
//
//            
//            return;
//            
//        } catch ( Exception ex ) {
//
//            if (ex instanceof RuntimeException)
//                throw (RuntimeException) ex;
//
//            if (ex instanceof IOException)
//                throw (IOException) ex;
//            
//            final IOException ex2 = new IOException("Problem loading data?");
//            
//            ex2.initCause(ex);
//            
//            throw ex2;
//            
////        } finally {
////            
////            // aggregate regardless of the outcome.
////            totals.add(stats);
//            
//        }
//
//    }
    
    private class AddStatementHandler extends RDFHandlerBase {

		public AddStatementHandler() {
            
        }
        
        public void handleStatement(final Statement stmt)
				throws RDFHandlerException {

			final URI p = stmt.getPredicate();

			P i = preds.get(p);

			if (i == null) {

				preds.put(p, i = new P(p));

			}

			i.cnt++;

		}

	}

	/**
	 * @param args
	 *            The file(s) to read.
	 *            
	 * @throws IOException
	 */
	public static void main(final String[] args) {

		final boolean generate = true;
		
		final int minFreq = 10;
		
		final VocabBuilder v = new VocabBuilder();

		final String baseURI = null; // unless overridden.

		final RDFFormat rdfFormat = RDFFormat.RDFXML; // default

		for (String file : args) {

			try {

				v.loadFiles(0/* depth */, new File(file), baseURI, rdfFormat,
						filter);

			} catch (IOException ex) {
				
				log.error("Could not read: file=" + file, ex);
				
			}

		}
		
		final int size = v.preds.size();
		
		final P[] a = v.preds.values().toArray(new P[size]);
		
		Arrays.sort(a);

		System.out.println("There are " + a.length
				+ " vocabulary items across " + args.length + " files");

		if (!generate) {
			/*
			 * Show on the console.
			 */
			for (int i = 0; i < size; i++) {

				final P p = a[i];

				if (p.cnt < minFreq)
					break;

				System.out.println("" + i + "\t" + p.cnt + "\t" + p.uri);

			}
		} else {
			/*
			 * Generate VocabularyDecl file.
			 */
			final String className = "MyVocabularyDecl";
			
			System.out.println("import java.util.Arrays;");
			System.out.println("import java.util.Collections;");
			System.out.println("import java.util.Iterator;");
			System.out.println("import org.openrdf.model.URI;");
			System.out.println("import org.openrdf.model.impl.URIImpl;");
			System.out.println("import com.bigdata.rdf.vocab.VocabularyDecl;");

			System.out.println("public class "+className+" implements VocabularyDecl {");
			
			System.out.println("static private final URI[] uris = new URI[] {");
			
			for (int i = 0; i < size; i++) {

				final P p = a[i];

				if (p.cnt < minFreq)
					break;

				System.out.println("new URIImpl(\"" + p.uri + "\"), // rank="
						+ i + ", count=" + p.cnt);

			}

			System.out.println("};"); // end uris.

			System.out.println("public " + className + "() {}");

			System.out
					.println("public Iterator<URI> values() {\n"
							+ "return Collections.unmodifiableList(Arrays.asList(uris)).iterator();\n"
							+ "}");

			System.out.println("}"); // end class
			
		}
		

	}

	/**
	 * A vocabulary item together with its frequency count.
	 */
	private static class P implements Comparable<P> {
		/**
		 * The predicate.
		 */
		final URI uri;
		/**
		 * The #of instances of that predicate.
		 */
		int cnt;
		
		public P(final URI uri) {

			this.uri = uri;
			
		}

		/**
		 * Place into descending order by count.
		 */
		@Override
		public int compareTo(final P arg0) {

			return arg0.cnt - cnt;
			
		}
		
	}
	
    /**
     * Note: The filter is chosen to select RDF data files and to allow the data
     * files to use owl, ntriples, etc as their file extension.  gzip and zip
     * extensions are also supported.
     */
    final private static FilenameFilter filter = new FilenameFilter() {

        public boolean accept(final File dir, final String name) {

            if (new File(dir, name).isDirectory()) {

                if(dir.isHidden()) {
                    
                    // Skip hidden files.
                    return false;
                    
                }
                
//                if(dir.getName().equals(".svn")) {
//                    
//                    // Skip .svn files.
//                    return false;
//                    
//                }
                
                // visit subdirectories.
                return true;
                
            }

            // if recognizable as RDF.
            boolean isRDF = RDFFormat.forFileName(name) != null
                    || (name.endsWith(".zip") && RDFFormat.forFileName(name
                            .substring(0, name.length() - 4)) != null)
                    || (name.endsWith(".gz") && RDFFormat.forFileName(name
                            .substring(0, name.length() - 3)) != null);

			if (log.isDebugEnabled())
				log.debug("dir=" + dir + ", name=" + name + " : isRDF=" + isRDF);

            return isRDF;

        }

    };

    /**
     * Force the load of the various integration/extension classes.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/439">
     *      Class loader problems </a>
     */
    static {

        ServiceProviderHook.forceLoad();
        
    }
    
}
