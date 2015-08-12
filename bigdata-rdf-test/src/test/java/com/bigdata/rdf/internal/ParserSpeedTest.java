package com.bigdata.rdf.internal;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.bigdata.Banner;

/**
 * Utility to measure the raw speed of the RDF parser.
 * <p>
 * Note: The RIO ntriples parser appears to do about 68k tps flat out on BSBM
 * 200M.
 * 
 * @author thompsonbry
 */
public class ParserSpeedTest {

	private final static Logger log = Logger.getLogger(ParserSpeedTest.class);
	
	/**
	 * Thread pool used to run the parser.
	 */
	private final ExecutorService parserService;

	private final int fileBufSize = 1024 * 8;// default 8k

	private final ValueFactory vf;

	final long begin = System.currentTimeMillis();

	/**
	 * #of statements visited.
	 */
	private final AtomicLong nstmts = new AtomicLong();

	public ParserSpeedTest() {
		
		// It is possible to run multiple parsers.
		this.parserService = Executors.newCachedThreadPool();

		// TODO compare w/ openrdf default value factory....
//		this.vf = BigdataValueFactoryImpl.getInstance("test");
		this.vf = new ValueFactoryImpl();
		
	}

	public void shutdown() {

		parserService.shutdown();
		
	}
	
	private void parseFileOrDirectory(final File fileOrDir)
			throws Exception {

		if (fileOrDir.isDirectory()) {

			final File[] files = fileOrDir.listFiles();

			for (int i = 0; i < files.length; i++) {

				final File f = files[i];

				parseFileOrDirectory(f);

			}

			return;

		}

		final File f = fileOrDir;

		final String n = f.getName();

		RDFFormat fmt = RDFFormat.forFileName(n);

		if (fmt == null && n.endsWith(".zip")) {
			fmt = RDFFormat.forFileName(n.substring(0, n.length() - 4));
		}

		if (fmt == null && n.endsWith(".gz")) {
			fmt = RDFFormat.forFileName(n.substring(0, n.length() - 3));
		}

		if (fmt == null) {
			log.warn("Ignoring: " + f);
			return;
		}

		final StatementHandler stmtHandler = new StatementHandler();

		final FutureTask<Void> ft = new FutureTask<Void>(new ParseFileTask(f,
				fileBufSize, vf, stmtHandler));

		// run the parser
		parserService.submit(ft);

		/*
		 * Await the future.
		 * 
		 * TODO We could run the parsers asynchronously and on a pool with
		 * limited parallelism. We would have to change how we monitor for
		 * errors and the shutdown logic (to wait until all submitted parser
		 * tasks are done).
		 */
		ft.get();

		if (log.isInfoEnabled())
			log.info("Finished parsing: " + f);

	}

	/**
	 * Task parses a single file.
	 * 
	 * @author thompsonbry
	 */
	private static class ParseFileTask implements Callable<Void> {

		private final File file;
		private final int fileBufSize;
		private final ValueFactory vf;
		private final StatementHandler stmtHandler;

		public ParseFileTask(final File file, final int fileBufSize,
				final ValueFactory vf, final StatementHandler stmtHandler) {

			if (file == null)
				throw new IllegalArgumentException();

			if (stmtHandler == null)
				throw new IllegalArgumentException();

			this.file = file;

			this.fileBufSize = fileBufSize;

			this.vf = vf;

			this.stmtHandler = stmtHandler;

		}

		public Void call() throws Exception {

			parseFile(file);

			return (Void) null;

		}

		private void parseFile(final File file) throws IOException,
				RDFParseException, RDFHandlerException,
				NoSuchAlgorithmException, InterruptedException {

			if (!file.exists())
				throw new RuntimeException("Not found: " + file);

			final RDFFormat format = RDFFormat.forFileName(file.getName());

			if (format == null)
				throw new RuntimeException("Unknown format: " + file);

			if (log.isDebugEnabled())
				log.debug("RDFFormat=" + format);

			final RDFParserFactory rdfParserFactory = RDFParserRegistry
					.getInstance().get(format);

			if (rdfParserFactory == null)
				throw new RuntimeException("No parser for format: " + format);

			final RDFParser rdfParser = rdfParserFactory.getParser();

			rdfParser.setValueFactory(vf);

			rdfParser.setVerifyData(false);

			rdfParser.setStopAtFirstError(false);

			rdfParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

			rdfParser.setRDFHandler(stmtHandler);

			/*
			 * Run the parser, which will cause statements to be inserted.
			 */

			if (log.isInfoEnabled())
				log.info("Parsing: " + file);

			InputStream is = new FileInputStream(file);

			try {

				is = new BufferedInputStream(is, fileBufSize);

				final boolean gzip = file.getName().endsWith(".gz");

				if (gzip)
					is = new GZIPInputStream(is);

				final String baseURI = file.toURI().toString();

				// parse the file
				rdfParser.parse(is, baseURI);

			} finally {

				is.close();

			}

		}

	}

	/**
	 * Helper class adds statements to the sail as they are visited by a parser.
	 */
	private class StatementHandler extends RDFHandlerBase {

		public StatementHandler() {

		}

		public void endRDF() {

			if (log.isInfoEnabled())
				log.info("End of source.");

		}

		public void handleStatement(final Statement stmt)
				throws RDFHandlerException {

			final long n = nstmts.incrementAndGet();

			if (n % 10000L == 0) {

				System.out.println("nstmts=" + n + ", tps="
						+ triplesPerSecond());

			}

		}

	} // class StatementHandler

	private long triplesPerSecond() {

		final long elapsed = System.currentTimeMillis() - begin;

		return ((long) (((double) nstmts.get()) / ((double) elapsed) * 1000d));

	}

	/**
	 * Parse some data.
	 * 
	 * @param args
	 *            The file(s) or directory(s) containing the data to be parsed.
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Banner.banner();

		// check args.
		{

			for (String filename : args) {

				final File file = new File(filename);

				if (!file.exists())
					throw new RuntimeException("Not found: " + file);

			}

		}

		final ParserSpeedTest u = new ParserSpeedTest();
		
		try {

			for (String filename : args) {

				u.parseFileOrDirectory(new File(filename));

			}

		} finally {

			u.shutdown();

			final long elapsed = System.currentTimeMillis() - u.begin;

			System.out.println("nstmts=" + u.nstmts + ", tps="
					+ u.triplesPerSecond() + ", elapsed=" + elapsed);

		}

	}

}
