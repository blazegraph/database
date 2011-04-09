package com.bigdata.rdf.sail.webapp;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.BigdataContext.Config;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.JiniClient;

/**
 * Utility class provides a simple SPARQL end point with a REST API.
 * 
 * @author thompsonbry
 * @author martyncutcher
 * 
 * @see https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer
 */
public class NanoSparqlServer {
	
	static private final Logger log = Logger.getLogger(NanoSparqlServer.class);

	/**
	 * Run an httpd service exposing a SPARQL endpoint. The service will respond
	 * to the following URL paths:
	 * <dl>
	 * <dt>http://localhost:port/</dt>
	 * <dd>The SPARQL end point for the default namespace as specified by the
	 * <code>namespace</code> command line argument.</dd>
	 * <dt>http://localhost:port/namespace/NAMESPACE</dt>
	 * <dd>where <code>NAMESPACE</code> is the namespace of some triple store or
	 * quad store, may be used to address ANY triple or quads store in the
	 * bigdata instance.</dd>
	 * <dt>http://localhost:port/status</dt>
	 * <dd>A status page.</dd>
	 * </dl>
	 * 
	 * @param args
	 *            USAGE:<br/>
	 *            To start the server:<br/>
	 *            <code>(options) <i>namespace</i> (propertyFile|configFile) )</code>
	 *            <p>
	 *            <i>Where:</i>
	 *            <dl>
	 *            <dt>port</dt>
	 *            <dd>The port on which the service will respond -or-
	 *            <code>0</code> to use any open port.</dd>
	 *            <dt>namespace</dt>
	 *            <dd>The namespace of the default SPARQL endpoint (the
	 *            namespace will be <code>kb</code> if none was specified when
	 *            the triple/quad store was created).</dd>
	 *            <dt>propertyFile</dt>
	 *            <dd>A java properties file for a standalone {@link Journal}.</dd>
	 *            <dt>configFile</dt>
	 *            <dd>A jini configuration file for a bigdata federation.</dd>
	 *            </dl>
	 *            and <i>options</i> are any of:
	 *            <dl>
	 *            <dt>-q</dt>
	 *            <dd>Suppress messages on stdout.</dd>
	 *            <dt>-nthreads</dt>
	 *            <dd>The #of threads which will be used to answer SPARQL
	 *            queries (default 8).</dd>
	 *            <dt>-forceOverflow</dt>
	 *            <dd>Force a compacting merge of all shards on all data
	 *            services in a bigdata federation (this option should only be
	 *            used for benchmarking purposes).</dd>
	 *            <dt>readLock</dt>
	 *            <dd>The commit time against which the server will assert a
	 *            read lock by holding open a read-only transaction against that
	 *            commit point. When given, queries will default to read against
	 *            this commit point. Otherwise queries will default to read
	 *            against the most recent commit point on the database.
	 *            Regardless, each query will be issued against a read-only
	 *            transaction.</dt>
	 *            </dl>
	 *            </p>
	 */
//	 *            To stop the server:<br/>
//	 *            <code>port -stop</code><br/>
//	 *            <dt>bufferCapacity [#bytes]</dt>
//	 *            <dd>Specify the capacity of the buffers used to decouple the
//	 *            query evaluation from the consumption of the HTTP response by
//	 *            the client. The capacity may be specified in bytes or
//	 *            kilobytes, e.g., <code>5k</code>.</dd>
	static public void main(String[] args) throws Exception {
		// PropertyConfigurator.configure("C:/CT_Config/ct_test_log4j.properties");

		final Config config = new Config();
		/*
		 * Connect to the database.
		 */
		final IIndexManager indexManager;
		boolean forceOverflow = false;
		Journal jnl = null;
		JiniClient<?> jiniClient = null;
		ITransactionService txs = null;
		Long readLock = null;
		JettySparqlServer server = null;

		try {
//			/*
//			 * First, handle the [port -stop] command, where "port" is the port
//			 * number of the service. This case is a bit different because the
//			 * "-stop" option appears after the "port" argument.
//			 */
//			if (args.length == 2) {
//				if ("-stop".equals(args[1])) {
//					final int port;
//					try {
//						port = Integer.valueOf(args[0]);
//					} catch (NumberFormatException ex) {
//						usage(1/* status */, "Could not parse as port# : '" + args[0] + "'");
//						// keep the compiler happy wrt [port] being bound.
//						throw new AssertionError();
//					}
//					// Send stop to server.
//					sendStop(port);
//					// Normal exit.
//					System.exit(0);
//				} else {
//					usage(1/* status */, null/* msg */);
//				}
//			}

			/*
			 * Now that we have that case out of the way, handle all arguments
			 * starting with "-". These should appear before any non-option
			 * arguments to the program.
			 */
			int i = 0;
			while (i < args.length) {
				final String arg = args[i];
				if (arg.startsWith("-")) {
					if (arg.equals("-q")) {
						config.quiet = true;
					} else if (arg.equals("-forceOverflow")) {
						forceOverflow = true;
					} else if (arg.equals("-nthreads")) {
						final String s = args[++i];
						config.queryThreadPoolSize = Integer.valueOf(s);
						if (config.queryThreadPoolSize < 0) {
							usage(1/* status */, "-nthreads must be non-negative, not: " + s);
						}
//					} else if (arg.equals("-bufferCapacity")) {
//						final String s = args[++i];
//						final long tmp = BytesUtil.getByteCount(s);
//						if (tmp < 1) {
//							usage(1/* status */, "-bufferCapacity must be non-negative, not: " + s);
//						}
//						if (tmp > Bytes.kilobyte32 * 100) {
//							usage(1/* status */, "-bufferCapacity must be less than 100kb, not: " + s);
//						}
//						config.bufferCapacity = (int) tmp;
					} else if (arg.equals("-readLock")) {
						final String s = args[++i];
						readLock = Long.valueOf(s);
						if (!TimestampUtility.isCommitTime(readLock.longValue())) {
							usage(1/* status */, "Read lock must be commit time: " + readLock);
						}
					} else {
						usage(1/* status */, "Unknown argument: " + arg);
					}
				} else {
					break;
				}
				i++;
			}

			/*
			 * Finally, there should be exactly THREE (3) command line arguments
			 * remaining. These are the [port], the [namespace] and the
			 * [propertyFile] (journal) or [configFile] (scale-out).
			 */
			final int nremaining = args.length - i;
			if (nremaining != 3) {
				/*
				 * There are either too many or too few arguments remaining.
				 */
				usage(1/* status */, nremaining < 3 ? "Too few arguments." : "Too many arguments");
			}
			/*
			 * http service port.
			 */
			{
				final String s = args[i++];
				try {
					config.port = Integer.valueOf(s);
				} catch (NumberFormatException ex) {
					usage(1/* status */, "Could not parse as port# : '" + s + "'");
				}
			}
			/*
			 * Namespace.
			 */
			config.namespace = args[i++];
			/*
			 * Property file.
			 */
			final String propertyFile = args[i++];
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
				usage(1/* status */, "File should have '.config' or '.properties' extension: " + file);
			}
			if (!config.quiet) {
				System.out.println("port: " + config.port);
				System.out.println("namespace: " + config.namespace);
				System.out.println("file: " + file.getAbsoluteFile());
			}

			{

				if (isJini) {

					/*
					 * A bigdata federation.
					 */

					jiniClient = new JiniClient(new String[] { propertyFile });

					indexManager = jiniClient.connect();

				} else {

					/*
					 * Note: we only need to specify the FILE when re-opening a
					 * journal containing a pre-existing KB.
					 */
					final Properties properties = new Properties();
					{
						// Read the properties from the file.
						final InputStream is = new BufferedInputStream(new FileInputStream(propertyFile));
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

					indexManager = jnl = new Journal(properties);

				}

			}

			txs = (indexManager instanceof Journal ? ((Journal) indexManager).getTransactionManager()
					.getTransactionService() : ((IBigdataFederation<?>) indexManager).getTransactionService());

			if (readLock != null) {

				/*
				 * Obtain a read-only transaction which will assert a read lock
				 * for the specified commit time. The database WILL NOT release
				 * storage associated with the specified commit point while this
				 * server is running. Queries will read against the specified
				 * commit time by default, but this may be overridden on a query
				 * by query basis.
				 */
				config.timestamp = txs.newTx(readLock);

				if (!config.quiet) {

					System.out.println("Holding read lock: readLock=" + readLock + ", tx: " + config.timestamp);

				}

			} else {

				/*
				 * The default for queries is to read against then most recent
				 * commit time as of the moment when the request is accepted.
				 */

				config.timestamp = ITx.READ_COMMITTED;

			}

			if (log.isInfoEnabled()) {
				/*
				 * Log some information about the default kb (#of statements,
				 * etc).
				 */
				// FIXME log.info("\n" + server.getKBInfo(config.namespace,
				// config.timestamp));
			}

			if (forceOverflow && indexManager instanceof IBigdataFederation<?>) {

				if (!config.quiet)
					System.out.println("Forcing compacting merge of all data services: " + new Date());

				((AbstractDistributedFederation<?>) indexManager)
						.forceOverflow(true/* compactingMerge */, false/* truncateJournal */);

				if (!config.quiet)
					System.out.println("Did compacting merge of all data services: " + new Date());

			}

			 server = new JettySparqlServer(config.port);
			 
			 server.startup(config, indexManager);

			if (!config.quiet) {

				log.warn("Service is running: port=" + server.m_port);

			}

			System.out.println("NanoSparqlServer running..");

			server.join(); // wait for server to exit

			System.out.println("NanoSparqlServer stopping!");
			
		} catch (Throwable ex) {
			ex.printStackTrace();
		} finally {
			if (txs != null && readLock != null) {
				try {
					txs.abort(config.timestamp);
				} catch (IOException e) {
					log.error("Could not release transaction: tx=" + config.timestamp, e);
				}
			}
//			if (server != null)
//				server.shutdownNow();
//			if (jnl != null) {
//				jnl.close();
//			}
			if (jiniClient != null) {
				jiniClient.disconnect(true/* immediateShutdown */);
			}
		}
	}

    /**
     * Send a STOP message to the service
     * 
     * @param port
     *            The port for that service.
     * 
     * @throws IOException
     * 
     * @todo This winds up warning <code>
     * java.net.SocketTimeoutException: Read timed out
     * </code> even though the shutdown request was
     *       accepted and processed by the server. I'm not sure why.
     */
    public static void sendStop(final int port) throws IOException {

        final URL url = new URL("http://localhost:" + port + "/stop");
        HttpURLConnection conn = null;
        try {

            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoInput(true); // true to read from the server.
            conn.setDoOutput(true); // true to write to the server.
            conn.setUseCaches(false);
            conn.setReadTimeout(2000/* ms */);
            conn.setRequestProperty("Content-Type",
                    "application/x-www-form-urlencoded");
            conn.setRequestProperty("Content-Length", "" + Integer.toString(0));
            conn.setRequestProperty("Content-Language", "en-US");

            // Send request
            conn.getOutputStream().close();

            // connect.
			try {

				conn.connect();

				final int rc = conn.getResponseCode();
				
				if (rc < 200 || rc >= 300) {
					
					log.error(conn.getResponseMessage());
					
				}

				System.out.println(conn.getResponseMessage());

			} catch (IOException ex) {

				log.warn(ex);
				
			}

		} finally {

			// clean up the connection resources
			if (conn != null)
				conn.disconnect();

		}

	}

    /**
	 * Print the optional message on stderr, print the usage information on
	 * stderr, and then force the program to exit with the given status code.
	 * 
	 * @param status
	 *            The status code.
	 * @param msg
	 *            The optional message
	 */
	private static void usage(final int status, final String msg) {

		if (msg != null) {

			System.err.println(msg);

		}

		System.err.println("[options] port namespace (propertyFile|configFile)");

//		System.err.println("-OR-");
//
//		System.err.println("port -stop");

		System.exit(status);

	}

}
