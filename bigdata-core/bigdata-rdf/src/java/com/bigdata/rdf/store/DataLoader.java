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
/*
 * Created on Nov 1, 2007
 */

package com.bigdata.rdf.store;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.Banner;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.DumpJournal;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RWStrategy;
import com.bigdata.rdf.ServiceProviderHook;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.load.IStatementBufferFactory;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RDFParserOptions;
import com.bigdata.rdf.rio.RioLoaderEvent;
import com.bigdata.rdf.rio.RioLoaderListener;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.spo.SPO;

/**
 * A utility class to load RDF data into an {@link AbstractTripleStore}. This
 * class supports a number of options, including a durable queues pattern, and
 * can be more efficient if multiple files are batched into a single commit
 * point. The {@link #main(String[]) main routine} will open the {@link Journal}
 * itself and therefore this class can not be used while the {@link Journal} is
 * open in the webapp.
 * <p>
 * Note: This class is not efficient for scale-out.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @see com.bigdata.rdf.load.MappedRDFDataLoadMaster
 */
public class DataLoader {

    /**
     * Logger.
     */
    protected static final transient Logger log = Logger.getLogger(DataLoader.class);

    private final RDFParserOptions parserOptions;
    


    /**
     * 
     * The {@link StatementBuffer} capacity.
     */
    private final int bufferCapacity;
    
    /**
	 * The capacity of the blocking queue for the backing
	 * {@link StatementBuffer}.
	 */
    private final int queueCapacity;
    
    /**
     * Utility to allow other {@link PrintStream} to be used for status.
     */
    private PrintStream output;
    
    /**
     * The target database.
     */
    private final AbstractTripleStore database;
    
    /**
     * The target database.
     */
    public AbstractTripleStore getDatabase() {
        
        return database;
        
    }
    
    /**
     * The object used to compute entailments for the database.
     */
    private final InferenceEngine inferenceEngine;
    
    /**
     * The object used to maintain the closure for the database iff incremental
     * truth maintenance is enabled.
     */
    private final TruthMaintenance tm;
    
    /**
     * The object used to compute entailments for the database.
     */
    public InferenceEngine getInferenceEngine() {
        
        return inferenceEngine;
        
    }
    
    /**
     * Used to buffer writes.
     * 
     * @see #getAssertionBuffer()
     */
    private StatementBuffer<?> buffer;
    
    /**
     * Return the assertion buffer.
     * <p>
     * The assertion buffer is used to buffer statements that are being asserted
     * so as to maximize the opportunity for batch writes. Truth maintenance (if
     * enabled) will be performed no later than the commit of the transaction.
     * <p>
     * Note: The same {@link #buffer} is reused by each loader so that we can on
     * the one hand minimize heap churn and on the other hand disable auto-flush
     * when loading a series of small documents. However, we obtain a new buffer
     * each time we perform incremental truth maintenance.
     * <p>
     * Note: When non-<code>null</code> and non-empty, the buffer MUST be
     * flushed (a) if a transaction completes (otherwise writes will not be
     * stored on the database); or (b) if there is a read against the database
     * during a transaction (otherwise reads will not see the unflushed
     * statements).
     * <p>
     * Note: if truthMaintenance is enabled then this buffer is backed by a
     * temporary store which accumulates the {@link SPO}s to be asserted.
     * Otherwise it will write directly on the database each time it is flushed,
     * including when it overflows.
     * 
     * @todo this should be refactored as an {@link IStatementBufferFactory}
     *       where the appropriate factory is required for TM vs non-TM
     *       scenarios (or where the factory is parameterize for tm vs non-TM).
     */
    @SuppressWarnings("rawtypes")
   synchronized protected StatementBuffer<?> getAssertionBuffer() {

        if (buffer == null) {

            if (tm != null) {

                buffer = new StatementBuffer(tm.newTempTripleStore(),
                        database, bufferCapacity, queueCapacity);

            } else {

                buffer = new StatementBuffer(database, bufferCapacity);

            }

        }
        
        return buffer;
        
    }

    /**
     * when true a durable queues pattern will be applied when loading from files.
     * 
	 * @see BLZG-1534 (durable queues)
	 * 
	 * @see Options#DURABLE_QUEUES
     */
    private final boolean durableQueues;

    /**
	 * When true, calls through to {@link #logCounters(AbstractTripleStore)} at
	 * each commit point.
	 * 
	 * @see Options#VERBOSE
	 */
    private final int verbose;

	/**
	 * when true, run DumpJournal after each commit (only makes sense in batch
	 * mode and even then only to gain detailed statistics on the branching
	 * factors as they evolve in a large bulk load).
	 * 
	 * @see BLZG-1535 (support dump journal)
	 * @see Options#DUMP_JOURNAL
	 */
    private final boolean dumpJournal;
    
    private final CommitEnum commitEnum;
    
    private final ClosureEnum closureEnum;
    
    private final boolean flush;
    
    /**
	 * When true, do not stop if there is a fatal error from rio for an input
	 * source.
	 * 
	 * @see BLZG-1531 (Add option to make the DataLoader robust to files that
	 *      cause rio to throw a fatal exception)
	 */
    private final boolean ignoreInvalidFiles;
    
//    public boolean setFlush(boolean newValue) {
//        
//        boolean ret = this.flush;
//        
//        this.flush = newValue;
//        
//        return ret;
//        
//    }
    
    /**
     * When <code>true</code> (the default) the {@link StatementBuffer} is
     * flushed by each {@link #loadData(String, String, RDFFormat)} or
     * {@link #loadData(String[], String[], RDFFormat[])} operation and when
     * {@link #doClosure()} is requested. When <code>false</code> the caller
     * is responsible for flushing the {@link #buffer}.
     * <p>
     * This behavior MAY be disabled if you want to chain load a bunch of small
     * documents without flushing to the backing store after each document and
     * {@link #loadData(String[], String[], RDFFormat[])} is not well-suited to
     * your purposes. This can be much more efficient, approximating the
     * throughput for large document loads. However, the caller MUST invoke
     * {@link #endSource()} once all documents are loaded successfully. If an error
     * occurs during the processing of one or more documents then the entire
     * data load should be discarded.
     * 
     * @return The current value.
     * 
     * @see Options#FLUSH
     */
    public boolean getFlush() {
        
        return flush;
        
    }
    
    /**
     * Flush the {@link StatementBuffer} to the backing store.
     * <p>
     * Note: If you disable auto-flush AND you are not using truth maintenance
     * then you MUST explicitly invoke this method once you are done loading
     * data sets in order to flush the last chunk of data to the store. In all
     * other conditions you do NOT need to call this method. However it is
     * always safe to invoke this method - if the buffer is empty the method
     * will be a NOP.
     */
    public void endSource() {

        if (buffer != null) {

            if(log.isDebugEnabled())
                log.debug("Flushing the buffer.");
            
            buffer.flush();
            
        }
        
    }
    
    /**
     * How the {@link DataLoader} will maintain closure on the database.
     */
    public ClosureEnum getClosureEnum() {
        
        return closureEnum;
        
    }

    /**
     * Whether and when the {@link DataLoader} will invoke
     * {@link ITripleStore#commit()}
     */
    public CommitEnum getCommitEnum() {
        
        return commitEnum;
        
    }

    /**
     * A type-safe enumeration of options effecting whether and when the database
     * will be committed.
     * 
     * @see ITripleStore#commit()
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static enum CommitEnum {
        
        /**
         * Commit as each document is loaded into the database.
         */
        Incremental,
        
        /**
         * Commit after each set of documents has been loaded into the database.
         */
        Batch,

        /**
         * The {@link DataLoader} will NOT commit the database - this is left to
         * the caller.
         */
        None;
        
    }
    
    /**
     * A type-safe enumeration of options effecting whether and when entailments
     * are computed as documents are loaded into the database using the
     * {@link DataLoader}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static enum ClosureEnum {
        
        /**
         * Document-at-a-time closure.
         * <p>
         * Each documents is loaded separately into a temporary store, the
         * temporary store is closed against the database, and the results of
         * the closure are transferred to the database.
         */
        Incremental,
        
        /**
         * Set-of-documents-at-a-time closure.
         * <p>
         * A set of documents are loaded into a temporary store, the temporary
         * store is closed against the database, and the results of the closure
         * are transferred to the database. maintaining closure.
         */
        Batch,

        /**
         * Closure is not maintained as documents are loaded.
         * <p>
         * You can always use the {@link InferenceEngine} to (re-)close a
         * database. If explicit statements MAY have been deleted, then you
         * SHOULD first delete all inferences before re-computing the closure.
         */
        None;
        
    }

    /**
     * Options for the {@link DataLoader}.
     * 
     * Note: The default for {@link RDFParserOptions.Options#PRESERVE_BNODE_IDS}
     * is conditionally overridden when
     * {@link LexiconRelation#isStoreBlankNodes()} is <code>true</code>.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static interface Options extends RDFParserOptions.Options {

		/**
		 * 
		 * Java property to override the default GZIP buffer size used for
		 * {@link GZipInputStream} and {@link GZipOutputStream}.
		 * 
		 * This specifies the size in Bytes to use. The default is 65535.
		 * 
		 * -Dcom.bigdata.journal.DataLoader.gzipBufferSize=65535
		 * 
		 * See BLZG-1777
		 * 
		 */

		static final String GZIP_BUFFER_SIZE = DataLoader.class
				.getClass().getName() + ".gzipBufferSize";
		
		static final int DEFAULT_GZIP_BUFFER_SIZE = 65535;

        /**
         * Optional property specifying whether and when the {@link DataLoader}
         * will {@link ITripleStore#commit()} the database (default
         * {@value CommitEnum#Batch}).
         * <p>
         * Note: commit semantics vary depending on the specific backing store.
         * See {@link ITripleStore#commit()}.
         */
        static final String COMMIT = DataLoader.class.getName()+".commit";
        
        static final String DEFAULT_COMMIT = CommitEnum.Batch.toString();

        /**
         * Optional property specifying the capacity of the
         * {@link StatementBuffer} (default is {@value #DEFAULT_BUFFER_CAPACITY}
         * statements).
		 * <p>
		 * Note: With BLGZ-1522, the {@link #QUEUE_CAPACITY} can increase the
		 * effective amount of data that is being buffered quite significantly.
		 * Caution is recommended when overriding the {@link #BUFFER_CAPACITY}
		 * in combination with a non-zero value of the {@link #QUEUE_CAPACITY}.
		 * The best performance will probably come from small (20k - 50k) buffer
		 * capacity values combined with a queueCapacity of 5-20. Larger values
		 * will increase the GC burden and could require a larger heap, but the
		 * net throughput might also increase.
         */
        static final String BUFFER_CAPACITY = DataLoader.class.getName()+".bufferCapacity";
        
        static final String DEFAULT_BUFFER_CAPACITY = "100000";

		/**
		 * Optional property specifying the capacity of blocking queue used by
		 * the {@link StatementBuffer} -or- ZERO (0) to disable the blocking
		 * queue and perform synchronous writes (default is
		 * {@value #DEFAULT_QUEUE_CAPACITY} statements). The blocking queue
		 * holds parsed data pending writes onto the backing store and makes it
		 * possible for the parser to race ahead while writer is blocked writing
		 * onto the database indices.
		 * 
		 * @see BLZG-1552
		 */
		static final String QUEUE_CAPACITY = DataLoader.class.getName() + ".queueCapacity";

		static final String DEFAULT_QUEUE_CAPACITY = "10";

        /**
         * Optional property controls whether and when the RDFS(+) closure is
         * maintained on the database as documents are loaded (default
         * {@value ClosureEnum#Batch}).
         * <p>
         * Note: The {@link InferenceEngine} supports a variety of options. When
         * closure is enabled, the caller's {@link Properties} will be used to
         * configure an {@link InferenceEngine} object to compute the
         * entailments. It is VITAL that the {@link InferenceEngine} is always
         * configured in the same manner for a given database with regard to
         * options that control which entailments are computed using forward
         * chaining and which entailments are computed using backward chaining.
         * <p>
         * Note: When closure is being maintained the caller's
         * {@link Properties} will also be used to provision the
         * {@link TempTripleStore}.
         * 
         * @see InferenceEngine
         * @see InferenceEngine.Options
         */
        static final String CLOSURE = DataLoader.class.getName()+".closure";
        
        static final String DEFAULT_CLOSURE = ClosureEnum.Batch.toString();

        /**
         * 
         * When <code>true</code> the {@link StatementBuffer} is flushed by each
         * {@link DataLoader#loadData(String, String, RDFFormat)} or
         * {@link DataLoader#loadData(String[], String[], RDFFormat[])}
         * operation and when {@link DataLoader#doClosure()} is requested. When
         * <code>false</code> the caller is responsible for flushing the
         * {@link #buffer}. The default is {@value #DEFAULT_FLUSH}.
         * <p>
         * This behavior MAY be disabled if you want to chain load a bunch of
         * small documents without flushing to the backing store after each
         * document and
         * {@link DataLoader#loadData(String[], String[], RDFFormat[])} is not
         * well-suited to your purposes. This can be much more efficient,
         * approximating the throughput for large document loads. However, the
         * caller MUST invoke {@link DataLoader#endSource()} (or
         * {@link DataLoader#doClosure()} if appropriate) once all documents are
         * loaded successfully. If an error occurs during the processing of one
         * or more documents then the entire data load should be discarded (this
         * is always true).
         * <p>
         * <strong>This feature is most useful when blank nodes are not in use,
         * but it causes memory to grow when blank nodes are in use and forces
         * statements using blank nodes to be deferred until the application
         * flushes the {@link DataLoader} when statement identifiers are
         * enabled. </strong>
         */
        static final String FLUSH = DataLoader.class.getName()+".flush";
        
        /**
         * The default value (<code>true</code>) for {@link #FLUSH}.
         */
        static final String DEFAULT_FLUSH = "true";
        
        /**
		 * When <code>true</code>, the loader will not break on unresolvable
		 * parse errors, but instead skip the file containing the error. This
		 * option is useful when loading large input that may contain invalid
		 * RDF, in order to make sure that the loading process does not fully
		 * fail when malicious files are detected. Note that an error will still
		 * be logged in case files cannot be loaded, so one is able to track the
		 * files that failed.
		 * 
		 * @see BLZG-1531 (Add option to make the DataLoader robust to files
		 *      that cause rio to throw a fatal exception)
		 */
        static final String IGNORE_INVALID_FILES = DataLoader.class.getName()+".ignoreInvalidFiles";
        
        /**
         * The default value (<code>false</code>) for {@link #IGNORE_INVALID_FILES)
         */
        static final String DEFAULT_IGNORE_INVALID_FILES = "false";

        /**
		 * When <code>true</code>, the data loader will rename each file as it
		 * is processed to either <code>file.good</code> or <code>file.fail</code>
		 * to indicate success or failure. In addition, the default for
		 * {@link #IGNORE_INVALID_FILES} will be <code>true</code> and the
		 * default for {@link RDFParserOptions#getStopAtFirstError()} will be
		 * <code>false</code>.
		 * 
		 * @see BLZG-1534 (durable queues)
		 */
		static final String DURABLE_QUEUES = DataLoader.class.getName() + ".durableQueues";

		/**
		 * The default value (<code>false</code>) for {@link #DURABLE_QUEUES)
		 */
		static final String DEFAULT_DURABLE_QUEUES = "false";

		/**
		 * When true, runs DumpJournal after each commit (with the -pages option) to obtain a distribution of the BTree index page sizes.
		 * 
		 * @see BLZG-1535 (support dump journal in data loader)
		 */
		static final String DUMP_JOURNAL = DataLoader.class.getName() + ".dumpJournal";

		/**
		 * The default value (<code>false</code>) for {@link #DUMP_JOURNAL)
		 */
		static final String DEFAULT_DUMP_JOURNAL = "false";

		/**
		 * When greater than ZERO (0), significant information may be reported
		 * at each commit point. At ONE (1) it enables a trace of the parser
		 * performance (statements loaded, statements per second, etc). At TWO
		 * (2) it provides detailed information about the performance counters
		 * at each commit. At THREE (3) it provides additional information about
		 * the assertion buffers each time it reports on the incremental parser
		 * performance.
		 */
		static final String VERBOSE = DataLoader.class.getName() + ".verbose";

		/**
		 * The default value (<code>0</code>) for {@link #VERBOSE)
		 */
		static final String DEFAULT_VERBOSE = "0";

    }

    /**
     * Configure {@link DataLoader} using properties used to configure the
     * database.
     * 
     * @param database
     *            The database.
     */
    public DataLoader(final AbstractTripleStore database) {
        
        this(database.getProperties(), database , System.out);
        
    }
    
	public DataLoader(final Properties properties,
			final AbstractTripleStore database) {
		this(properties, database, System.out);
	}

    /**
     * Configure a data loader with overridden properties.
     * 
     * @param properties
     *            Configuration properties - see {@link Options}.
     * 
     * @param database
     *            The database.
     *            
     * @param os
     * 			  The {@link PrintStream} for output messages              
     *            
     */
    public DataLoader(final Properties properties,
            final AbstractTripleStore database, final PrintStream os) {
    	
    	output = os;

        if (properties == null)
            throw new IllegalArgumentException();

        if (database == null)
            throw new IllegalArgumentException();

        // setup the parser options.
        {
            
            this.parserOptions = new RDFParserOptions(properties);

            if ((properties.getProperty(Options.PRESERVE_BNODE_IDS) == null)
                    && database.getLexiconRelation().isStoreBlankNodes()) {

                /*
                 * Note: preserveBNodeIDs is overridden based on whether or not
                 * the target is storing the blank node identifiers (unless the
                 * property was explicitly set - this amounts to a conditional
                 * default).
                 */

                parserOptions.setPreserveBNodeIDs(true);

            }

        }

        { // durableQueues. See BLZG-1534

        	durableQueues = Boolean
					.parseBoolean(properties.getProperty(Options.DURABLE_QUEUES, Options.DEFAULT_DURABLE_QUEUES));

			if (durableQueues) {

				// Implied defaults when using the durable queues pattern.
				properties.setProperty(Options.IGNORE_INVALID_FILES, "true");
				properties.setProperty(RDFParserOptions.Options.STOP_AT_FIRST_ERROR, "false");
			}

			if (log.isInfoEnabled())
				log.info(Options.DURABLE_QUEUES + "=" + durableQueues);
		}

        {
			commitEnum = CommitEnum.valueOf(properties.getProperty(Options.COMMIT, Options.DEFAULT_COMMIT));

			if (log.isInfoEnabled())
				log.info(Options.COMMIT + "=" + commitEnum);
		}
        
		{
			closureEnum = database.getAxioms().isNone() ? ClosureEnum.None
					: (ClosureEnum.valueOf(properties.getProperty(Options.CLOSURE, Options.DEFAULT_CLOSURE)));

			if (log.isInfoEnabled())
				log.info(Options.CLOSURE + "=" + closureEnum);
		}

		{
			bufferCapacity = Integer
					.parseInt(properties.getProperty(Options.BUFFER_CAPACITY, Options.DEFAULT_BUFFER_CAPACITY));

			if (log.isInfoEnabled())
				log.info(Options.BUFFER_CAPACITY+ "=" + bufferCapacity);
		}
		
		{
			queueCapacity = Integer
					.parseInt(properties.getProperty(Options.QUEUE_CAPACITY, Options.DEFAULT_QUEUE_CAPACITY));

			if (log.isInfoEnabled())
				log.info(Options.QUEUE_CAPACITY + "=" + queueCapacity);
			
		}

        this.database = database;

        inferenceEngine = database.getInferenceEngine();

        if (closureEnum != ClosureEnum.None) {

            /*
             * Truth maintenance: buffer will write on a tempStore.
             */

//            inferenceEngine = database.getInferenceEngine();

            tm = new TruthMaintenance(inferenceEngine);

        } else {

            /*
             * No truth maintenance: buffer will write on the database.
             */

//            inferenceEngine = null;

            tm = null;

        }

		{
			flush = Boolean.parseBoolean(properties.getProperty(Options.FLUSH, Options.DEFAULT_FLUSH));

			if (log.isInfoEnabled())
				log.info(Options.FLUSH + "=" + flush);
		}

        // ignoreInvalidFiles. See BLZG-1531
		{
			ignoreInvalidFiles = Boolean.parseBoolean(
					properties.getProperty(Options.IGNORE_INVALID_FILES, Options.DEFAULT_IGNORE_INVALID_FILES));

			if (log.isInfoEnabled())
				log.info(Options.IGNORE_INVALID_FILES + "=" + ignoreInvalidFiles);

		}

		{ // verbose.
			
			verbose = Integer
					.parseInt(properties.getProperty(Options.VERBOSE, Options.DEFAULT_VERBOSE));

			if (log.isInfoEnabled())
				log.info(Options.VERBOSE + "=" + verbose);

		}
		
		{ // dumpJournal. See BLZG-1535
			
			dumpJournal = Boolean
					.parseBoolean(properties.getProperty(Options.DUMP_JOURNAL, Options.DEFAULT_DUMP_JOURNAL));

			if (log.isInfoEnabled())
				log.info(Options.DUMP_JOURNAL + "=" + dumpJournal);
		}

    }

    public class MyLoadStats extends LoadStats {
    	
    	/**
    	 * The set of resources that failed during a load.
    	 */
		private final Set<File> failSet = new LinkedHashSet<File>();
		
		/**
		 * The set of resources that were successfully loaded.
		 */
		private final Set<File> goodSet = new LinkedHashSet<File>();
    	
		/**
		 * Method must be invoked if load of a {@link File} fails.
		 */
		void didFail(final File file) {

			failSet.add(file);
			
			/*
			 * Immediately rename failures. They are failed regardless of
			 * whether we commit.
			 */
			if (durableQueues && !file.renameTo(new File(file.getPath() + ".fail"))) {
			
				log.error("File rename failed: file=" + file + " (fail)");
				
			}
			
		}

		/**
		 * Method must be invoked if load of a {@link File} succeeds.
		 */
		void didGood(final File file) {
			goodSet.add(file);
		}
		
		@Override
		public String toString() {
			
			return super.toString() + ", {failSet=" + failSet.size() + ",goodSet=" + goodSet.size() + "}";
			
		}

		@Override
		public void add(final LoadStats stats) {

			super.add(stats);
			
			if (stats instanceof MyLoadStats) {
			
				failSet.addAll(((MyLoadStats) stats).failSet);
				
				goodSet.addAll(((MyLoadStats) stats).goodSet);
				
			}
			
		}

		/**
		 * Hook used to support durable queues pattern when the rename happens
		 * only once we actually go through a commit.
		 * 
		 * @see BLZG-1534 (durable queues)
		 */
		public void commit() {

			if (durableQueues) {

				// Rename the file whose contents were made restart safe on the db.
				for (File file : goodSet) {
					if (!file.renameTo(new File(file.getPath() + ".good"))) {
						log.error("File rename failed: file=" + file + " (good)");
					}
				}

			}

			failSet.clear();
			goodSet.clear();

		}
		
    } // MyLoadStats
    
    /**
     * Factory for {@link DataLoader} specific {@link LoadStats} extension.
     */
    public MyLoadStats newLoadStats() {
		
    	return new MyLoadStats();
    	
	}

    /**
	 * Load a resource into the associated triple store and commit.
	 * 
	 * @param resource
	 *            A resource to be loaded (required).
	 * @param baseURL
	 *            The baseURL to use for that resource (required).
	 * @param rdfFormat
	 *            The {@link RDFFormat} to use as a fall back for the resource
	 *            (required).
	 * 
	 * @return Statistics about the load.
	 * 
	 * @throws IOException
	 */
    final public LoadStats loadData(final String resource, final String baseURL,
            final RDFFormat rdfFormat) throws IOException {

        if (resource == null)
            throw new IllegalArgumentException();

        if (baseURL == null)
            throw new IllegalArgumentException();

        if (rdfFormat == null)
            throw new IllegalArgumentException();

        return loadData(//
                new String[] { resource }, //
                new String[] { baseURL },//
                new RDFFormat[] { rdfFormat }//
                );

    }
    
    /**
	 * Load a set of RDF resources into the associated triple store and commit.
	 * 
	 * @param resource
	 *            An array of resources to be loaded (required).
	 * @param baseURL
	 *            An array baseURL to use for those resources (required and must
	 *            be 1:1 with the array of resources).
	 * @param rdfFormat
	 *            An array of {@link RDFFormat} values to use as a fall back for
	 *            each resource (required and must be 1:1 with the array of
	 *            resources).
	 * 
	 * @return Statistics about the load.
	 * 
	 * @throws IOException
	 */
	final public LoadStats loadData(final String[] resource, final String[] baseURL, final RDFFormat[] rdfFormat)
			throws IOException {

		if (resource.length != baseURL.length)
			throw new IllegalArgumentException();

		if (resource.length != rdfFormat.length)
			throw new IllegalArgumentException();

        if (log.isInfoEnabled())
            log.info("commit=" + commitEnum + ", closure=" + closureEnum
                    + ", resource=" + Arrays.toString(resource));

        final MyLoadStats totals = newLoadStats();

        for (int i = 0; i < resource.length; i++) {

            final boolean endOfBatch = i + 1 == resource.length;

            loadData2(//
                    totals,//
                    resource[i],//
                    baseURL[i],//
                    rdfFormat[i],//
                    endOfBatch//
                    );
            
        }

        doCommit(totals);
        
		if (log.isInfoEnabled())
			log.info("Loaded " + resource.length + " resources: " + totals);

        return totals;
        
    }

	/**
	 * Helper method for top-level loadXXX() methods. This method flushes the
	 * buffer and commits (if there is anything to commit).
	 * 
	 * @param totals
	 */
	private void doCommit(final MyLoadStats totals) {

		if (flush && buffer != null) {

			// Flush the buffer after the document(s) have been loaded.

			buffer.flush();

		}

		if (commitEnum == CommitEnum.Batch) {

			if (log.isInfoEnabled())
				log.info("Commit after batch");

			final long beginCommit = System.currentTimeMillis();

			database.commit(); // database commit.

			totals.commit(); // Note: durable queues pattern.

			totals.commitTime.add(System.currentTimeMillis() - beginCommit);

			if (log.isInfoEnabled())
				log.info("commit: latency=" + totals.commitTime + "ms");

			if (verbose > 1)
				logCounters(database);

		}

	}
	
    /**
	 * Load from a reader and commit.
	 * 
	 * @param reader
	 *            The reader (required).
	 * @param baseURL
	 *            The base URL (required).
	 * @param rdfFormat
	 *            The {@link RDFFormat} to use as a fallback (required).
	 *            
	 * @return Statistics about the load.
	 * 
	 * @throws IOException
	 */
    public LoadStats loadData(final Reader reader, final String baseURL,
            final RDFFormat rdfFormat) throws IOException {

        final MyLoadStats totals = newLoadStats();

		/*
		 * We are not processing Files so the durable queues pattern does
		 * not apply and we can call a method that handles the
		 * RDFParseException for us.
		 */
		loadData4_ParserErrors_Trapped(totals, reader, baseURL/* label */, baseURL, rdfFormat, null/* defaultGraph */,
				true/* endOfBatch */);

		doCommit(totals);
		
        return totals;

    }

    /**
     * Load from an input stream.
     * 
     * @param is
     *            The input stream (required).
     * @param baseURL
     *            The base URL (required).
     * @param rdfFormat
     *            The format (required).
     * @return
     * @throws IOException
     */
    public LoadStats loadData(final InputStream is, final String baseURL,
            final RDFFormat rdfFormat) throws IOException {

        final MyLoadStats totals = newLoadStats();

		/*
		 * We are not processing Files so the durable queues pattern does
		 * not apply and we can call a method that handles the
		 * RDFParseException for us.
		 */
		loadData4_ParserErrors_Trapped(totals, is, baseURL/* label */, baseURL, rdfFormat, null/* defaultGraph */,
				true/* endOfBatch */);

		doCommit(totals);
		
        return totals;

    }

    /**
	 * Load from a {@link URL}. If in quads mode, the triples in the default
	 * graph will be inserted into the named graph associate with the specified
	 * <code>url</code>.
	 * 
	 * @param url
	 *            The URL (required).
	 * @param baseURL
	 *            The base URL (required).
	 * @param rdfFormat
	 *            The {@link RDFFormat} (required).
	 * @return
	 * @throws IOException
	 */
	public LoadStats loadData(final URL url, final String baseURL, final RDFFormat rdfFormat) throws IOException {

		if (url == null)
			throw new IllegalArgumentException();

		if (log.isInfoEnabled())
			log.info("loading: " + url);

		final MyLoadStats totals = newLoadStats();

		final InputStream is = url.openStream();

		try {

			/*
			 * We are not processing Files so the durable queues pattern does
			 * not apply and we can call a method that handles the
			 * RDFParseException for us.
			 */
			loadData4_ParserErrors_Trapped(totals, is, url.toString()/* label */, baseURL, rdfFormat,
					url.toString()/* defaultGraph */, true/* endOfBatch */);

		} finally {

			is.close();

		}

		doCommit(totals);

		return totals;
    
    }

	/**
     * Load an RDF resource into the database.
     * 
     * @param resource
     *            Either the name of a resource which can be resolved using the
     *            CLASSPATH, or the name of a resource in the local file system,
     *            or a URL.
     * @param baseURL
     * @param rdfFormat
     * @param endOfBatch
     * @return
     * 
     * @throws IOException
     *             if the <i>resource</i> can not be resolved or loaded.
     */
    protected void loadData2(final MyLoadStats totals, final String resource,
            final String baseURL, final RDFFormat rdfFormat,
            final boolean endOfBatch) throws IOException {

        if (log.isInfoEnabled())
            log.info("loading: " + resource);

        // The stringValue() of the URI of the resource from which the data will
        // be read.
        String defaultGraph = null;
        
        // try the classpath
        InputStream rdfStream = getClass().getResourceAsStream(resource);

        if (rdfStream != null)
            defaultGraph = getClass().getResource(resource).toString();

        if (rdfStream == null) {

            // Searching for the resource from the root of the class returned
            // by getClass() (relative to the class' package) failed.
            // Next try searching for the desired resource from the root
            // of the jar; that is, search the jar file for an exact match
            // of the input string.
            rdfStream = getClass().getClassLoader().getResourceAsStream(
                    resource);
            
            if (rdfStream != null)
                defaultGraph = getClass().getClassLoader()
                        .getResource(resource).toString();

            if (rdfStream == null) {

                /*
                 * If we do not find as a Resource then try the file system.
                 */

                final File file = new File(resource);

                if (file.exists()) {

                    defaultGraph = file.toURI().toString();
                    
                    loadFiles(totals, 0/* depth */, file, baseURL, rdfFormat,
                            defaultGraph, filter, endOfBatch);

                    return;

                }

            }

        }

        /* 
         * Obtain a buffered reader on the input stream.
         */
        
        if (rdfStream == null) {

            throw new IOException("Could not locate resource: " + resource);
            
        }

        // @todo reuse the backing buffer to minimize heap churn. 
        final Reader reader = new BufferedReader(
                new InputStreamReader(rdfStream)
//               , 20*Bytes.kilobyte32 // use a large buffer (default is 8k)
                );

        try {

			/*
			 * We are not processing Files so the durable queues pattern does
			 * not apply and we can call a method that handles the
			 * RDFParseException for us.
			 */
			loadData4_ParserErrors_Trapped(totals, reader, resource/* label */, baseURL, rdfFormat, defaultGraph, endOfBatch);

        } catch (Exception ex) {

			if (ex instanceof RuntimeException)
				throw (RuntimeException) ex;

			if (ex instanceof IOException)
				throw (IOException) ex;

            throw new RuntimeException("While loading: " + resource, ex);

        } finally {

            reader.close();

            rdfStream.close();

        }
        
    }

    /**
     * 
     * @param file
     *            The file or directory (required).
     * @param baseURI
     *            The baseURI (optional, when not specified the name of the each
     *            file load is converted to a URL and used as the baseURI for
     *            that file).
     * @param rdfFormat
     *            The format of the file (optional, when not specified the
     *            format is deduced for each file in turn using the
     *            {@link RDFFormat} static methods).
     * @param defaultGraph
     *            The value that will be used for the graph/context co-ordinate when
     *            loading data represented in a triple format into a quad store.
     * @param filter
     *            A filter selecting the file names that will be loaded
     *            (optional). When specified, the filter MUST accept directories
     *            if directories are to be recursively processed.
     * 
     * @return The aggregated load statistics.
     * 
     * @throws IOException
     */
    public LoadStats loadFiles(final File file, final String baseURI,
            final RDFFormat rdfFormat, final String defaultGraph,
            final FilenameFilter filter)
            throws IOException {

        if (file == null)
            throw new IllegalArgumentException();
        
        final MyLoadStats totals = newLoadStats();

        loadFiles(totals, 0/* depth */, file, baseURI, rdfFormat, defaultGraph, filter, true/* endOfBatch */
        );

        doCommit(totals);
        
        return totals;

    }

	/**
	 * Recursive load of a file or directory.
	 * 
	 * @param totals
	 * @param depth
	 * @param file
	 * @param baseURI
	 * @param rdfFormat
	 * @param defaultGraph
	 * @param filter
	 * @param endOfBatch
	 * @throws IOException
	 */
    public void loadFiles(final MyLoadStats totals, final int depth,
            final File file, final String baseURI, final RDFFormat rdfFormat,
            final String defaultGraph, final FilenameFilter filter,
            final boolean endOfBatch)
            throws IOException {

        if (file.isDirectory()) {

            if (log.isDebugEnabled())
                log.debug("loading directory: " + file);

//            final LoadStats loadStats = new LoadStats();

            final File[] files = (filter != null ? file.listFiles(filter)
                    : file.listFiles());
            Arrays.sort(files);

            for (int i = 0; i < files.length; i++) {

                final File f = files[i];

//                final RDFFormat fmt = RDFFormat.forFileName(f.toString(),
//                        rdfFormat);

                loadFiles(totals, depth + 1, f, baseURI, rdfFormat, defaultGraph, filter,
                        (depth == 0 && i < (files.length-1) ? false : endOfBatch));
                
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
                
        InputStream is = null;
        
        if(log.isInfoEnabled())
        	log.info("Loading next file: " + file + " now...");

        try {

            is = new FileInputStream(file);

            if (n.endsWith(".gz")) {

                is = new GZIPInputStream(is, getGzipBuffer());

            } else if (n.endsWith(".zip")) {

				is = new ZipInputStream(new BufferedInputStream(is,
						getGzipBuffer()));

            }

            /*
             * Obtain a buffered reader on the input stream.
             */

            // @todo reuse the backing buffer to minimize heap churn.
            final Reader reader = new BufferedReader(new InputStreamReader(is)
            // , 20*Bytes.kilobyte32 // use a large buffer (default is 8k)
            );

            try {

				// baseURI for this file.
				final String s = baseURI != null ? baseURI : file.toURI().toString();

				loadData4_ParserErrors_Not_Trapped(totals, reader, file.toString()/* label */,
						file/* fileIfSourceIfFile */, s, fmt, defaultGraph, endOfBatch);
                
                return;

			} catch (RDFParseException ex) {

				if (ignoreInvalidFiles) {

    				/*
					 * Log warning and DO NOT rethrow the exception.
					 * 
					 * Note: The file will still be entered into the "failSet"
					 * if durable queues are enabled.
					 */
					
    				log.error("Parser error - skipping source: source=" + file, ex);

				} else
					throw new RuntimeException("Could not parse file: " + file, ex);

            } catch (Exception ex) {

				if (ex instanceof RuntimeException)
					throw (RuntimeException) ex;

				if (ex instanceof IOException)
					throw (IOException) ex;

				// throw a runtime exception, causing an abort
				throw new RuntimeException("While loading: " + file, ex);
               
            } finally {

            	// Note: Must close() before renameTo().
                reader.close();

            }

        } finally {
            
            if (is != null)
                is.close();

        }

    }

    /**
	 * Loads data from the <i>source</i>. The caller is responsible for closing
	 * the <i>source</i> if there is an error.
	 * 
	 * @param totals
	 *            Used to report out the total {@link LoadStats}.
	 * @param source
	 *            A {@link Reader} or {@link InputStream}.
	 * @param baseURL
	 *            The baseURI (optional, when not specified the name of the each
	 *            file load is converted to a URL and used as the baseURI for
	 *            that file).
	 * @param rdfFormat
	 *            The format of the file (optional, when not specified the
	 *            format is deduced for each file in turn using the
	 *            {@link RDFFormat} static methods).
	 * @param defaultGraph
	 *            The value that will be used for the graph/context co-ordinate
	 *            when loading data represented in a triple format into a quad
	 *            store.
	 * @param endOfBatch
	 *            Signal indicates the end of a batch.
	 */
    @Deprecated
	protected void loadData3(final LoadStats totals, final Object source, final String baseURL,
			final RDFFormat rdfFormat, final String defaultGraph, final boolean endOfBatch) throws IOException {

		final MyLoadStats tmp = newLoadStats();

		loadData4_ParserErrors_Trapped(tmp, source, null/* label */, baseURL, rdfFormat, defaultGraph, endOfBatch);

		totals.add(tmp);

	}

    /*
     * Code path for non-File loads. 
     */
	private void loadData4_ParserErrors_Trapped(final MyLoadStats totals, final Object source, String label,
			final String baseURI, final RDFFormat rdfFormat, final String defaultGraph, final boolean endOfBatch)
					throws IOException {

		try {

			loadData4_ParserErrors_Not_Trapped(totals, source, label, null/* fileIfSourceIsFile */, baseURI, rdfFormat,
					defaultGraph, endOfBatch);

		} catch (RDFParseException ex) {

			if (ignoreInvalidFiles) {

				// log warning
				log.error("Parser error - skipping source: source=" + label, ex);

				// Note: Do not throw out an exception.
				return;

			}

			throw new RuntimeException(ex);
			
		}

	}

   	/**
	 * Loads data from the <i>source</i> (core method). The caller is
	 * responsible for closing the <i>source</i> if there is an error.
	 * 
	 * @param totals
	 *            Used to report out the total {@link LoadStats}.
	 * @param source
	 *            A {@link Reader} or {@link InputStream}.
	 * @param label
	 *            A label used to report error messages (optional, defaults to
	 *            baseURL if given and otherwise source.toString()).
	 * @param baseURI
	 *            The baseURI (optional, when not specified the name of the each
	 *            file load is converted to a URL and used as the baseURI for
	 *            that file).
	 * @param rdfFormat
	 *            The format of the file (optional, when not specified the
	 *            format is deduced for each file in turn using the
	 *            {@link RDFFormat} static methods).
	 * @param defaultGraph
	 *            The value that will be used for the graph/context co-ordinate
	 *            when loading data represented in a triple format into a quad
	 *            store.
	 * @param endOfBatch
	 *            Signal indicates the end of a batch.
	 * 
	 * @throws IOException
	 *             if there is a problem reading the source.
	 * @throws RDFParseException
	 *             if there is a RIO level parser error.
	 */
	private void loadData4_ParserErrors_Not_Trapped(final MyLoadStats totals, final Object source, String label,
			final File fileIfSourceIsFile, final String baseURI, final RDFFormat rdfFormat, final String defaultGraph,
			final boolean endOfBatch) throws IOException, RDFParseException {

		if (label == null) {
			// Use baseURI
			label = baseURI;
			if (label == null) {
				// Note: messages will not have a useful label in this case.
				label = source.toString();
			}
		}
	   
        final long begin = System.currentTimeMillis();
        
        final MyLoadStats stats = new MyLoadStats();
        
        // Note: allocates a new buffer iff the [buffer] is null.
        getAssertionBuffer();
        
        /*
		 * Nope! We do not call reset() here. The buffer is non-empty when
		 * flush:=false.
		 * 
		 * @see BLZG-1562 (DataLoader.Options.FLUSH does not defer flush of
		 * StatementBuffer)
		 */
//		if (!buffer.isEmpty()) {
//            
//            /*
//             * Note: this is just paranoia. If the buffer is not empty when we
//             * are starting to process a new document then either the buffer was
//             * not properly cleared in the error handling for a previous source
//             * or the DataLoader instance is being used by concurrent threads.
//             */
//            
//            buffer.reset();
//            
//        }
        
		// Setup the loader. Flush buffer at end of source iff flush:=true.
        final PresortRioLoader loader = new PresortRioLoader ( buffer, flush ) ;

        // @todo review: disable auto-flush - caller will handle flush of the buffer.
//        loader.setFlush(false);
        // add listener to log progress.
        loader.addRioLoaderListener( new RioLoaderListener() {

            @Override
            public void processingNotification( final RioLoaderEvent e ) {
				/*
				 * This reports as statements are parsed. Depending on how
				 * things are buffered, the parser can run ahead of the index
				 * writes.
				 */
				if (log.isInfoEnabled() || verbose > 1) {
					final String msg = e.getStatementsProcessed() + " stmts buffered in "
							+ (e.getTimeElapsed() / 1000d) + " secs, rate= "
							+ e.getInsertRate()
							+ (baseURI != null ? ", baseURL=" + baseURI : "") + //
							(", totalStatementsSoFar="//
							+ (e.getStatementsProcessed()//
							+ totals.toldTriples.get())//
							);//

					if (log.isInfoEnabled())
						log.info(msg);

					if (verbose > 1)
						output.println(msg);
				}

				if (verbose > 2) {
					// Show more details, especially about the assertion buffers.
					final StatementBuffer<?> tmp = buffer;
					if (tmp != null) {
						output.println(tmp.toString());
						output.println(tmp.getCounters().toString());
					}
				}
                
            }
            
        });

        try {

			boolean ok = false;
			try {
				
				if (source instanceof Reader) {

					loader.loadRdf((Reader) source, baseURI, rdfFormat, defaultGraph, parserOptions);

				} else if (source instanceof InputStream) {

					loader.loadRdf((InputStream) source, baseURI, rdfFormat, defaultGraph, parserOptions);

				} else
					throw new AssertionError();

				ok = true;

			} finally {

				if (fileIfSourceIsFile != null) {
				
					/*
					 * Record output in support of durable queues pattern.
					 * 
					 * Note: We need to defer the rename until the next
					 * commit(). So we just make a note of the outcome here.
					 */
					
					if (ok) {
					
						stats.didGood(fileIfSourceIsFile);
						
					} else {
						
						stats.didFail(fileIfSourceIsFile);
						
					}

				}

			}
            
            final long nstmts = loader.getStatementsAdded();

            stats.toldTriples.set( nstmts );

            stats.loadTime.set(System.currentTimeMillis() - begin);

            if (closureEnum == ClosureEnum.Incremental
                    || (endOfBatch && closureEnum == ClosureEnum.Batch)) {

                /*
                 * compute the closure.
                 * 
                 * @todo batch closure logically belongs in the outer method.
                 */

                if (log.isInfoEnabled())
                    log.info("Computing closure.");

                stats.closureStats.add(doClosure());

            }

            // commit the data.  
            if (commitEnum == CommitEnum.Incremental) {

                if(log.isInfoEnabled())
                    log.info("Commit after each resource");

                final long beginCommit = System.currentTimeMillis();

                database.commit(); // database commit.

                stats.commit(); // Note: durable queues pattern.

                stats.commitTime.set(System.currentTimeMillis() - beginCommit);

                if (log.isInfoEnabled())
                    log.info("commit: latency=" + stats.commitTime + "ms");

				if (verbose > 1)
					logCounters(database);
    			
            }

            stats.totalTime.set(System.currentTimeMillis() - begin);

            // aggregate stats
            totals.add(stats);

            if (log.isInfoEnabled()) {
				log.info("file:: " + stats + "; totals:: " + totals
						+ (baseURI != null ? "; baseURL=" + baseURI : ""));
                if (buffer != null
                        && buffer.getDatabase() instanceof AbstractLocalTripleStore) {
                	if(log.isDebugEnabled())
                    log.debug(((AbstractLocalTripleStore) buffer.getDatabase())
                            .getLocalBTreeBytesWritten(new StringBuilder())
                            .toString());
                }
            }
            
            return;
            
        } catch ( Throwable t ) {

        	// aggregate stats even for exceptions.
            totals.add(stats);

            /*
             * Note: discard anything in the buffer in case auto-flush is
             * disabled. This prevents the buffer from retaining data after a
             * failed load operation. The caller must still handle the thrown
             * exception by discarding the writes already on the backing store
             * (that is, by calling abort()).
             */

			if (buffer != null) {

                // clear any buffer statements.
                buffer.reset();

                if (tm != null) {
                    
                    // delete the tempStore if truth maintenance is enabled.
                    buffer.getStatementStore().close();
                    
                }

                buffer = null;
                
            }

			if (t instanceof RuntimeException)
				throw (RuntimeException) t;

			if (t instanceof RDFParseException)
				throw (RDFParseException) t;

            if (t instanceof IOException)
                throw (IOException) t;
            
            final IOException ex2 = new IOException("Problem loading data?");
            
            ex2.initCause(t);
            
            throw ex2;
            
        }

    }

   	/**
	 * Report out a variety of interesting information on stdout and the
	 * {@link #log}.
	 * 
	 * @param database
	 * 
	 * @see Options#VERBOSE
	 */
    public void logCounters(final AbstractTripleStore database) {

		final IIndexManager store = database.getIndexManager();
		
		final CounterSet counters = store.getCounters();

		{
			final StatementBuffer<?> tmp = buffer;
			if (tmp != null) {
				counters.makePath("assertionBuffer").attach(buffer.getCounters());
			}
		}

		output.println(counters.toString());

		/*
		 * This provides total page bytes written per index and average page
		 * size by index. Use DumpJournal for detailed histogram of index page
		 * size distribution.
		 */
		System.out
				.println(((AbstractLocalTripleStore) database)
						.getLocalBTreeBytesWritten(new StringBuilder())
						.toString());

    	if (!(store instanceof Journal))
			return;
		
    	// RWStore only. Allocators also dumped by DumpJournal.
		if (((Journal) store).getBufferStrategy() instanceof RWStrategy && !dumpJournal) {

			final StringBuilder sb = new StringBuilder("\n");

			((RWStrategy) ((Journal) store).getBufferStrategy()).getStore().showAllocators(sb);

			log.info(sb.toString());
		}

		if (dumpJournal) {
			final PrintWriter out = new PrintWriter(System.out);
			new DumpJournal((Journal) store).dumpJournal(out, null/* namespaces */, false/* dumpHistory */,
					true/* dumpPages */, false/* dumpIndices */, false/* showTuples */);
			out.flush();
			out.close();
		}

//    	if(true) {
//			/*
//			 * Remove this. It assumes that the journal has only one
//			 * triple store.
//			 */
//			final long extent = ((Journal)store).getBufferStrategy().getExtent();
//			final long stmts = database.getStatementCount();
//			final long bytesPerStmt = stmts==0?0:(extent/stmts);
//			log.info("extent=" + extent + ", stmts=" + stmts + ", bytes/stat="
//					+ bytesPerStmt);
//    	}
    	
	}
    
    /**
     * Compute closure as configured. If {@link ClosureEnum#None} was selected
     * then this MAY be used to (re-)compute the full closure of the database.
     * 
     * @see #removeEntailments()
     * 
     * @throws IllegalStateException
     *             if assertion buffer is <code>null</code>
     */
    public ClosureStats doClosure() {
        
        final ClosureStats stats;
        
        switch (closureEnum) {

        case Incremental:
        case Batch: {

            /*
             * Incremental truth maintenance.
             */
            
            if (buffer == null)
                throw new IllegalStateException();
            
            // flush anything in the buffer.
            buffer.flush();
            
            stats = new TruthMaintenance(inferenceEngine)
                    .assertAll((TempTripleStore) buffer.getStatementStore());
            
            /*
             * Discard the buffer since the backing tempStore was closed when
             * we performed truth maintenance.
             */
            
            buffer = null;

            break;

        }

        case None: {

            /*
             * Close the database against itself.
             * 
             * Note: if there are already computed entailments in the database
             * AND any explicit statements have been deleted then the caller
             * needs to first delete all entailments from the database.
             */

            stats = inferenceEngine.computeClosure(null/* focusStore */);
            
            break;
            
        }

        default:
            throw new AssertionError();

        }

        return stats;
        
    }

    /**
     * Utility to return the gzip buffer either from the 
     * default or the {@link Options#GZIP_BUFFER_SIZE} 
     * 
     * See BLZG-1777
     * 
     * @return
     * 			int with the buffer size
     */
	private static int getGzipBuffer() {

		final String s = System.getProperty(Options.GZIP_BUFFER_SIZE);

		if (s == null || s.isEmpty()) {
			return Options.DEFAULT_GZIP_BUFFER_SIZE;
		} else {
			return Integer.parseInt(s);
		}

	}


    /**
	 * Utility method may be used to create and/or load RDF data into a local
	 * database instance. Directories will be recursively processed. The data
	 * files may be compressed using zip or gzip, but the loader does not
	 * support multiple data files within a single archive.
	 * 
	 * @param args
	 *            <code>[-quiet][-closure][-verbose][-durableQueues][-namespace <i>namespace</i>] propertyFile (fileOrDir)*</code>
	 *            where
	 *            <dl>
	 *            <dt>-quiet</dt>
	 *            <dd>Suppress all stdout messages.</dd>
	 *            <dt>-verbose</dt>
	 *            <dd>Show additional messages detailing the load performance.
	 *            This may be specified more than once to increase the amount of
	 *            information reported. This is a shorthand for
	 *            {@link Options#VERBOSE}.</dd>
	 *            <dt>-closure</dt>
	 *            <dd>Compute the RDF(S)+ closure.</dd>
	 *            <dt>-durableQueues</dt>
	 *            <dd>Files will be renamed to either <code>.good</code> or
	 *            <code>.fail</code> as they are processed. The files will
	 *            remain in the same directory. The changes the default for
	 *            {@link Options#IGNORE_INVALID_FILES} to <code>true</code> and
	 *            the default for
	 *            {@link RDFParserOptions.Options#STOP_AT_FIRST_ERROR} to
	 *            <code>false</code>. Failures can be detected by looking for
	 *            ".fail" files. (This is a shorthand for
	 *            {@link Options#DURABLE_QUEUES}.)</dd>
	 *            <dt>-namespace</dt>
	 *            <dd>The namespace of the KB instance.</dd>
	 *            <dt>propertyFile</dt>
	 *            <dd>The configuration file for the database instance.</dd>
	 *            <dt>fileOrDir</dt>
	 *            <dd>Zero or more files or directories containing the data to
	 *            be loaded.</dd>
	 *            </dl>
	 * 
	 * @throws IOException
	 * @see BLZG-1534 (durable queues)
	 */
    public static void main(final String[] args) throws IOException {
    	
    	Banner.banner();
    	
        // default namespace.
        String namespace = "kb";
        boolean doClosure = false;
        int verbose = 0;
        boolean quiet = false;
        boolean durableQueues = false;
        RDFFormat rdfFormat = null;
        String defaultGraph = null;
        String baseURI = null;
        
        int i = 0;

        while (i < args.length) {
            
            final String arg = args[i];

            if (arg.startsWith("-")) {

                if (arg.equals("-namespace")) {

                    namespace = args[++i];

                } else if (arg.equals("-format")) {

                    rdfFormat = RDFFormat.valueOf(args[++i]);

                } else if (arg.equals("-baseURI")) {

                    baseURI = args[++i];

                } else if (arg.equals("-defaultGraph")) {

                	defaultGraph = args[++i];
                    if (defaultGraph.length() == 0)
                    	defaultGraph = null;

                } else if (arg.equals("-closure")) {

                    doClosure = true;

                } else if (arg.equals("-durableQueues")) {

                    durableQueues = true;
                    
                } else if (arg.equals("-verbose")) {

                    verbose++;
                    quiet = false;

                } else if (arg.equals("-quiet")) {

                    quiet = true;
                    verbose = 0;

                } else {

                    System.err.println("Unknown argument: " + arg);

                    usage();
                    
                }
                
            } else {
                
                break;

            }
            
            i++;
            
        }
        
        final int remaining = args.length - i;

        if (remaining < 1/*allow run w/o any named files or directories*/) {

            System.err.println("Not enough arguments.");

            usage();

        }
        
        final String propertyFileName = args[i++];
        
        final List<File> files = new LinkedList<File>();
		
		final Properties properties = processProperties(propertyFileName, quiet, verbose, durableQueues);
        
		while (i < args.length) {

            final File fileOrDir = new File(args[i++]);
            
            if(!fileOrDir.exists()) {
                
                throw new FileNotFoundException(fileOrDir.toString());
                
            }
            
            files.add(fileOrDir);
            
            if(!quiet)
                System.out.println("Will load from: " + fileOrDir);

        }
            
        Journal jnl = null;
        try {

        	final long begin = System.currentTimeMillis();
        	
            jnl = new Journal(properties);
            
//            // #of bytes on the journal before (user extent).
////            final long firstOffset = jnl.getRootBlockView().getNextOffset();
//            final long userData0 = jnl.getBufferStrategy().size();

			if (!quiet)
                System.out.println("Journal file: "+jnl.getFile());

            AbstractTripleStore kb = (AbstractTripleStore) jnl
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);

            if (kb == null) {

                kb = new LocalTripleStore(jnl, namespace, Long
                        .valueOf(ITx.UNISOLATED), properties);

                kb.create();
                
            }

            final DataLoader dataLoader = //kb.getDataLoader();
            	new DataLoader(properties,kb, System.out); // use the override properties.
            
            final MyLoadStats totals = dataLoader.newLoadStats();
            
            for (File fileOrDir : files) {

//                dataLoader.loadFiles(fileOrDir, null/* baseURI */,
//                        rdfFormat, filter);

                dataLoader.loadFiles(totals, 0/* depth */, fileOrDir, baseURI,
                        rdfFormat, defaultGraph, filter, true/* endOfBatch */
                );

            }
            
            dataLoader.endSource();

			if(!quiet)
			    System.out.println("Load: " + totals);
			
        	if (dataLoader.closureEnum == ClosureEnum.None && doClosure) {

				if (verbose > 0)
					dataLoader.logCounters(dataLoader.database);

				if (!quiet)
					System.out.println("Computing closure.");
				log.info("Computing closure.");

				final ClosureStats stats = dataLoader.doClosure();

				if (!quiet)
					System.out.println("Closure: " + stats.toString());

				if (log.isInfoEnabled())
					log.info("Closure: " + stats.toString());

			}

            jnl.commit(); // database commit.

            totals.commit(); // Note: durable queues pattern.
			
			if (verbose > 1)
				dataLoader.logCounters(dataLoader.database);

			/*
			 * Note: This value is not correct for the RWStore. It is the
			 * difference in the extents, not the bytes actually written.
			 */
			//            // #of bytes on the journal (user data only).
//            final long userData1 = jnl.getBufferStrategy().size();
//            
//            // #of bytes written (user data only)
//            final long bytesWritten = (userData1 - userData0);
//
//            if (!quiet)
//                System.out.println("Wrote: " + bytesWritten + " bytes.");

            final long elapsedTotal = System.currentTimeMillis() - begin;

            if (!quiet)
                System.out.println("Total elapsed=" + elapsedTotal + "ms");
            if (log.isInfoEnabled())
                log.info("Total elapsed=" + elapsedTotal + "ms");

        } finally {

            if (jnl != null) {

                jnl.close();

            }
            
        }

    }

   public static Properties processProperties(final String propertyFileName, final boolean quiet, 
		   final int verbose, final boolean durableQueues ) throws IOException {

        final File propertyFile = new File(propertyFileName);

        if (!propertyFile.exists()) {

            throw new FileNotFoundException(propertyFile.toString());

        }

        final Properties properties = new Properties();
        {
            if(!quiet)
                System.out.println("Reading properties: "+propertyFile);
            final InputStream is = new FileInputStream(propertyFile);
            try {
                properties.load(is);
            } finally {
                if (is != null) {
                    is.close();
                }
            }
//			if (System.getProperty(com.bigdata.journal.Options.FILE) != null) {
//				// Override/set from the environment.
//				final String file = System
//						.getProperty(com.bigdata.journal.Options.FILE);
//				if(!quiet) System.out.println("Using: " + com.bigdata.journal.Options.FILE
//						+ "=" + file);
//                properties.setProperty(com.bigdata.journal.Options.FILE, file);
//            }
        }
		if (durableQueues) {
			// @see BLZG-1534 (durable queues)
			properties.setProperty(Options.DURABLE_QUEUES, "true");
		}
		if (verbose > 0) {
			properties.setProperty(Options.VERBOSE, Integer.toString(verbose));
		}
        /*
         * Allow override of select options.
         */
        {
            final String[] overrides = new String[] {
                    // Journal options.
                    com.bigdata.journal.Options.FILE,
                    // RDFParserOptions.
                    RDFParserOptions.Options.DATATYPE_HANDLING,
                    RDFParserOptions.Options.PRESERVE_BNODE_IDS,
                    RDFParserOptions.Options.STOP_AT_FIRST_ERROR,
                    RDFParserOptions.Options.VERIFY_DATA,
                    // DataLoader options.
                    DataLoader.Options.BUFFER_CAPACITY,
                    DataLoader.Options.QUEUE_CAPACITY,
                    DataLoader.Options.CLOSURE,
                    DataLoader.Options.COMMIT,
                    DataLoader.Options.FLUSH,
                    DataLoader.Options.IGNORE_INVALID_FILES,
                    DataLoader.Options.DURABLE_QUEUES,
                    DataLoader.Options.DUMP_JOURNAL,
                    DataLoader.Options.VERBOSE,
                    // Useful Journal options.
                    Journal.Options.WRITE_CACHE_BUFFER_COUNT,
                    Journal.Options.WRITE_CACHE_MIN_CLEAN_LIST_SIZE,
                    // HttpPlugin
                    com.bigdata.journal.HttpPlugin.Options.HTTPD_PORT,
                    // DirectBufferPool options.
                    com.bigdata.io.DirectBufferPool.Options.BUFFER_CAPACITY,
                    // B+Tree
                    com.bigdata.btree.IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY,
                    // Index procedure // FIXME Remove or replace with symbolic Options.
                    // @see BLZG-1537 (Schedule more IOs when loading data)
                    "com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.maxReaders",
                    "com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.skipCount",
                    "com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.spannedRangeMultiplier",
                    "com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.batchSize",
                    "com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.queueCapacity",

            };
            for (String s : overrides) {
                if (System.getProperty(s) != null) {
                    // Override/set from the environment.
                    final String v = System.getProperty(s);
                    if(!quiet)
                        System.out.println("Using: " + s + "=" + v);
                    properties.setProperty(s, v);
                }
            }
        }
        
        return properties;
   } 

    private static void usage() {
        
        System.err.println("usage: [-closure][-verbose][-durableQueues][-namespace namespace] propertyFile (fileOrDir)+");

        System.exit(1);
        
    }
    
    public static FilenameFilter getFilenameFilter() {
    	return filter;
    }
    

    /**
     * Note: The filter is chosen to select RDF data files and to allow the data
     * files to use owl, ntriples, etc as their file extension.  gzip and zip
     * extensions are also supported.
     */
    final private static FilenameFilter filter = new FilenameFilter() {

    	@Override
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
