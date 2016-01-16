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
 * Created on Mar 17, 2012
 */

package com.bigdata.bop.rdf.update;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.ILocatableResourceAnnotations;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.rio.IRDFParserOptions;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RDFParserOptions;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sail.webapp.client.MiniMime;
import com.bigdata.rdf.sparql.ast.LoadGraph;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.rdf.store.DataLoader.CommitEnum;
import com.bigdata.rdf.store.DataLoader.Options;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.util.Bytes;

/**
 * Operator parses a RDF data source, writing bindings which represent
 * statements onto the output sink. This operator is compatible with the
 * {@link ChunkedResolutionOp} and the {@link InsertStatementsOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Examine the integration point for Truth Maintenance (TM).
 *          <p>
 *          {@link ClosureEnum} and {@link CommitEnum} shape the way in which
 *          the update plan is generated. They are not options on the
 *          {@link ParseOp} itself.
 *          <p>
 *          We need to setup the assertion and retraction buffers such that they
 *          have the appropriate scope or (for database at once closure) we do
 *          not setup those buffers but we recompute the closure of the database
 *          afterwards.
 *          <p>
 *          The assertion buffers might be populated after the IV resolution
 *          step and before we write on the indices. We then compute the fixed
 *          point of the closure over the delta and then write that onto the
 *          database. We should be able to specify that some sources contain
 *          data to be removed (INSERT DATA and REMOVE DATA or UNLOAD src). The
 *          operation should combine assertions and retractions to be efficient.
 *          <p>
 *          See {@link DataLoader}.
 * 
 *          TODO Add an operator which handles a zip archive, creating a LOAD
 *          for each resource in that archive. Recursive directory processing is
 *          similar. Both should result in multiple ParseOp instances which can
 *          run in parallel. Those ParseOp instances will feed the IV
 *          resolution, optional TM, and statement writer operations.
 *          <p>
 *          If we can make the SOURCE_URI a value expression, then we could flow
 *          solutions into the LOAD operation which would be the bindings for
 *          the source URI. Very nice! Then we could hash partition the LOAD
 *          operator across a cluster and do a parallel load very easily. If the
 *          source for those solutions was the parse of a single RDF file (or
 *          streamed URI) containing the files to be loaded then we could also
 *          gain the indirection necessary to load large numbers of files in
 *          parallel on a cluster.
 * 
 *          TODO In at least the SIDS mode, we need to do some special
 *          operations when the statement buffer is flushed. That statement
 *          buffer could either be fed directly by the {@link ParserOp} or
 *          indirectly through solutions modeling statements flowing through the
 *          query engine. I am inclined to the latter for better parallelism.
 *          Even though there is more stuff on the heap and more latency within
 *          the stages, I think that we will get more out of the increased
 *          parallelism.
 * 
 *          TODO Any annotation here should be configurable from the
 *          {@link LoadGraph} AST node and (ideally) the SPARQL UPDATE syntax.
 * 
 *          FIXME This does not handle SIDS. The {@link StatementBuffer} logic
 *          needs to get into {@link InsertStatementsOp} for that to work, or
 *          the plan needs to be slightly different and hit a different insert
 *          operator for statements all together.
 * 
 *          FIXME This does not handle Truth Maintenance.
 * 
 * @see PresortRioLoader
 * @see StatementBuffer
 * @see DataLoader
 * @see DataLoader.Options
 * @see RDFParserOptions
 * @see ClosureEnum
 * @see CommitEnum
 */
public class ParseOp extends PipelineOp {

    private static final transient Logger log = Logger.getLogger(ParseOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Note: {@link BOp.Annotations#TIMEOUT} is respected to limit the read time
     * on an HTTP connection.
     * <p>
     * Note: The {@link RDFParserOptions} are initialized based on
     * {@link AbstractTripleStore#getProperties()}. Those defaults are then
     * overriden by any {@link RDFParserOptions.Options} which are specified as
     * annotations. When {@link LexiconRelation#isStoreBlankNodes()} is
     * <code>true</code>, then blank nodes IDs will be preserved unless that
     * option has been explicitly overridden.
     */
    public interface Annotations extends PipelineOp.Annotations,
            ILocatableResourceAnnotations, RDFParserOptions.Options {

        /**
         * The data source to be parsed.
         */
        String SOURCE_URI = ParseOp.class.getName() + ".sourceUri";

        /**
         * The base URI for that data source (defaults to the
         * {@link #SOURCE_URI}).
         */
        String BASE_URI = ParseOp.class.getName() + ".baseUri";

        /**
         * The target graph (optional).
         * <p>
         * Note: This is ignored unless we are in quads mode. If we are in quads
         * mode and the data is not quads, then it is an error.
         */
        String TARGET_URI = ParseOp.class.getName() + ".targetUri";

        /**
         * When <code>true</code>, a parse error will be ignored.
         */
        String SILENT = ParseOp.class.getName() + ".silent";
        
        boolean DEFAULT_SILENT = false;
        
//        /**
//         * Optional property specifying the capacity of the
//         * {@link StatementBuffer} (default is {@value #DEFAULT_BUFFER_CAPACITY}
//         * statements).
//         */
//        String BUFFER_CAPACITY = ParseOp.class.getName() + ".bufferCapacity";
//
//        int DEFAULT_BUFFER_CAPACITY = 100000;

        /**
         * The name of the fallback {@link RDFFormat} (default
         * {@link #DEFAULT_FALLBACK}).
         * <p>
         * Note: {@link RDFFormat} is not {@link Serializable}, so this
         * annotation must be the format name rather than the {@link RDFFormat}
         * instance.
         */
        String FALLBACK = ParseOp.class.getName() + ".fallback";

        String DEFAULT_FALLBACK = RDFFormat.RDFXML.getName();
        
        /**
         * When <code>true</code>, the context position will be stripped from
         * the statements. This may be used to remove context from quads data
         * when loading into a triples or SIDs mode database. It may also be
         * used to force quads data into a specific context in quads mode.
         */
        String STRIP_CONTEXT = ParseOp.class.getName() + ".stripContext";
        
        boolean DEFAULT_STRIP_CONTEXT = false;
        
        /**
         * <code>true</code> iff HTTP requests may use cached responses.
         */
        String USES_CACHE = ParseOp.class.getName() + ".usesCache";
        
        boolean DEFAULT_USES_CACHE = true;

        /**
         * The {@link BufferedReader}'s internal buffer size (default is
         * {@value #DEFAULT_READ_BUFFER_SIZE}).
         */
        String READ_BUFFER_SIZE = ParseOp.class.getName() + ".readBufferSize";

        /**
         * Note: 8k is the default buffer size for {@link BufferedReader} so
         * this default is the same as not specifying an override.
         */
        int DEFAULT_READ_BUFFER_SIZE = Bytes.kilobyte32 * 8;

    }
    
    public ParseOp(final BOp[] args, final Map<String, Object> annotations) {

        super(args, annotations);
        
        getRequiredProperty(Annotations.SOURCE_URI);
        
        getRequiredProperty(Annotations.RELATION_NAME);
        
        getRequiredProperty(Annotations.TIMESTAMP);
        
    }

    public ParseOp(final ParseOp op) {
        
        super(op);
        
    }

    /**
     * The s, p, o, and c variable names.
     */
    static protected final Var<?> s = Var.var("s"), p = Var.var("p"), o = Var
            .var("o"), c = Var.var("c");

    @Override
    public ParserStats newStats() {

        return new ParserStats();

    }

    @Override
    public FutureTask<Void> eval(BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(context, this));

    }

    static private class ChunkTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;
        private final ParserStats stats;
        
        private final URI sourceUri;
        private final String uriStr;
        private final String baseUri;
        private final URI targetUri;
        
        /**
         * When true, attempt to resolve the resource against CLASSPATH.
         * 
         * TODO Should we permit this?
         */
        private final boolean allowClassPath = true;

        /**
         * The default {@link RDFFormat}.
         */
        private final RDFFormat fallback;

        private final boolean sids, quads, silent;

        private final boolean stripContext;
        
        /** The connection timeout (ms) -or- ZERO (0) for an infinite timeout. */
        private final int timeout;

        /**
         * When <code>true</code> HTTP caching of the request is allowed.
         */
        private final boolean usesCache;

        /**
         * The {@link BufferedReader}'s internal buffer size (default is 8k).
         */
        private final int readBufferSize;

        /**
         * Output chunk size.
         */
        private final int chunkCapacity;
        
        /**
         * TODO Javadoc for annotations (which need to be defined) and
         * interaction with the triple store properties.
         */
        private final IRDFParserOptions parserOptions;

        /**
         * The {@link AbstractTripleStore} on which the statements will
         * eventually be written.
         */
        private final AbstractTripleStore database;
        
        /*
         * FIXME Both the {@link #buffer} and the {@link #tm} objects need to be
         * part of shared state in order to support truth maintenance. That
         * state needs to be shared across the operations used to add/resolve
         * IVs, compute the closure over the statements to be added and/or
         * removed, and then insert/remove the statements and entailments,
         * updating the proof chains.
         * 
         * This does not belong on the ParserOp, but as something which wraps
         * ANY sequence of UPDATE operations (INSERT DATA, REMOVE DATA, LOAD
         * DATA, DELETE/INSERT, CLEAR, COPY, ADD, etc). TM is only available for
         * triples and SIDS on a Journal.
         */
        //
//        /** StatementBuffer capacity. */
//        private final int bufferCapacity;
//        
//        /**
//         * The object used to compute entailments for the database.
//         */
//        private final InferenceEngine inferenceEngine;
//        
//        /**
//         * The object used to compute entailments for the database.
//         */
//        public InferenceEngine getInferenceEngine() {
//
//            return inferenceEngine;
//
//        }
//
//        /**
//         * Used to buffer writes.
//         * 
//         * @see #getAssertionBuffer()
//         */
//        private StatementBuffer<?> buffer;
//
//        /**
//         * The object used to maintain the closure for the database iff
//         * incremental truth maintenance is enabled.
//         */
//        private final TruthMaintenance tm;
//        
//        /**
//         * Return the assertion buffer.
//         * <p>
//         * The assertion buffer is used to buffer statements that are being
//         * asserted so as to maximize the opportunity for batch writes. Truth
//         * maintenance (if enabled) will be performed no later than the commit
//         * of the transaction.
//         * <p>
//         * Note: The same {@link #buffer} is reused by each loader so that we
//         * can on the one hand minimize heap churn and on the other hand disable
//         * auto-flush when loading a series of small documents. However, we
//         * obtain a new buffer each time we perform incremental truth
//         * maintenance.
//         * <p>
//         * Note: When non-<code>null</code> and non-empty, the buffer MUST be
//         * flushed (a) if a transaction completes (otherwise writes will not be
//         * stored on the database); or (b) if there is a read against the
//         * database during a transaction (otherwise reads will not see the
//         * unflushed statements).
//         * <p>
//         * Note: if {@link #truthMaintenance} is enabled then this buffer is
//         * backed by a temporary store which accumulates the {@link SPO}s to be
//         * asserted. Otherwise it will write directly on the database each time
//         * it is flushed, including when it overflows.
//         * 
//         * @todo this should be refactored as an {@link IStatementBufferFactory}
//         *       where the appropriate factory is required for TM vs non-TM
//         *       scenarios (or where the factory is parameterize for tm vs
//         *       non-TM).
//         */
//        private StatementBuffer<?> getAssertionBuffer() {
//
//            if (buffer == null) {
//
//                if (tm != null) {
//
//                    buffer = new StatementBuffer(tm.newTempTripleStore(),
//                            database, bufferCapacity);
//
//                } else {
//
//                    buffer = new StatementBuffer(database, bufferCapacity) {
//                        
//                    };
//
//                }
//
//            }
//
//            return buffer;
//
//        }

        public ChunkTask(final BOpContext<IBindingSet> context, final ParseOp op) {

            this.context = context;

            this.stats = (ParserStats) context.getStats();
            
            this.sourceUri = (URI) op
                    .getRequiredProperty(Annotations.SOURCE_URI);

            // String value of that URI.
            this.uriStr = sourceUri.stringValue();
            
            // base URI defaults to the source URI
            this.baseUri = op.getProperty(Annotations.BASE_URI,
                    sourceUri.stringValue());

            final String namespace = ((String[]) op
                    .getRequiredProperty(Annotations.RELATION_NAME))[0];

            final long timestamp = (Long) op
                    .getRequiredProperty(Annotations.TIMESTAMP);

            this.database = (AbstractTripleStore) context.getResource(
                    namespace, timestamp);

            this.sids = database.isStatementIdentifiers();

            this.quads = database.isQuads();

            this.silent = op.getProperty(Annotations.SILENT,
                    Annotations.DEFAULT_SILENT);
            
            /*
             * Note: This is used as the default context position for the
             * statement if the database mode is quads. It needs to be a
             * BigdataURI from the value factory for the target database - the
             * same factory that is used by the parser.
             */
            this.targetUri = (URI) database.asValue((URI) op
                    .getProperty(Annotations.TARGET_URI));

            this.chunkCapacity = op.getChunkCapacity();
            
            /*
             * Note: RDFFormat is not Serializable, so this annotation must be
             * the format name rather than the RDFFormat instance.
             */
            this.fallback = RDFFormat.valueOf(op.getProperty(
                    Annotations.FALLBACK, Annotations.DEFAULT_FALLBACK));

            this.stripContext = op.getProperty(Annotations.STRIP_CONTEXT,
                    Annotations.DEFAULT_STRIP_CONTEXT);

//            this.inferenceEngine = database.getInferenceEngine();
//
//            if (database.getAxioms().isNone()) {
//                
//                tm = null;
//                
//            } else {
//
//                /*
//                 * Truth maintenance: buffer will write on a tempStore.
//                 */
//
//                tm = new TruthMaintenance(inferenceEngine);
//
//            }
//            
//          this.bufferCapacity = op.getProperty(Annotations.BUFFER_CAPACITY,
//          Annotations.DEFAULT_BUFFER_CAPACITY);
            
            // HTTPConnection timeout.
            this.timeout = op.getProperty(Annotations.TIMEOUT, 0/* infinite */);

            // HTTP caching allowed.
            this.usesCache = op.getProperty(Annotations.USES_CACHE,
                    Annotations.DEFAULT_USES_CACHE);
            
            // BufferedReader buffer size.
            this.readBufferSize = op.getProperty(Annotations.READ_BUFFER_SIZE,
                    Annotations.DEFAULT_READ_BUFFER_SIZE);

            /*
             * Setup the parser options.
             */
            {
                
                final Properties properties = database.getProperties();

                /*
                 * Initialize the parser options based on the database
                 * properties.
                 */
                
                this.parserOptions = new RDFParserOptions(properties);

                /*
                 * Now do explicit annotations which override anything we picked
                 * up above.
                 */

                if (op.getProperty(Options.VERIFY_DATA) != null) {
                    
                    parserOptions.setVerifyData((Boolean) op
                            .getProperty(Options.VERIFY_DATA));
                    
                }
                
                if (op.getProperty(Options.STOP_AT_FIRST_ERROR) != null) {
                    
                    parserOptions.setStopAtFirstError((Boolean) op
                            .getProperty(Options.STOP_AT_FIRST_ERROR));
                    
                }

                if (op.getProperty(Options.DATATYPE_HANDLING) != null) {
                
                    parserOptions.setDatatypeHandling((DatatypeHandling) op
                            .getProperty(Options.DATATYPE_HANDLING));
                    
                }

                if (op.getProperty(Options.PRESERVE_BNODE_IDS) != null) {
                
                    parserOptions.setPreserveBNodeIDs((Boolean) op
                            .getProperty(Options.PRESERVE_BNODE_IDS));
                    
                }

                if ((properties.getProperty(Options.PRESERVE_BNODE_IDS) == null)
                        && op.getProperty(Options.PRESERVE_BNODE_IDS) == null
                        && database.getLexiconRelation().isStoreBlankNodes()) {

                    /*
                     * Note: preserveBNodeIDs is overridden based on whether or
                     * not the target is storing the blank node identifiers
                     * (unless the property was explicitly set - this amounts to
                     * a conditional default).
                     */

                    parserOptions.setPreserveBNodeIDs(true);

                }

            }

        }

        /**
         */
        @Override
        public Void call() throws Exception {

            InputStream is = null;
            Reader reader = null;
            RDFFormat fmt = null;
            
            try {

                /*
                 * The expected format based on the file name component of the
                 * URL.
                 */
                fmt = RDFFormat.forFileName(uriStr, fallback);
                
                String contentEncoding = null;
                
                if (allowClassPath) {
                    
                    // try the classpath :
                    is = getClass()
                            .getResourceAsStream(uriStr);

                    if (is == null) {

                        /*
                         * Searching for the resource from the root of the class
                         * returned by getClass() (relative to the class'
                         * package) failed. Next try searching for the desired
                         * resource from the root of the jar; that is, search
                         * the jar file for an exact match of the input string.
                         */
                        
                        is = getClass().getClassLoader().getResourceAsStream(
                                uriStr);

                    }
                    
                }

                if (is == null) {

                    /*
                     * TODO Refactor to use the apache http components and
                     * provide for managed connection pools, authentication,
                     * etc. However, make sure that we preserve the ability to
                     * read from the local file system.
                     */

                    URLConnection conn = null;

                    final URL url = new URL(uriStr);

                    conn = (URLConnection) url.openConnection();
                    
                    final HttpURLConnection conn2 = (conn instanceof HttpURLConnection) ? (HttpURLConnection) conn
                            : null;
                    
                    if (conn2 != null) {

                        /*
                         * Only if using HTTP.
                         */

                        conn2.setRequestMethod("GET");

                        /*
                         * Set the AcceptHeader based on the expected format.
                         */
                        final String acceptHeader;
                        {
                        
                            final StringBuilder sb = new StringBuilder();

                            final List<String> acceptParams = RDFFormat
                                    .getAcceptParams(RDFFormat.values(),
                                            quads/* requireContext */, fmt/* preferredRDFFormat */);

                            for (String acceptParam : acceptParams) {

                                if (sb.length() > 0) {

                                    sb.append(",");

                                }

                                sb.append(acceptParam);

                            }

                            acceptHeader = sb.toString();

                            if (log.isDebugEnabled())
                                log.debug("Accept: " + acceptHeader);
                            
                        }

                        conn2.setRequestProperty("Accept", acceptHeader);
                        
                        conn.setUseCaches(usesCache);
                        
                        conn.setReadTimeout(timeout);
                        
                    }

                    conn.setDoInput(true);

                    // connect.
                    conn.connect();

                    if (conn2 != null) {

                        // Extract the MIME type from the Content-Type header.
                        final String mimeType = new MiniMime(
                                conn.getContentType()).getMimeType();

                        // Figure out the RDFFormat from that.
                        fmt = RDFFormat.forMIMEType(mimeType, fallback);
                        
                        contentEncoding = conn2.getContentEncoding();
                        
                    }

                    is = conn.getInputStream();

                }

                /*
                 * Obtain a buffered reader on the input stream.
                 */

                if (contentEncoding == null) {

                    /*
                     * Assume the default content encoding if we have no better
                     * information.
                     */

                    contentEncoding = fmt.getCharset().name();

                }

                reader = new BufferedReader(new InputStreamReader(is,
                        contentEncoding), readBufferSize);

                parse(reader, baseUri, fmt, targetUri);

            } catch (Exception ex) {

                final String msg = "While loading: " + uriStr
                        + (fmt != null ? ", fmt=" + fmt : "");

                if (silent) {

                    log.warn(msg);
                    
                } else {

                    throw new RuntimeException(msg, ex);
                
                }

            } finally {

                if (reader != null)
                    reader.close();

                if (is != null) {
                    is.close();
                }

            }
            
            // done.
            return null;

        }
        
        /**
         * 
         * @param source
         * @param baseURL
         * @param rdfFormat
         * @param defaultGraph
         * 
         * @throws IOException 
         * @throws RDFHandlerException 
         * @throws RDFParseException 
         */
        private void parse(final Reader source, final String baseURL,
                final RDFFormat fmt, final URI defaultGraph
                ) throws RDFParseException, RDFHandlerException, IOException {

            final RDFParserFactory rdfParserFactory = RDFParserRegistry
                    .getInstance().get(fmt);

            if (rdfParserFactory == null) {

                throw new RuntimeException(
                        "Parser factory not found: source=" + uriStr
                                + ", fmt=" + fmt);

            }

            final RDFParser rdfParser = rdfParserFactory.getParser();
            rdfParser.setValueFactory(database.getValueFactory());
            rdfParser.setVerifyData(parserOptions.getVerifyData());
            rdfParser.setStopAtFirstError(parserOptions.getStopAtFirstError());
            rdfParser.setDatatypeHandling(parserOptions.getDatatypeHandling());
            rdfParser.setPreserveBNodeIDs(parserOptions.getPreserveBNodeIDs());

            /*
             * Note: The vector size should be pretty big for this. 100k might
             * be good (that is the DataLoader default).
             */
            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                    chunkCapacity, context.getSink());

            rdfParser.setRDFHandler(new AddStatementHandler(unsyncBuffer));

            /*
             * Run the parser, which will cause statements to be inserted.
             */

            rdfParser.parse(source, baseUri);

            unsyncBuffer.flush();

        }

        /**
         * Helper class adds statements to the sail as they are visited by a parser.
         */
        private class AddStatementHandler extends RDFHandlerBase {

            private final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer;

            public AddStatementHandler(
                    final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer
                    ) {

                this.unsyncBuffer = unsyncBuffer;
                
            }

            @SuppressWarnings({ "rawtypes", "unchecked" })
            private final IConstant<IV> asConst(final Value v) {

                final IV iv = TermId.mockIV(VTE.valueOf(v));

                iv.setValue((BigdataValue)v);

                return new Constant<IV>(iv);

            }
            
            public void handleStatement(final Statement stmt)
                    throws RDFHandlerException {

                final ListBindingSet bset = new ListBindingSet();
                
                bset.set(s, asConst(stmt.getSubject()));
                
                bset.set(p, asConst(stmt.getPredicate()));
                
                bset.set(o, asConst(stmt.getObject()));

                Resource context = stmt.getContext();
                
                if (stripContext) {
                    
                    // Strip off the context position.
                    context = null;
                }

                if (quads && context == null) {

                    // Use the default context.
                    context = targetUri;
                    
                }

                if (quads && context == null) {

                    throw new RuntimeException(
                            "Quads mode, but data are triples and the target graph was not specified: "
                                    + uriStr);

                }

                if (context != null) {

                    /*
                     * Bind [c] if available regardless of the database mode.
                     * This stops people from loading quads data into a triples
                     * or SIDs database in a way which throws away the context
                     * when they are not expecting that.
                     */

                    bset.set(c, asConst(context));

                }

                unsyncBuffer.add(bset);

                stats.toldTriples.increment();

            }

        }

    } // ChunkTask

}
