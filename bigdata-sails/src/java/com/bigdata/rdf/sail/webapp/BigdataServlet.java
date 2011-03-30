package com.bigdata.rdf.sail.webapp;

import info.aduna.xml.XMLWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IAtomicStore;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RWStrategy;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sail.webapp.BigdataContext.Config;
import com.bigdata.rdf.sail.webapp.BigdataContext.RunningQuery;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.RelationSchema;
import com.bigdata.rwstore.RWStore;
import com.bigdata.sparse.ITPS;
import com.bigdata.util.concurrent.ThreadPoolExecutorBaseStatisticsTask;
import com.bigdata.util.httpd.NanoHTTPD;

public class BigdataServlet extends HttpServlet {
	
	/**
	 * The logger for the concrete {@link NanoSparqlServer} class.  The {@link NanoHTTPD}
	 * class has its own logger.
	 */
	static private final Logger log = Logger.getLogger(BigdataServlet.class); 

	/**
	 * The character set used for the response (not negotiated).
	 */
    static protected final String charset = "UTF-8";

	/**
	 * Some HTTP response status codes
	 */
	public static final int
		HTTP_OK = HttpServletResponse.SC_ACCEPTED,
		HTTP_REDIRECT = HttpServletResponse.SC_TEMPORARY_REDIRECT,
		HTTP_FORBIDDEN = HttpServletResponse.SC_FORBIDDEN,
		HTTP_NOTFOUND = HttpServletResponse.SC_NOT_FOUND,
        HTTP_BADREQUEST = HttpServletResponse.SC_BAD_REQUEST,
        HTTP_METHOD_NOT_ALLOWED = HttpServletResponse.SC_METHOD_NOT_ALLOWED,
		HTTP_INTERNALERROR = HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
		HTTP_NOTIMPLEMENTED = HttpServletResponse.SC_NOT_IMPLEMENTED;

	/**
	 * Common mime types for dynamic content
	 */
	public static final String
		MIME_TEXT_PLAIN = "text/plain",
		MIME_TEXT_HTML = "text/html",
		MIME_TEXT_XML = "text/xml",
		MIME_DEFAULT_BINARY = "application/octet-stream",
        MIME_APPLICATION_XML = "application/xml",
        MIME_TEXT_JAVASCRIPT = "text/javascript",
        /**
         * The traditional encoding of URL query parameters within a POST
         * message body.
         */
        MIME_APPLICATION_URL_ENCODED = "application/x-www-form-urlencoded";

	/**
	 * A SPARQL results set in XML.
	 */
	static public final String MIME_SPARQL_RESULTS_XML = "application/sparql-results+xml";

	/**
	 * RDF/XML.
	 */
	static public final String MIME_RDF_XML = "application/rdf+xml";
	
	/**
	 * Helper class to figure out the type of a query.
	 */
	public static enum QueryType {
		
		ASK(0),
		DESCRIBE(1),
		CONSTRUCT(2),
		SELECT(3);
		
		private final int order;
		
		private QueryType(final int order) {
			this.order = order;
		}

		private static QueryType getQueryType(final int order) {
			switch (order) {
			case 0:
				return ASK;
			case 1:
				return DESCRIBE;
			case 2:
				return CONSTRUCT;
			case 3:
				return SELECT;
			default:
				throw new IllegalArgumentException("order=" + order);
			}
		}

		/**
		 * Used to note the offset at which a keyword was found. 
		 */
		static private class P implements Comparable<P> {

			final int offset;
			final QueryType queryType;

			public P(final int offset, final QueryType queryType) {
				this.offset = offset;
				this.queryType = queryType;
			}
			/** Sort into descending offset. */
			public int compareTo(final P o) {
				return o.offset - offset;
			}
		}

		/**
		 * Hack returns the query type based on the first occurrence of the
		 * keyword for any known query type in the query.
		 * 
		 * @param queryStr
		 *            The query.
		 * 
		 * @return The query type.
		 */
		static public QueryType fromQuery(final String queryStr) {

			// force all to lower case.
			final String s = queryStr.toUpperCase();

			final int ntypes = QueryType.values().length;

			final P[] p = new P[ntypes];

			int nmatch = 0;
			for (int i = 0; i < ntypes; i++) {

				final QueryType queryType = getQueryType(i);
				
				final int offset = s.indexOf(queryType.toString());

				if (offset == -1)
					continue;

				p[nmatch++] = new P(offset, queryType); 
				
			}

			if (nmatch == 0) {

				throw new RuntimeException(
						"Could not determine the query type: " + queryStr);

			}

			Arrays.sort(p, 0/* fromIndex */, nmatch/* toIndex */);

			final P tmp = p[0];

//			System.out.println("QueryType: offset=" + tmp.offset + ", type="
//					+ tmp.queryType);

			return tmp.queryType;

		}
		
	}
	

	
	IIndexManager getIndexManager() {
		return getContext().getIndexManager();
	}
	
	protected BigdataSailRepositoryConnection getQueryConnection(
            final String namespace, final long timestamp)
            throws RepositoryException {
		return getContext().getQueryConnection(namespace, timestamp);
	}
	
	protected AtomicLong getQueryIdFactory() {
		return getContext().getQueryIdFactory();
	}
	
    protected Map<Long, RunningQuery> getQueries() {
		return getContext().getQueries();
    }
	/**
	 * Return a list of the registered {@link AbstractTripleStore}s.
	 */
	protected List<String> getNamespaces() {
	
		// the triple store namespaces.
		final List<String> namespaces = new LinkedList<String>();

		// scan the relation schema in the global row store.
		final Iterator<ITPS> itr = (Iterator<ITPS>) getIndexManager()
				.getGlobalRowStore().rangeIterator(RelationSchema.INSTANCE);

		while (itr.hasNext()) {

			// A timestamped property value set is a logical row with
			// timestamped property values.
			final ITPS tps = itr.next();

			// If you want to see what is in the TPS, uncomment this.
//			System.err.println(tps.toString());
			
			// The namespace is the primary key of the logical row for the
			// relation schema.
			final String namespace = (String) tps.getPrimaryKey();

			// Get the name of the implementation class
			// (AbstractTripleStore, SPORelation, LexiconRelation, etc.)
			final String className = (String) tps.get(RelationSchema.CLASS)
					.getValue();

			try {
				final Class<?> cls = Class.forName(className);
				if (AbstractTripleStore.class.isAssignableFrom(cls)) {
					// this is a triple store (vs something else).
					namespaces.add(namespace);
				}
			} catch (ClassNotFoundException e) {
				log.error(e,e);
			}

		}

		return namespaces;

	}
	
    /**
     * Return the namespace which will be used to execute the query. The
     * namespace is represented by the first component of the URI. If there is
     * no namespace, then return the configured default namespace.
     * 
     * @param uri
     *            The URI path string.
     * 
     * @return The namespace.
     */
    protected String getNamespace(final String uri) {

//        // locate the "//" after the protocol.
//        final int index = uri.indexOf("//");
    	
    	int snmsp = uri.indexOf("/namespace/");

        if (snmsp == -1) {
            // use the default namespace.
            return getConfig().namespace;
        }

        // locate the next "/" in the URI path.
        final int beginIndex = uri.indexOf('/', snmsp + 1/* fromIndex */);

        // locate the next "/" in the URI path.
        int endIndex = uri.indexOf('/', beginIndex + 1/* fromIndex */);

        if (endIndex == -1) {
            // use the rest of the URI.
            endIndex = uri.length();
        }

        // return the namespace.
        return uri.substring(beginIndex + 1, endIndex);

    }
    
    Config getConfig() {
    	return getContext().getConfig();
    }

    BigdataContext getContext() {
    	return BigdataContext.getContext();
    }

	/**
	 * Return the timestamp which will be used to execute the query. The uri
	 * query parameter <code>timestamp</code> may be used to communicate the
	 * desired commit time against which the query will be issued. If that uri
	 * query parameter is not given then the default configured commit time will
	 * be used. Applications may create protocols for sharing interesting commit
	 * times as reported by {@link IAtomicStore#commit()} or by a distributed
	 * data loader (for scale-out).
	 * 
	 * @todo the configured timestamp should only be used for the default
	 *       namespace (or it should be configured for each graph explicitly, or
	 *       we should bundle the (namespace,timestamp) together as a single
	 *       object).
	 */
	protected long getTimestamp(final String uri,
			final HttpServletRequest req) {
		final String timestamp = req.getParameter("timestamp");
		
		if (timestamp == null) {
			return getConfig().timestamp;
		}

		return Long.valueOf(timestamp);

    }
	
	void buildResponse(HttpServletResponse resp, int status, String mimeType) throws IOException {
        resp.setStatus(status);
        resp.setContentType(mimeType);
	}
	
	void buildResponse(HttpServletResponse resp, int status, String mimeType, String content) throws IOException {
        buildResponse(resp, status, mimeType);
        
		resp.getWriter().print(content);
	}
	
	void buildResponse(HttpServletResponse resp, int status, String mimeType, InputStream content) throws IOException {
        buildResponse(resp, status, mimeType);
        
		final OutputStream outstr = resp.getOutputStream();
		final byte[] buf = new byte[1024];
		while (true) {
			int rdlen = content.read(buf);
			if (rdlen <= 0) {
				break;
			}
			outstr.write(buf, 0, rdlen);
		}
	}

    /**
     * Write the stack trace onto the output stream. This will show up in the
     * client's response. This code path should be used iff we have already
     * begun writing the response. Otherwise, an HTTP error status should be
     * used instead.
     * 
     * @param t
     *            The thrown error.
     * @param os
     *            The stream on which the response will be written.
     * @param queryStr
     *            The query string (if available).
     * 
     * @return The laundered exception.
     * 
     * @throws Exception
     */
    public static RuntimeException launderThrowable(final Throwable t,
            final OutputStream os, final String queryStr) throws Exception {
        try {
            // log an error for the service.
            log.error(t, t);
        } finally {
            // ignore any problems here.
        }
		if (os != null) {
			try {
                final PrintWriter w = new PrintWriter(os);
                if (queryStr != null) {
                    /*
                     * Write the query onto the output stream.
                     */
                    w.write(queryStr);
                    w.write("\n");
                }
                /*
                 * Write the stack trace onto the output stream.
                 */
                t.printStackTrace(w);
                w.flush();
				// flush the output stream.
				os.flush();
			} finally {
				// ignore any problems here.
			}
			try {
				// ensure output stream is closed.
				os.close();
			} catch (Throwable t2) {
				// ignore any problems here.
			}
		}
		if (t instanceof RuntimeException) {
			return (RuntimeException) t;
		} else if (t instanceof Error) {
			throw (Error) t;
		} else if (t instanceof Exception) {
			throw (Exception) t;
		} else
			throw new RuntimeException(t);
	}

    /**
     * Return various interesting metadata about the KB state.
     * 
     * @todo The range counts can take some time if the cluster is heavily
     *       loaded since they must query each shard for the primary statement
     *       index and the TERM2ID index.
     */
    protected StringBuilder getKBInfo(final String namespace,
            final long timestamp) {

		final StringBuilder sb = new StringBuilder();

        BigdataSailRepositoryConnection conn = null;

		try {

            conn = getQueryConnection(namespace, timestamp);
		    
			final AbstractTripleStore tripleStore = conn.getTripleStore();

			sb.append("class\t = " + tripleStore.getClass().getName() + "\n");

			sb
					.append("indexManager\t = "
							+ tripleStore.getIndexManager().getClass()
									.getName() + "\n");

			sb.append("namespace\t = " + tripleStore.getNamespace() + "\n");

			sb.append("timestamp\t = "
					+ TimestampUtility.toString(tripleStore.getTimestamp())
					+ "\n");

			sb.append("statementCount\t = " + tripleStore.getStatementCount()
					+ "\n");

			sb.append("termCount\t = " + tripleStore.getTermCount() + "\n");

			sb.append("uriCount\t = " + tripleStore.getURICount() + "\n");

			sb.append("literalCount\t = " + tripleStore.getLiteralCount() + "\n");

            /*
             * Note: The blank node count is only available when using the told
             * bnodes mode.
             */
            sb
                    .append("bnodeCount\t = "
                            + (tripleStore.getLexiconRelation()
                                    .isStoreBlankNodes() ? ""
                                    + tripleStore.getBNodeCount() : "N/A")
                            + "\n");

			sb.append(IndexMetadata.Options.BTREE_BRANCHING_FACTOR
					+ "="
					+ tripleStore.getSPORelation().getPrimaryIndex()
							.getIndexMetadata().getBranchingFactor() + "\n");

			sb.append(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY
					+ "="
					+ tripleStore.getSPORelation().getPrimaryIndex()
							.getIndexMetadata()
							.getWriteRetentionQueueCapacity() + "\n");

            sb.append(BigdataSail.Options.STAR_JOINS + "="
                    + conn.getRepository().getSail().isStarJoins() + "\n");

			sb.append("-- All properties.--\n");
			
			// get the triple store's properties from the global row store.
			final Map<String, Object> properties = getIndexManager()
					.getGlobalRowStore().read(RelationSchema.INSTANCE,
							namespace);

			// write them out,
			for (String key : properties.keySet()) {
				sb.append(key + "=" + properties.get(key)+"\n");
			}

			/*
			 * And show some properties which can be inherited from
			 * AbstractResource. These have been mainly phased out in favor of
			 * BOP annotations, but there are a few places where they are still
			 * in use.
			 */
            
			sb.append("-- Interesting AbstractResource effective properties --\n");
            
			sb.append(AbstractResource.Options.CHUNK_CAPACITY + "="
					+ tripleStore.getChunkCapacity() + "\n");

			sb.append(AbstractResource.Options.CHUNK_OF_CHUNKS_CAPACITY + "="
					+ tripleStore.getChunkOfChunksCapacity() + "\n");

			sb.append(AbstractResource.Options.CHUNK_TIMEOUT + "="
					+ tripleStore.getChunkTimeout() + "\n");

			sb.append(AbstractResource.Options.FULLY_BUFFERED_READ_THRESHOLD + "="
					+ tripleStore.getFullyBufferedReadThreshold() + "\n");

			sb.append(AbstractResource.Options.MAX_PARALLEL_SUBQUERIES + "="
					+ tripleStore.getMaxParallelSubqueries() + "\n");

			/*
			 * And show some interesting effective properties for the KB, SPO
			 * relation, and lexicon relation.
			 */
			sb.append("-- Interesting KB effective properties --\n");
			
			sb
					.append(AbstractTripleStore.Options.TERM_CACHE_CAPACITY
							+ "="
							+ tripleStore
									.getLexiconRelation()
									.getProperties()
									.getProperty(
											AbstractTripleStore.Options.TERM_CACHE_CAPACITY,
											AbstractTripleStore.Options.DEFAULT_TERM_CACHE_CAPACITY) + "\n");

			/*
			 * And show several interesting properties with their effective
			 * defaults.
			 */

			sb.append("-- Interesting Effective BOP Annotations --\n");

			sb.append(BufferAnnotations.CHUNK_CAPACITY
					+ "="
					+ tripleStore.getProperties().getProperty(
							BufferAnnotations.CHUNK_CAPACITY,
							"" + BufferAnnotations.DEFAULT_CHUNK_CAPACITY)
					+ "\n");

			sb
					.append(BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY
							+ "="
							+ tripleStore
									.getProperties()
									.getProperty(
											BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY,
											""
													+ BufferAnnotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY)
							+ "\n");

			sb.append(BufferAnnotations.CHUNK_TIMEOUT
					+ "="
					+ tripleStore.getProperties().getProperty(
							BufferAnnotations.CHUNK_TIMEOUT,
							"" + BufferAnnotations.DEFAULT_CHUNK_TIMEOUT)
					+ "\n");

			sb.append(PipelineJoin.Annotations.MAX_PARALLEL_CHUNKS
					+ "="
					+ tripleStore.getProperties().getProperty(
							PipelineJoin.Annotations.MAX_PARALLEL_CHUNKS,
							"" + PipelineJoin.Annotations.DEFAULT_MAX_PARALLEL_CHUNKS) + "\n");

			sb
					.append(IPredicate.Annotations.FULLY_BUFFERED_READ_THRESHOLD
							+ "="
							+ tripleStore
									.getProperties()
									.getProperty(
											IPredicate.Annotations.FULLY_BUFFERED_READ_THRESHOLD,
											""
													+ IPredicate.Annotations.DEFAULT_FULLY_BUFFERED_READ_THRESHOLD)
							+ "\n");

			// sb.append(tripleStore.predicateUsage());

            if (tripleStore.getIndexManager() instanceof Journal) {

                final Journal journal = (Journal) tripleStore.getIndexManager();
                
                final IBufferStrategy strategy = journal.getBufferStrategy();
                
                if (strategy instanceof RWStrategy) {
                
                    final RWStore store = ((RWStrategy) strategy).getRWStore();
                    
                    store.showAllocators(sb);
                    
                }
                
            }

		} catch (Throwable t) {

			log.warn(t.getMessage(), t);

		} finally {
		    
		    if(conn != null) {
                try {
                    conn.close();
                } catch (RepositoryException e) {
                    log.error(e, e);
                }
		        
		    }
		    
		}

		return sb;

    }

    protected ThreadPoolExecutorBaseStatisticsTask getSampleTask() {
    	return getContext().getSampleTask();
    }

	<T> TreeMap<Long, T> getQueryMap() {
		return new TreeMap<Long, T>(new Comparator<Long>() {
			/**
			 * Comparator puts the entries into descending order by the query
			 * execution time (longest running queries are first).
			 */
			public int compare(final Long o1, final Long o2) {
				if(o1.longValue()<o2.longValue()) return 1;
				if(o1.longValue()>o2.longValue()) return -1;
				return 0;
			}
		});
	}


}
