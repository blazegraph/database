package com.bigdata.rdf.internal;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.bigdata.Banner;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder.DefaultFrontCodedRabaCoder;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;

/**
 * Utility class to parse some RDF resource(s) and count hash collisions using a
 * variety of hash codes.
 * 
 * @author thompsonbry
 */
public class HashCollisionUtility {

	private final static Logger log = Logger
			.getLogger(HashCollisionUtility.class);

	private final BTree[] indices;
	
////	private interface IHashCode {
////		void hashCode(IKeyBuilder keyBuilder,Object o);
////	}
//	
//	private static class Int32HashCode { //implements IHashCode {
//
//		public void hashCode(IKeyBuilder keyBuilder, Object o) {
//			
//			keyBuilder.append(o.hashCode());
//			
//		}
//		
//	}
//	
//	private static class MessageDigestHashCode { //implements IHashCode {
//
//		final MessageDigest d;
//		
//		MessageDigestHashCode() throws NoSuchAlgorithmException {
//
//			d = MessageDigest.getInstance("SHA-256"); // 256 bits (32 bytes)
//			
//		}
//		
//		public void hashCode(IKeyBuilder keyBuilder, final byte[] b) {
//		
//			d.reset();
//			d.digest(b);
//			keyBuilder.append(d.digest());
//			
//		}
//		
//	}
	
	private HashCollisionUtility(final Journal jnl) {
		
		final String name = "lex";
		
		BTree ndx = jnl.getIndex(name);
		
		if(ndx == null) {
			
			final IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());
			
			final DefaultTupleSerializer tupleSer = new DefaultTupleSerializer(
					new DefaultKeyBuilderFactory(new Properties()),//
					DefaultFrontCodedRabaCoder.INSTANCE,//
					new SimpleRabaCoder()//
			);
			
			md.setTupleSerializer(tupleSer);
			
			ndx = jnl.registerIndex(name, md);
			
		}
		
		indices = new BTree[] {ndx};
				
	}

	private void parseFileOrDirectory(final File fileOrDir)
			throws RDFParseException, RDFHandlerException, IOException, NoSuchAlgorithmException {

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

		parseFile(f);

	}
	
	private void parseFile(final File file) throws IOException,
			RDFParseException, RDFHandlerException, NoSuchAlgorithmException {

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

		rdfParser.setValueFactory(new ValueFactoryImpl());

		rdfParser.setVerifyData(false);

		rdfParser.setStopAtFirstError(false);

		rdfParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

		rdfParser.setRDFHandler(new StatementHandler());

		/*
		 * Run the parser, which will cause statements to be inserted.
		 */
		
		if (log.isInfoEnabled())
			log.info("Parsing: " + file);

		InputStream is = new FileInputStream(file);

		try {

			is = new BufferedInputStream(is);

			final boolean gzip = file.getName().endsWith(".gz");
			
			if (gzip)
				is = new GZIPInputStream(is);

			final String baseURI = file.toURI().toString();

			rdfParser.parse(is, baseURI);

		} finally {

			is.close();

		}

	}

    /**
     * Helper class adds statements to the sail as they are visited by a parser.
     */
    private class StatementHandler extends RDFHandlerBase {

    	/**
    	 * #of statements visited.
    	 */
    	private final AtomicLong nstmts = new AtomicLong();

		/**
		 * #of collisions - the array indices are correlated with the array of
		 * BTree indices.
		 */
    	private final AtomicLong ncollision[];
        
    	/**
    	 * Used to build the keys.
    	 */
    	private final IKeyBuilder keyBuilder = KeyBuilder.newInstance();
    	
		private final MessageDigest d;

		public StatementHandler() throws NoSuchAlgorithmException {
        	
			d = MessageDigest.getInstance("SHA-256"); // 256 bits (32 bytes)

			ncollision = new AtomicLong[indices.length];

			for (int i = 0; i < indices.length; i++) {

				ncollision[i] = new AtomicLong();

			}

		}

		public void handleStatement(Statement stmt) throws RDFHandlerException {

			for (int i = 0; i < indices.length; i++) {

				final AtomicLong c = ncollision[i];

				final BTree ndx = indices[i];

				addValue(ndx, c, stmt.getSubject());

				addValue(ndx, c, stmt.getPredicate());

				addValue(ndx, c, stmt.getObject());

				if (stmt.getContext() != null) {

					addValue(ndx, c, stmt.getContext());

				}

			}

			nstmts.incrementAndGet();

		}

		private byte[] buildKey(Value r, byte[] val) {

			if (false) {
				
				final int hashCode = r.hashCode();

				return keyBuilder.reset().append(hashCode).getKey();
				
			} else {
				
				// Note: d.digest(byte[]) already calls reset() so this is redundant.
//				d.reset();
				
				final byte[] hashCode = d.digest(val);

				/*
				 * @todo now try with only N bytes worth of the SHA hash code,
				 * leaving some bits left over for partitioning URIs, Literals,
				 * and BNodes (for told bnode mode) and for a counter to break
				 * ties when there is a hash collision. We should wind up with
				 * an 8-12 byte termId which is collision proof and very well
				 * distributed.
				 * 
				 * @todo benchmark the load time with different hash codes. the
				 * cost of the hash computation and the randomness of the
				 * distribution will both play a role. The B+Tree will need to
				 * be setup with a sufficient [writeRetentionQueue] and we will
				 * need to specify [-server -Xmx1G].
				 */
				
				return keyBuilder.reset().append(hashCode).getKey();
				
			}

		}

		private void addValue(final BTree ndx, final AtomicLong c, final Value r) {

			final byte[] val = SerializerUtil.serialize(r);

			final byte[] key = buildKey(r, val);

			byte[] val2 = ndx.lookup(key);

			if (val2 != null) {

				if (BytesUtil.bytesEqual(val, val2)) {

					// Already in the index.
					return;

				}

				// Hash collision.
				
				c.incrementAndGet();

				log.warn("Collision: hashCode=" + BytesUtil.toString(key)
						+ ", ncoll=" + c + ", resource=" + r + " with "
						+ SerializerUtil.deserialize(val2));

				return;
				
			}
			
			// Insert into the index.
			ndx.insert(key, val);

		}
		
	}

	/**
	 * Parse files, inserting {@link Value}s into indices and counting hash
	 * collisions.
	 * 
	 * @param args
	 *            filename(s)
	 * 
	 * @throws IOException
	 * @throws RDFHandlerException
	 * @throws RDFParseException
	 * @throws NoSuchAlgorithmException 
	 */
	public static void main(final String[] args) throws RDFParseException,
			RDFHandlerException, IOException, NoSuchAlgorithmException {

		Banner.banner();
		
		// check args.
		{

			for (String filename : args) {

				final File file = new File(filename);

				if (!file.exists())
					throw new RuntimeException("Not found: " + file);

			}

		}
		
		final Properties properties = new Properties();

		properties.setProperty(Journal.Options.BUFFER_MODE, BufferMode.DiskRW
				.toString());

		// The caller MUST specify the filename using -D on the command line.
		properties.setProperty(Journal.Options.FILE, System
				.getProperty(Journal.Options.FILE));
		
		final Journal jnl = new Journal(properties);

		try {

			HashCollisionUtility u = new HashCollisionUtility(jnl);

			for (String filename : args) {

				u.parseFileOrDirectory(new File(filename));
				
			}

			jnl.commit();
			
		} finally {
			
			jnl.close();
			
		}

	}

}
