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

	private final StatementHandler stmtHandler;
	
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
			
			// enable raw record support.
			md.setRawRecords(true);
			
			// set the maximum length of a byte[] value in a leaf.
			md.setMaxRecLen(256);
			
			ndx = jnl.registerIndex(name, md);
			
		}
		
		indices = new BTree[] { ndx };

		stmtHandler = new StatementHandler();
		
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

		rdfParser.setRDFHandler(stmtHandler);

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

		public StatementHandler() {
        	
			try {
			
				d = MessageDigest.getInstance("SHA-256"); // 256 bits (32 bytes)
				
			} catch (NoSuchAlgorithmException e) {
				
				throw new RuntimeException(e);
				
			}

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

			if (true) {

				/*
				 * Simple 32-bit hash code based on the byte[] representation of
				 * the RDF Value.
				 */
				
				final int hashCode = r.hashCode();

				return keyBuilder.reset().append(hashCode).getKey();
				
			} else {
				
				/*
				 * Message digest of the serialized representation of the RDF
				 * Value.
				 */
				
				// Note: d.digest(byte[]) already calls reset() so this is redundant.
//				d.reset();
				
				final byte[] hashCode = d.digest(val);

				/*
				 * SHA-256 - no collisions on BSBM 200M. 30G file. time?
				 * 
				 * 32-bit hash codes. #collisions=1544132 Elapsed: 16656445ms
				 * Journal size: 23841341440 bytes (23G)
				 * 
				 * @todo Do not bother comparing time until I have moved the
				 * large records out of line (blob references). This should be
				 * automatic, but that interacts with how we code values and
				 * leaves and might be tricky to make automatic for all coders.
				 * 
				 * @todo We should COUNT the collisions for a given tuple and
				 * then report on the distribution of #of collisions per tuple.
				 * Even considering the worst case (max collisions per tuple)
				 * will tell us whether or not a counter could have been used to
				 * differentiate the hash codes. In fact, the easiest way to do
				 * this is to simply introduce an N-bit counter into the key.
				 * Not only does this mirror the target index structure, but the
				 * distribution can then be obtained by a post-factor traversal
				 * of the index in which we only report on tuples where the
				 * counter is non-zero. If the counter exceeds the bits
				 * available then we have a load error for that tuple (we could
				 * always use a BigInteger in the key to avoid this problem).
				 * 
				 * @todo now try with only N bytes worth of the SHA hash code,
				 * leaving some bits left over for partitioning URIs, Literals,
				 * and BNodes (for told bnode mode) and for a counter to break
				 * ties when there is a hash collision. We should wind up with
				 * an 8-12 byte termId which is collision proof and very well
				 * distributed.
				 * 
				 * @todo if we inline small unicode values (<32 bytes) and
				 * reserve the TERM2ID index for large(r) values then we can
				 * approach a situation in which it serves solely for blobs but
				 * with a tradeoff in size (of the statement indices) versus
				 * indirection.
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

			/*
			 * This is the fixed length hash code prefix. When a collision
			 * exists we can either append a counter -or- use more bits from the
			 * prefix. An extensible hash index works by progressively
			 * increasing the #of bits from the hash code which are used to
			 * create a distinction in the index. Records with identical hash
			 * values are stored in an (unordered, and possibly chained) bucket.
			 * We can approximate this by using N-bits of the hash code for the
			 * key and then increasing the #of bits in the key when there is a
			 * hash collision. Unless a hash function is used which has
			 * sufficient bits available to ensure that there are no collisions,
			 * we may be forced eventually to append a counter to impose a
			 * distinction among records which are hash identical but whose
			 * values differ.
			 * 
			 * In the case of a hash collision, we can determine the records
			 * which have already collided using the fast range count between
			 * the hash code key and the fixed length successor of that key. We
			 * can create a guaranteed distinct key by creating a BigInteger
			 * whose values is (#collisions+1) and appending it to the key. This
			 * approach will give us keys whose byte length increases slowly as
			 * the #of collisions grows (though these might not be the minimum
			 * length keys - depending on how we are encoding the BigInteger in
			 * the key.)
			 * 
			 * When we have a hash collision, we first need to scan all of the
			 * collision records and make sure that none of those records has
			 * the same value as the given record. This is done using the fixed
			 * length successor of the hash code key as the exclusive upper
			 * bound of a key range scan. Each record associated with a tuple in
			 * that key range must be compared for equality with the given
			 * record to decide whether or not the given record already exists
			 * in the index.
			 * 
			 * @todo order preserving hash codes could be interesting here. Look
			 * at 32 and 64 bit variants of the math and at generalized order
			 * preserving hash codes. With order preserving hash codes, it makes
			 * sense to insert all Unicode terms into TERM2ID such that we have
			 * 
			 * @todo Regarding blobs, I am going to add the ability to the
			 * B+Tree to automatically write large tuple values as raw
			 * journal/shard records. This will be transparent, so it you
			 * materialize the tuple, you get the byte[] value as well. However,
			 * such transparent promotion will not really let us handle large
			 * blobs (multi-megabytes) in s/o as a 50 4M blobs would fill up a
			 * shard. There, I think that we need to give the control over to
			 * the application and require it to write on a shared resource
			 * (shared file system, S3, etc). The value inserted into the index
			 * would then be just the pathname in the shared file system or the
			 * URL of the S3 resource.
			 * 
			 * This breaks the ACID decision boundary though as the application
			 * has no means available to atomically decide that the resource
			 * does not exist and hence create it. Even using a conditional
			 * E-Tag on S3 would not work since it would have to have an index
			 * over the S3 entities to detect a write-write conflict for the
			 * same data under different URLs.
			 */
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
		
		final long begin = System.currentTimeMillis();
		
		final Properties properties = new Properties();

		properties.setProperty(Journal.Options.BUFFER_MODE, BufferMode.DiskRW
				.toString());
		
		// The caller MUST specify the filename using -D on the command line.
		final String journalFile = System.getProperty(Journal.Options.FILE);
		
		properties.setProperty(Journal.Options.FILE, journalFile);
		
		final Journal jnl = new Journal(properties);

		HashCollisionUtility u = null;
		try {

			u = new HashCollisionUtility(jnl);

			for (String filename : args) {

				u.parseFileOrDirectory(new File(filename));
				
			}

			jnl.commit();
			
		} finally {
			
			jnl.close();

			if (u != null) {

				for (int i = 0; i < u.stmtHandler.ncollision.length; i++) {
				
					System.out.println("#collisions="
							+ u.stmtHandler.ncollision[i]);
					
				}
				
			}
			
			final long elapsed = System.currentTimeMillis() - begin;

			System.out.println("Elapsed: " + elapsed + "ms");

			if (new File(journalFile).exists()) {

				System.out.println("Journal size: "
						+ new File(journalFile).length() + " bytes");
				
			}
			
		}

	}

}
