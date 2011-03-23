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
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder.DefaultFrontCodedRabaCoder;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * Utility class to parse some RDF resource(s) and count hash collisions using a
 * variety of hash codes.
 * 
 * TODO order preserving hash codes could be interesting here. Look at 32 and 64
 * bit variants of the math and at generalized order preserving hash codes. With
 * order preserving hash codes, it makes sense to insert all Unicode terms into
 * TERM2ID such that we have
 * 
 * TODO benchmark the load time with different hash codes. the cost of the hash
 * computation and the randomness of the distribution will both play a role. The
 * B+Tree will need to be setup with a sufficient [writeRetentionQueue] and we
 * will need to specify [-server -Xmx1G].
 * 
 * SHA-256 - no collisions on BSBM 200M. 30G file. time?
 * 
 * 32-bit hash codes. #collisions=1544132 Elapsed: 16656445ms Journal size:
 * 23841341440 bytes (23G)
 * 
 * Now limiting the size of values in a leaf and also increasing the branching
 * factor to 512 (was 32). [The current run is scanning after the initial
 * insert, which involves a little wasted effort. It was also without the -server
 * -Xmx, and write retention queue parameters].
 * 
 * TODO Do not bother comparing time until I have moved the large records out of
 * line (blob references). This should be automatic, but that interacts with how
 * we code values and leaves and might be tricky to make automatic for all
 * coders.
 * 
 * TODO Try with only N bytes worth of the SHA hash code, leaving some bits left
 * over for partitioning URIs, Literals, and BNodes (for told bnode mode) and
 * for a counter to break ties when there is a hash collision. We should wind up
 * with an 8-12 byte termId which is collision proof and very well distributed.
 * 
 * TODO Add bit flags at the front for {BLOB, URI, Literal, BNode} (BLOB being
 * the odd one out). If we move BLOBs out of the key range of other plain
 * literals, or literals of a given language code or datatype, then we can not
 * do an ordered scan of the literals anymore which is inclusive of the blobs.
 * There is a similar consequence of moving small literals into the statement
 * index.
 * <p>
 * If we inline small unicode values (<32 bytes) and reserve the TERM2ID index
 * for large(r) values then we can approach a situation in which it serves
 * solely for blobs but with a tradeoff in size (of the statement indices)
 * versus indirection.
 * <p>
 * Large value promotion does not really let us handle large blobs
 * (multi-megabytes) in s/o as a 50 4M blobs would fill up a shard. There, I
 * think that we need to give the control over to the application and require it
 * to write on a shared resource (shared file system, S3, etc). The value
 * inserted into the index would then be just the pathname in the shared file
 * system or the URL of the S3 resource. This breaks the ACID decision boundary
 * though as the application has no means available to atomically decide that
 * the resource does not exist and hence create it. Even using a conditional
 * E-Tag on S3 would not work since it would have to have an index over the S3
 * entities to detect a write-write conflict for the same data under different
 * URLs.
 * 
 * @author thompsonbry
 */
public class HashCollisionUtility {

	private final static Logger log = Logger
			.getLogger(HashCollisionUtility.class);

	private final BTree[] indices;

	private final BigdataValueFactory vf;

	private final LexiconConfiguration<BigdataValue> conf;
	
	private final StatementHandler stmtHandler;
	
	/** 
	 * The size of the hash collision set for the RDF Value with the most
	 * hash collisions observed to date.
	 */
	private final AtomicLong maxCollisions = new AtomicLong();
	
	/**
	 * The #of distinct RDF {@link Value}s inserted into the index.
	 */
	private final AtomicLong ninserted = new AtomicLong();
	
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

			/*
			 * increase the branching factor since leaf size is smaller w/o
			 * large records.
			 */
			md.setBranchingFactor(512);
			
			// Note: You need to give sufficient heap for this option!
			md.setWriteRetentionQueueCapacity(8000);
			
			ndx = jnl.registerIndex(name, md);
			
		}
		
		indices = new BTree[] { ndx };

		vf = BigdataValueFactoryImpl.getInstance("test");
		
		// factory does not support any extensions.
		final IExtensionFactory xFactory = new IExtensionFactory() {

			public void init(LexiconRelation lex) {
				// NOP
			}

			@SuppressWarnings("unchecked")
			public IExtension[] getExtensions() {
				return new IExtension[] {};
			}
		};

		/*
		 * Note: This inlines everything *except* xsd:dateTime, which
		 * substantially reduces the data we will put into the index.
		 * 
		 * @todo Do a special IExtension implementation to handle xsd:dateTime
		 * since the DateTimeExtension uses the LexiconRelation to do its work.
		 */
		conf = new LexiconConfiguration<BigdataValue>(
				true, // inlineLiterals
				true, // inlineBNodes
				false, // inlineDateTimes
				xFactory // extension factory
				);
		
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

		private IKeyBuilder buildKey(Value r, byte[] val) {

			if (true) {

				/*
				 * Simple 32-bit hash code based on the byte[] representation of
				 * the RDF Value.
				 */
				
				final int hashCode = r.hashCode();

				return keyBuilder.reset().append(hashCode);
				
			} else {

				/*
				 * Message digest of the serialized representation of the RDF
				 * Value.
				 * 
				 * TODO There are methods to copy out the digest (hash code)
				 * without memory allocations. getDigestLength() and
				 * getDigest(out,start,len).
				 */
				
				final byte[] hashCode = d.digest(val);

				return keyBuilder.reset().append(hashCode);
				
			}

		}

		private void addValue(final BTree ndx, final AtomicLong c, final Value r) {

			if (conf.createInlineIV(r) != null) {

				/*
				 * This is something that we would inline into the statement
				 * indices.
				 */

				return;
				
			}

			/*
			 * TODO This can not handle very large UTF strings. To do that we
			 * need to either explore ICU support for that feature or serialize
			 * the data using "wide" characters (java uses 2-byte characters).
			 */
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
			 */
			
			final IKeyBuilder keyBuilder = buildKey(r, val);
			
			// key strictly LT any full key for the hash code of this val but
			// strictly GT any key have a hash code LT the hash code of this val.
			final byte[] fromKey = keyBuilder.getKey();

			// key strictly LT any successor of the hash code of this val.
			final byte[] toKey = SuccessorUtil.successor(fromKey.clone());

			// fast range count. this tells us how many collisions there are.
			// this is an exact collision count since we are not deleting tuples
			// from the TERMS index.
			final long rangeCount = ndx.rangeCount(fromKey, toKey);
			
			if (rangeCount == 0) {

				/*
				 * This is the first time we have observed a Value which
				 * generates this hash code, so append a [short] ZERO (0) to
				 * generate the actual key and then insert the Value into the
				 * index. Since there is nothing in the index for this hash
				 * code, no collision is possible and we do not need to test the
				 * index for the value before inserting the value into the
				 * index.
				 */
				final byte[] key = keyBuilder.append((short) rangeCount)
						.getKey();

				if (ndx.insert(key, val) != null) {

					throw new AssertionError();
					
				}
				
				ninserted.incrementAndGet();

				return;
				
			}

			/*
			 * iterator over that key range
			 * 
			 * TODO filter for the value of interest so we can optimize the
			 * scan by comparing with the value without causing it to be
			 * materialized. we can also visit something iff the desired tuple
			 * already exists. if we visit nothing then we know that we have to
			 * insert a tuple and we know the counter value from the collision
			 * count.
			 */
			final ITupleIterator<?> itr = ndx.rangeIterator(fromKey, toKey,
					0/* capacity */, IRangeQuery.VALS, null/* filter */);

			boolean found = false;
			
			while(itr.hasNext()) {
				
				final ITuple<?> tuple = itr.next();
				
				if(BytesUtil.bytesEqual(val, tuple.getValue())) {
					
					found = true;

					break;
					
				}
				
			}
			
			if(found) {
				
				// Already in the index.
				return;

			}

			/*
			 * Hash collision.
			 */

			if (rangeCount >= Short.MAX_VALUE) {

				/*
				 * Impose a hard limit on the #of hash collisions we will accept
				 * in this utility.
				 * 
				 * @todo We do not need to have a hard limit if we use
				 * BigInteger for the counter, but the performance will go
				 * through the floor if we have to scan 32k entries on a hash
				 * collision!
				 */

				throw new RuntimeException("Too many hash collisions: ncoll="
						+ rangeCount);

			}

			if (rangeCount > maxCollisions.get()) {

				// Raise the maximum collision count.

				maxCollisions.set(rangeCount);

				log.warn("MAX COLLISIONS NOW: " + maxCollisions.get());

			}
			
			final byte[] key = keyBuilder.append((short) rangeCount).getKey();

			// Insert into the index.
			if (ndx.insert(key, val) != null) {

				throw new AssertionError();

			}

			ninserted.incrementAndGet();

			if (rangeCount > 128) { // arbitrary limit to log @ WARN.
				log.warn("Collision: hashCode=" + BytesUtil.toString(key)
						+ ", ninserted=" + ninserted + ", mcoll="
						+ maxCollisions + ", ncoll=" + rangeCount
						+ ", resource=" + r);
			} else if (log.isInfoEnabled())
				log.info("Collision: hashCode=" + BytesUtil.toString(key)
						+ ", ninserted=" + ninserted + ", mcoll="
						+ maxCollisions + ", ncoll=" + rangeCount
						+ ", resource=" + r);

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
