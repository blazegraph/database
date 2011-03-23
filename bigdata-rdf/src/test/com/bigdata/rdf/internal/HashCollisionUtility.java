package com.bigdata.rdf.internal;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.XMLSchema;
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
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.btree.raba.codec.CanonicalHuffmanRabaCoder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueSerializer;

/**
 * Utility class to parse some RDF resource(s) and count hash collisions using a
 * variety of hash codes.
 * 
 * TODO Various data sets:
 * 
 * <pre>
 * /nas/data/bsbm/bsbm_2785/dataset.nt.gz
 * /nas/data/bsbm/bsbm_566496/dataset.nt.gz
 * /nas/data/lubm/U1/data/University0/
 * 
 * 8B triple bioinformatics data set.
 * 
 * BTC data (some very large literals, also fix nxparser to not drop/truncate)
 * </pre>
 * 
 * TODO order preserving hash codes could be interesting here. Look at 32 and 64
 * bit variants of the math and at generalized order preserving hash codes. With
 * order preserving hash codes, it makes sense to insert all Unicode terms into
 * TERM2ID such that we have a total order there.
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
 * insert, which involves a little wasted effort. It was also without the
 * -server -Xmx2g, and write retention queue parameters. Finally, it was
 * serializing BigdataValue objects, including their IV, rather than RDF Value
 * objects. The code has since been modified to serialize just the BigdataValue
 * Also, I've since raised the initial extent from 10M to 200M].
 * maxCollisions=3, Elapsed: 22579073ms Journal size: 35270950912 bytes
 * 
 * Now buffering 100k values at a time: 2x faster.
 * 
 * <pre>
 * U1:
 * Elapsed: 23379ms
 * NumStatements: 1000313
 * NumDistinctVals: 291259
 * TotalKeyBytes: 1747554
 * TotalValBytes: 60824514
 * MaxCollisions: 1
 * TotalCollisions: 6
 * Journal size: 209715200 bytes
 * name	m	height	nnodes	nleaves	nodeBytes	leafBytes	totalBytes	avgNodeBytes	avgLeafBytes	minNodeBytes	maxNodeBytes	minLeafBytes	maxLeafBytes
 * lex	1024	1	1	474	7913	3662623	3670536	7913	7727	7913	7913	5786	13784
 * </pre>
 * 
 * With only a byte (versus short) counter in the key. Oddly, this has no impact
 * on the average leaf size. That suggests that the keys in the leaves are very
 * sparse in terms of the hash code space such that prefix compression is not
 * really doing that much for us.
 * 
 * <pre>
 * Elapsed: 23235ms
 * NumStatements: 1000313
 * NumDistinctVals: 291259
 * TotalKeyBytes: 1456295
 * TotalValBytes: 60824514
 * MaxCollisions: 1
 * TotalCollisions: 6
 * Journal size: 209715200 bytes
 * name	m	height	nnodes	nleaves	nodeBytes	leafBytes	totalBytes	avgNodeBytes	avgLeafBytes	minNodeBytes	maxNodeBytes	minLeafBytes	maxLeafBytes
 * lex	1024	1	1	474	7913	3371370	3379283	7913	7112	7913	7913	5274	12774
 * </pre>
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

	private final BTree ndx;

	private final BigdataValueFactory vf;

	private final LexiconConfiguration<BigdataValue> conf;
	
	private final StatementHandler stmtHandler;
	
	/** 
	 * The size of the hash collision set for the RDF Value with the most
	 * hash collisions observed to date.
	 */
	private final AtomicLong maxCollisions = new AtomicLong();

	/**
	 * The total #of hash collisions.
	 */
	private final AtomicLong totalCollisions = new AtomicLong();

	/**
	 * The #of distinct RDF {@link Value}s inserted into the index.
	 */
	private final AtomicLong ninserted = new AtomicLong();
	
	/** The total #of bytes in the generated B+Tree keys (leaves only). */
	private final AtomicLong totalKeyBytes = new AtomicLong();

	/** The total #of bytes in the serialized RDF Values. */
	private final AtomicLong totalValBytes = new AtomicLong();
	
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
		

		/*
		 * TODO CanonicalHuffmanRabaCoder for U1 drops the average leaf size
		 * 
		 * @ m=512 from 24k to 16k. Experiment with performance tradeoff
		 * when compared with gzip of the record.
		 * 
		 * No apparent impact for U1 on the leaves or nodes for 32 versus 8
		 * on the front-coded raba.
		 * 
		 * Dropping maxRecLen from 256 to 64 reduces the leaves from 16k to
		 * 10k. Dropping it to ZERO (0) reduces the leaves to 5k. This
		 * suggests that we could to much better if we keep all RDF Values
		 * out of the index. In standalone, we can give people a TermId
		 * which is the raw record address. However, in scale-out it needs
		 * to be the key (to locate the shard) and we will resolve the RDF
		 * Value using the index on the shard.
		 * 
		 * Suffix compression would allow us to generalize the counter and
		 * avoid index space costs when collisions are rare while being able
		 * to tolerate more collisions (short versus byte).

U1: m=800, q=8000, ratio=8, maxRecLen=0, 
Elapsed: 41340ms
NumStatements: 1000313
NumDistinctVals: 291259
TotalKeyBytes: 1747554
TotalValBytes: 60824514
MaxCollisions: 1
TotalCollisions: 6
Journal size: 209715200 bytes
Average node: 9813
Average leaf: 6543

U1: m=800, q=8000, ratio=32, maxRecLen=0, 
Elapsed: 40971ms
NumStatements: 1000313
NumDistinctVals: 291259
TotalKeyBytes: 1747554
TotalValBytes: 60824514
MaxCollisions: 1
TotalCollisions: 6
Journal size: 209715200 bytes
Average node: 9821	
Average leaf: 6478

U1: m=800, q=8000, ratio=64, maxRecLen=0, 
Elapsed: 41629ms
NumStatements: 1000313
NumDistinctVals: 291259
TotalKeyBytes: 1747554
TotalValBytes: 60824514
MaxCollisions: 1
TotalCollisions: 6
Journal size: 209715200 bytes
Average node: 9822
Average leaf: 6467

U1: m=512, q=8000, ratio=32, maxRecLen=0, 
Elapsed: 44722ms
NumStatements: 1000313
NumDistinctVals: 291259
TotalKeyBytes: 1747554
TotalValBytes: 60824514
MaxCollisions: 1
TotalCollisions: 6
Journal size: 209715200 bytes
Average node/leaf: 3969	4149

U1: m=512, q=8000, ratio=32, maxRecLen=0, 
Elapsed: 40519ms
NumStatements: 1000313
NumDistinctVals: 291259
TotalKeyBytes: 1747554
TotalValBytes: 60824514
MaxCollisions: 1
TotalCollisions: 6
Journal size: 209715200 bytes
Average node/leaf, node(min/max), leaf(min/max): 7583	8326	7583	7583	5755	14660

It would be great if we tracked the node/leaf data live on the RWStore for
these counters so it could all be reported periodically (via http) or at the
end in a summary.

TODO The front compression of the keys is not helping out much since the keys
are so sparse in the hash code space.  It is a Good Thing that the keys are so
sparse, but this suggests that we should try a different coder for the leaf keys.
		 */
		final int m = 1024;
		final int q = 8000;
		final int ratio = 32;
		final int maxRecLen = 0;
		final int valBufSize = 1000000; // TODO Try 1M.
		if(ndx == null) {
			
			final IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());
			
			md.setNodeKeySerializer(new FrontCodedRabaCoder(ratio));
			
			final DefaultTupleSerializer tupleSer = new DefaultTupleSerializer(
					new DefaultKeyBuilderFactory(new Properties()),//
//					DefaultFrontCodedRabaCoder.INSTANCE,//
					new FrontCodedRabaCoder(ratio),//
					CanonicalHuffmanRabaCoder.INSTANCE
//					new SimpleRabaCoder()//
			);
			
			md.setTupleSerializer(tupleSer);
			
			// enable raw record support.
			md.setRawRecords(true);
			
			// set the maximum length of a byte[] value in a leaf.
			md.setMaxRecLen(maxRecLen);

			/*
			 * increase the branching factor since leaf size is smaller w/o
			 * large records.
			 */
			md.setBranchingFactor(m);
			
			// Note: You need to give sufficient heap for this option!
			md.setWriteRetentionQueueCapacity(q);
			
			ndx = jnl.registerIndex(name, md);
			
		}
		
		this.ndx = ndx;

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
		
		stmtHandler = new StatementHandler(valBufSize);
		
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
    	 * Map of distinct values in the buffer.
    	 */
    	private final Set<Value> distinctValues = new LinkedHashSet<Value>();
    	
		/** Buffer for values. */
		private final Value[] values;

		/** #of buffered values. */
		private int nvalues = 0;
    	
    	/**
    	 * #of statements visited.
    	 */
    	private final AtomicLong nstmts = new AtomicLong();

    	/**
    	 * Used to build the keys.
    	 */
    	private final IKeyBuilder keyBuilder = KeyBuilder.newInstance();
    	
		/** Used to serialize RDF Values as byte[]s. */
    	private final DataOutputBuffer out = new DataOutputBuffer();

		/** Used to serialize RDF Values as byte[]s. */
    	private final BigdataValueSerializer<Value> valSer = new BigdataValueSerializer<Value>(vf);
    	
		private final MessageDigest d;

		public StatementHandler(final int valueBufSize) {
        	
			values = new Value[valueBufSize];
			
			try {
			
				d = MessageDigest.getInstance("SHA-256"); // 256 bits (32 bytes)
				
			} catch (NoSuchAlgorithmException e) {
				
				throw new RuntimeException(e);
				
			}

		}

		public void handleStatement(final Statement stmt)
				throws RDFHandlerException {

			bufferValue(stmt.getSubject());

			bufferValue(stmt.getPredicate());

			bufferValue(stmt.getObject());

			if (stmt.getContext() != null) {

				bufferValue(stmt.getContext());

			}

			nstmts.incrementAndGet();

		}

		private void bufferValue(final Value value) {

			if (conf.createInlineIV(value) != null) {
				
				/*
				 * This is something that we would inline into the statement
				 * indices.
				 */

				return;
				
			}

			if (value instanceof BigdataLiteral) {

				final URI datatype = ((BigdataLiteral) value).getDatatype();

				if (datatype != null && XMLSchema.DATETIME.equals(datatype)) {

					// TODO xsd:dateTime should be inlined by the real code....
					return;
					
				}
				
			}
			
			if (nvalues == values.length) {

				flush();
				
			}
			
			if (nvalues < values.length) {

				if(distinctValues.add(value)) {

					// Something not already buffered.
					values[nvalues++] = value;
					
				}
				
			}
			
		}

		protected void flush() {

			if (nvalues == 0)
				return;

			if (log.isInfoEnabled())
				log.info("Flushing " + nvalues + " values");
			
			final KVO<Value>[] a = new KVO[nvalues];

			for (int i = 0; i < nvalues; i++) {

				final Value r = values[i];
				
				/*
				 * TODO This can not handle very large UTF strings. To do that
				 * we need to either explore ICU support for that feature or
				 * serialize the data using "wide" characters (java uses 2-byte
				 * characters).
				 */
//				final byte[] val = SerializerUtil.serialize(r);
				final byte[] val = valSer.serialize(r, out.reset());

				/*
				 * Note: This is an exclusive lower bound (it does not include
				 * the counter).
				 * 
				 * TODO We could format the counter in here as a ZERO (0) since
				 * it is a fixed length value and then patch it up later. That
				 * would involve less copying. Also, the [fromKey] and [toKey]
				 * could reuse a pair of buffers to reduce heap churn,
				 * especially since they are FIXED length keys. The fromKey
				 * would have to be formed more intelligently as we do not have
				 * a version of SuccessorUtil#successor() which works with a
				 * byte offset and length.
				 */
				final byte[] key = buildKey(r, val).getKey();

				a[i] = new KVO<Value>(key, val, r);
				
			}

			// Place into sorted order by the keys.
			Arrays.sort(a, 0, nvalues);

			for(KVO<Value> x : a) {
				
				addValue(x);
				
			}
			
			reset();

		}

		private void reset() {
			
			// clear distinct value set.
			distinctValues.clear();
			
			for (int i = 0; i < nvalues; i++) {

				// clear references from the buffer.
				values[i] = null;
				
			}

			// reset buffer counter.
			nvalues = 0;

		}
		
		private IKeyBuilder buildKey(final Value r, final byte[] val) {

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

		private void addValue(final KVO<Value> x) {

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
			
//			final IKeyBuilder keyBuilder = buildKey(r, val);
			
			final byte[] val = x.val;
			
			// key strictly LT any full key for the hash code of this val but
			// strictly GT any key have a hash code LT the hash code of this val.
			final byte[] fromKey = x.key;

			// key strictly LT any successor of the hash code of this val.
			final byte[] toKey = SuccessorUtil.successor(fromKey.clone());

			// fast range count. this tells us how many collisions there are.
			// this is an exact collision count since we are not deleting tuples
			// from the TERMS index.
			final long rangeCount = ndx.rangeCount(fromKey, toKey);

			if (rangeCount >= Byte.MAX_VALUE) {

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

			// force range count into (signed) byte
			final byte counter = (byte) rangeCount;
			
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
				final byte[] key = keyBuilder.reset().append(fromKey).append(
						counter).getKey();

				if (ndx.insert(key, val) != null) {

					throw new AssertionError();
					
				}
				
				ninserted.incrementAndGet();
				totalKeyBytes.addAndGet(key.length);
				totalValBytes.addAndGet(val.length);

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

			if (rangeCount > maxCollisions.get()) {

				// Raise the maximum collision count.

				maxCollisions.set(rangeCount);

				log.warn("MAX COLLISIONS NOW: " + maxCollisions.get());

			}
			
			final byte[] key = keyBuilder.reset().append(fromKey).append(
					counter).getKey();

			// Insert into the index.
			if (ndx.insert(key, val) != null) {

				throw new AssertionError();

			}

			ninserted.incrementAndGet();
			totalKeyBytes.addAndGet(key.length);
			totalValBytes.addAndGet(val.length);
			
			totalCollisions.incrementAndGet();

			if (rangeCount > 128) { // arbitrary limit to log @ WARN.
				log.warn("Collision: hashCode=" + BytesUtil.toString(key)
						+ ", ninserted=" + ninserted + ", totalCollisions="
						+ totalCollisions + ", maxCollisions=" + maxCollisions
						+ ", ncollThisTerm=" + rangeCount + ", resource=" + x.obj);
			} else if (log.isInfoEnabled())
				log.info("Collision: hashCode=" + BytesUtil.toString(key)
						+ ", ninserted=" + ninserted + ", totalCollisions="
						+ totalCollisions + ", maxCollisions=" + maxCollisions
						+ ", ncollThisTerm=" + rangeCount + ", resource=" + x.obj);

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

		properties.setProperty(Journal.Options.INITIAL_EXTENT, ""
				+ (Bytes.megabyte * 200));

		properties.setProperty(Journal.Options.COLLECT_PLATFORM_STATISTICS,"true");
		properties.setProperty(Journal.Options.COLLECT_QUEUE_STATISTICS,"true");
		properties.setProperty(Journal.Options.HTTPD_PORT,"8081");
		
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

			// flush anything left in the buffer.
			u.stmtHandler.flush();
			
			jnl.commit();
			
		} finally {
			
			jnl.close();

			final long elapsed = System.currentTimeMillis() - begin;

			System.out.println("Elapsed: " + elapsed + "ms");

			if (u != null) {

				System.out.println("NumStatements: " + u.stmtHandler.nstmts);

				System.out.println("NumDistinctVals: " + u.ninserted);

				System.out.println("TotalKeyBytes: " + u.totalKeyBytes);
				
				System.out.println("TotalValBytes: " + u.totalValBytes);

				System.out.println("MaxCollisions: " + u.maxCollisions);

				System.out.println("TotalCollisions: " + u.totalCollisions);

			}
			
			if (new File(journalFile).exists()) {

				System.out.println("Journal size: "
						+ new File(journalFile).length() + " bytes");
				
			}
			
		}

	}

}
