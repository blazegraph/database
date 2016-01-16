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
 * Created on Jul 25, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BaseIndexStats;
import com.bigdata.btree.DumpIndex;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexTypeEnum;
import com.bigdata.htree.AbstractHTree;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.io.SerializerUtil;
import com.bigdata.relation.RelationSchema;
import com.bigdata.rwstore.IRWStrategy;
import com.bigdata.rwstore.IStore;
import com.bigdata.rwstore.RWStore;
import com.bigdata.rwstore.RWStore.DeleteBlockStats;
import com.bigdata.sparse.GlobalRowStoreSchema;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.stream.Stream;
import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;
import com.bigdata.util.InnerCause;

/**
 * A utility class that opens the journal in a read-only mode and dumps the root
 * blocks and metadata about the indices on a journal file.
 * 
 * TODO add an option to dump only as of a specified commitTime?
 * 
 * TODO GIST : Support all types of indices.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/585"> GIST
 *      </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DumpJournal {

    private static final Logger log = Logger.getLogger(DumpJournal.class);

    /**
     * Dump out the Global Row Store.
     * 
     * TODO Raise as parameter, put on main(), and clean up the code.
     */
    private static final boolean dumpGRS = false;
    
    /**
     * Validate the delete blocks (RWStore only). If there are double- deletes
     * in the delete blocks, then log out more information about those
     * addresses.
     * 
     * TODO Raise as parameter, put on main(), and clean up the code.
     */
    private static final boolean validateDeleteBlocks = false;

//    public DumpJournal() {
//        
//    }
    
    /**
     * Dump one or more journal files:
     * 
     * <pre>
     * usage: (option*) filename+
     * </pre>
     * 
     * where <i>option</i> is any of:
     * <dl>
     * <dt>-namespace</dt>
     * <dd>Dump only those indices having the specified namespace prefix.</dd>
     * <dt>-history</dt>
     * <dd>Dump metadata for indices in all commit records (default only dumps
     * the metadata for the indices as of the most current committed state).</dd>
     * <dt>-indices</dt>
     * <dd>Dump the indices (does not show the tuples by default).</dd>
     * <dt>-pages</dt>
     * <dd>Dump the pages of the indices and reports some information on the
     * page size.</dd>
     * <dt>-tuples</dt>
     * <dd>Dump the records in the indices.</dd>
     * </dl>
     * 
     * where <i>filename</i> is one or more journal file names.
     */
//    FIXME feature is not finished.  Must differentiate different address types.
//    *            <dt>-addr ADDR</dt>
//    *            <dd>Dump the record at that address on the store.</dd>
    public static void main(final String[] args) {
        
		if (args.length == 0) {
            
            System.err.println("usage: (-history|-indices|-pages|-tuples) <filename>+");
            
            System.exit(1);
            
        }

        int i = 0;
        
        // Zero or more namespaces to be dumped. All are dumped if none are
        // specified.
        final List<String> namespaces = new LinkedList<String>();
        
        boolean dumpHistory = false;
        
        boolean dumpIndices = false;

        boolean dumpPages = false;
        
        boolean showTuples = false;
        
        boolean alternateRootBlock = false;
        
        final List<Long> addrs = new LinkedList<Long>();
        
        for(; i<args.length; i++) {
            
            String arg = args[i];
            
            if( ! arg.startsWith("-")) {
                
                // End of options.
                break;
                
            }
            
            if(arg.equals("-history")) {
                
                dumpHistory = true;
                
            }
            
            else if(arg.equals("-namespace")) {
                
                namespaces.add(args[i + 1]);
                
            }
            
            else if(arg.equals("-indices")) {
                
                dumpIndices = true;
                
            }

            else if(arg.equals("-pages")) {
                
                dumpPages = true;
                
            }

            else if(arg.equals("-tuples")) {
                
                showTuples = true;
                
            }

            else if(arg.equals("-alternateRootBlock")) {
                
                alternateRootBlock = true;
                
            }
           
            else if(arg.equals("-addr")) {
                
                addrs.add(Long.valueOf(args[i + 1]));

                i++;
                
            }
            
			else
				throw new RuntimeException("Unknown argument: " + arg);
            
        }

        for (; i < args.length; i++) {

            final File file = new File(args[i]);

            try {

                /*
                 * Stat the file and report on its size, etc.
                 */
                {
                    
                  System.out.println("File: "+file);

                  if(!file.exists()) {

                      System.err.println("No such file");
                      
                      System.exit(1);
                      
                  }
                  
                  if(!file.isFile()) {
                      
                      System.err.println("Not a regular file");
                      
                      System.exit(1);
                      
                  }
                  
                  System.out.println("Length: "+file.length());

                  System.out.println("Last Modified: "+new Date(file.lastModified()));
                  
                }
                
                final Properties properties = new Properties();

                {
                
                    properties.setProperty(Options.FILE, file.toString());
                
                    properties.setProperty(Options.READ_ONLY, "" + true);

                    if (alternateRootBlock)
                        properties.setProperty(Options.ALTERNATE_ROOT_BLOCK,
                                "" + true);

                    properties.setProperty(Options.BUFFER_MODE,
                            BufferMode.Disk.toString());

                }

                System.out.println("Opening (read-only): " + file);

                final Journal journal = new Journal(properties);

                try {

                    final DumpJournal dumpJournal = new DumpJournal(journal);

                    final PrintWriter out = new PrintWriter(System.out, true/* autoFlush */);

                    try {

                        dumpJournal.dumpJournal(out, namespaces, dumpHistory,
                                dumpPages, dumpIndices, showTuples);

                        for (Long addr : addrs) {

                            out.println("addr=" + addr + ", offset="
                                    + journal.getOffset(addr) + ", length="
                                    + journal.getByteCount(addr));

                            // Best effort attempt to dump the record.
                            out.println(dumpJournal.dumpRawRecord(addr));

                        }

                        out.flush();

                    } finally {

                        out.close();

                    }
                    
                } finally {
                    
                    journal.close();
                    
                }

            } catch( Throwable t) {
                
                t.printStackTrace();

                System.err.println("Error: " + t + " on file: " + file);

                // Abnormal completion.
                System.exit(1);
                
            }
            
            System.out.println("==================================");

        }

        System.out.println("Normal completion");
        
    }
    
    /**
     * 
     * @param dumpHistory
     *            Dump metadata for indices in all commit records (default only
     *            dumps the metadata for the indices as of the most current
     *            committed state).
     * @param dumpPages
     *            Dump the pages of the indices and reports some information on
     *            the page size.
     * @param dumpIndices
     *            Dump the indices (does not show the tuples by default).
     * @param showTuples
     *            Dump the records in the indices.
     */
    public void dumpJournal(final boolean dumpHistory, final boolean dumpPages,
            final boolean dumpIndices, final boolean showTuples) {

        final PrintWriter w = new PrintWriter(System.out, true/* autoFlush */);

        try {

            dumpJournal(w, null/* namespaces */, dumpHistory, dumpPages,
                    dumpIndices, showTuples);

            w.flush();
            
        } finally {

            // Note: DO NOT close stdout!
//            w.close();
        }

    }
    
    /**
     * @param out
     *            Where to write the output.
     * @param namespaces
     *            When non-empty and non-<code>null</code>, dump only those
     *            indices having any of the specified namespaces.
     * @param dumpHistory
     *            Dump metadata for indices in all commit records (default only
     *            dumps the metadata for the indices as of the most current
     *            committed state).
     * @param dumpPages
     *            Dump the pages of the indices and reports some information on
     *            the page size.
     * @param dumpIndices
     *            Dump the indices (does not show the tuples by default).
     * @param showTuples
     *            Dump the records in the indices.
     */
    public void dumpJournal(final PrintWriter out, final List<String> namespaces,
            final boolean dumpHistory, final boolean dumpPages,
            final boolean dumpIndices, final boolean showTuples) {

//        Note: This does not fix the issue.
//        /**
//         * Start a transaction. This will bracket all index access and protect
//         * the data on the journal from concurrent recycling.
//         * 
//         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/762">
//         *      DumpJournal does not protect against concurrent updates (NSS)
//         *      </a>
//         */
//        final long tx = journal.newTx(ITx.READ_COMMITTED);
//        try {
//        
        final FileMetadata fmd = journal.getFileMetadata();

        if (fmd != null) {
            
            /*
             * Note: The FileMetadata is only available on a re-open of an
             * existing Journal.
             */

            // dump the MAGIC and VERSION.
            out.println("magic=" + Integer.toHexString(fmd.magic));
            out.println("version="
                    + Integer.toHexString(fmd.version));

            /*
             * Report on:
             * 
             * - the length of the journal. - the #of bytes available for
             * user data in the journal. - the offset at which the next
             * record would be written. - the #of bytes remaining in the
             * user extent.
             */

            final long bytesAvailable = (fmd.userExtent - fmd.nextOffset);

            out.println("extent=" + fmd.extent + "(" + fmd.extent
                    / Bytes.megabyte + "M)" + ", userExtent="
                    + fmd.userExtent + "(" + fmd.userExtent
                    / Bytes.megabyte + "M)" + ", bytesAvailable="
                    + bytesAvailable + "(" + bytesAvailable
                    / Bytes.megabyte + "M)" + ", nextOffset="
                    + fmd.nextOffset);

        }

        {

            /*
             * Dump the root blocks.
             * 
             * Note: This uses the IBufferStrategy to access the root
             * blocks. The code used to use the FileMetadata, but that was
             * only available for a re-opened journal. This approach works
             * for a new Journal as well.
             */
            {

                final ByteBuffer rootBlock0 = journal.getBufferStrategy()
                        .readRootBlock(true/* rootBlock0 */);
                
                if (rootBlock0 != null) {
                
                    out.println(new RootBlockView(
                            true/* rootBlock0 */, rootBlock0,
                            new ChecksumUtility()).toString());
                    
                }
                
            }

            {

                final ByteBuffer rootBlock1 = journal.getBufferStrategy()
                        .readRootBlock(false/* rootBlock0 */);
                
                if (rootBlock1 != null) {
                
                    out.println(new RootBlockView(
                            false/* rootBlock0 */, rootBlock1,
                            new ChecksumUtility()).toString());
                    
                }
                
            }
            // out.println(fmd.rootBlock0.toString());
            // out.println(fmd.rootBlock1.toString());

            // report on which root block is the current root block.
            out.println("The current root block is #"
                    + (journal.getRootBlockView().isRootBlock0() ? 0 : 1));

        }

        final IBufferStrategy strategy = journal.getBufferStrategy();

        if (strategy instanceof RWStrategy) {

            final RWStore store = ((RWStrategy) strategy).getStore();

            {
                final StringBuilder sb = new StringBuilder();

                store.showAllocators(sb);

                out.println(sb);

            }

            // Validate the logged delete blocks.
            if (validateDeleteBlocks) {

                final DeleteBlockStats stats = store.checkDeleteBlocks(journal);

                out.println(stats.toString(store));
                
                final Set<Integer> duplicateAddrs = stats
                        .getDuplicateAddresses();

                if(!duplicateAddrs.isEmpty()) {
                    
                    for(int latchedAddr : duplicateAddrs) {

                        final byte[] b;
                        try {
                            b = store.readFromLatchedAddress(latchedAddr);
                        } catch (IOException ex) {
                            log.error("Could not read: latchedAddr="
                                    + latchedAddr, ex);
                            continue;
                        }
                        final ByteBuffer buf = ByteBuffer.wrap(b);
                        
                        final Object obj = decodeData(buf);

                        if (obj == null) {
                            System.err.println("Could not decode: latchedAddr="
                                    + latchedAddr);
                            final StringBuilder sb = new StringBuilder();
                            BytesUtil.printHexString(sb,
                                    BytesUtil.toHexString(b, b.length));
                            System.err.println("Undecoded record:"
                                    + sb.toString());
                        } else {
                            System.err.println("Decoded record: latchedAddr="
                                    + latchedAddr + " :: class="
                                    + obj.getClass() + ", object="
                                    + obj.toString());
                        }
                    }

                }
                
            }
            
        }

        /*
         * Note: A read-only view is used since the Journal could be exposed to
         * concurrent operations through the NSS.
         */
        final CommitRecordIndex commitRecordIndex = journal
                .getReadOnlyCommitRecordIndex();

		out.println("There are " + commitRecordIndex.getEntryCount()
				+ " commit points.");

        if (dumpGRS) {

            dumpGlobalRowStore(out);
            
		}
		
		if (dumpHistory) {

            out.println("Historical commit points follow in temporal sequence (first to last):");
            
//                final IKeyBuilder keyBuilder = KeyBuilder.newInstance(Bytes.SIZEOF_LONG);
//
//				final long targetTime = 1303505388420L;
//				int indexOf = commitRecordIndex.indexOf(keyBuilder.reset()
//						.append(targetTime).getKey());
//				if (indexOf < 0)
//					indexOf = (-(indexOf) - 1);
//
//				// @todo handle leading/trailing edge cases.
//                final long fromTime = KeyBuilder.decodeLong(commitRecordIndex.keyAt(indexOf-3), 0/*off*/);
//                final long toTime = KeyBuilder.decodeLong(commitRecordIndex.keyAt(indexOf+3), 0/*off*/);
//                
//                final ITupleIterator<CommitRecordIndex.Entry> itr = commitRecordIndex.rangeIterator(
//                		keyBuilder.reset().append(fromTime+1).getKey(),
//                		keyBuilder.reset().append(toTime+1).getKey()
//                		);

            @SuppressWarnings("unchecked")
            final ITupleIterator<CommitRecordIndex.Entry> itr = commitRecordIndex.rangeIterator();
            
            while(itr.hasNext()) {
                
                out.println("----");

                final CommitRecordIndex.Entry entry = itr.next().getObject();
                
                out.print("Commit Record: " + entry.commitTime
                        + ", addr=" + journal.toString(entry.addr)+", ");
                
                final ICommitRecord commitRecord = journal
                        .getCommitRecord(entry.commitTime);
                
                out.println(commitRecord.toString());

                dumpNamedIndicesMetadata(out, namespaces, commitRecord,
                        dumpPages, dumpIndices, showTuples);

            }
            
        } else {

            /*
             * Dump the current commit record.
             */
            
            final ICommitRecord commitRecord = journal.getCommitRecord();
            
            out.println(commitRecord.toString());

            dumpNamedIndicesMetadata(out, namespaces, commitRecord,
                    dumpPages, dumpIndices, showTuples);

        }
//        } finally {
//            journal.abort(tx);
//        }

    }

    private final Journal journal;

    public DumpJournal(final Journal journal) {

        if (journal == null)
            throw new IllegalArgumentException();
        
        this.journal = journal;

    }
    
    private void dumpGlobalRowStore(final PrintWriter out) {
        
        final SparseRowStore grs = journal.getGlobalRowStore(journal
                .getLastCommitTime());
        
        {
            final Iterator<? extends ITPS> itr = grs
                    .rangeIterator(GlobalRowStoreSchema.INSTANCE);
            
            while(itr.hasNext()) {
                
                final ITPS tps = itr.next();
                
                out.println(tps.toString());
                
            }
            
        }

        // The schema for "relations".
        {
            
            final Iterator<? extends ITPS> itr = grs
                    .rangeIterator(RelationSchema.INSTANCE);
            
            while(itr.hasNext()) {
                
                final ITPS tps = itr.next();
                
                out.println(tps.toString());
                
            }
            
        }

    }
    
    /**
    * Dump metadata about each named index as of the specified commit record.
    * 
    * @param dumpPages
    *           When <code>true</code>, the index pages will be recursively
    *           scanned to collect statistics about the index.
    */
    private void dumpNamedIndicesMetadata(final PrintWriter out,
            final List<String> namespaces, final ICommitRecord commitRecord,
            final boolean dumpPages, final boolean dumpIndices,
            final boolean showTuples) {

        final Iterator<String> nitr = journal.indexNameScan(null/* prefix */,
                commitRecord.getTimestamp());

        final Map<String, BaseIndexStats> pageStats = new TreeMap<String, BaseIndexStats>();

        while (nitr.hasNext()) {

            // a registered index.
            final String name = nitr.next();

            if (namespaces != null && !namespaces.isEmpty()) {

                boolean found = false;
                
                for(String namespace : namespaces) {
                    
                    if (name.startsWith(namespace)) {
                        
                        found = true;
                
                        break;
                        
                    }

                }
                
                if (!found) {
                 
                    // Skip this index. Not a desired namespace.
                    continue;
                    
                }

            }
            
            out.println("name=" + name);

            // load index from its checkpoint record.
            final ICheckpointProtocol ndx;
            try {

                ndx = journal.getIndexWithCommitRecord(name, commitRecord);

            } catch (Throwable t) {

                if (InnerCause.isInnerCause(t, ClassNotFoundException.class)) {

                    /*
                     * This is typically a tuple serializer that has a
                     * dependency on an application class that is not present in
                     * the CLASSPATH. Add the necessary dependency(s) and you
                     * should no longer see this message.
                     */
                    
                    log.warn("Could not load index: "
                            + InnerCause.getInnerCause(t,
                                    ClassNotFoundException.class));
                    
                    continue;
                    
                } else
                    throw new RuntimeException(t);
                
            }

            // show checkpoint record.
            out.println("\t" + ndx.getCheckpoint());

            // show metadata record.
            out.println("\t" + ndx.getIndexMetadata());

            /*
             * Collect statistics on the page usage for the index.
             * 
             * TODO If we kept the BTree counters for the #of bytes written per
             * node and per leaf up to date when nodes and leaves were recycled
             * then we could generate (parts of) this table very quickly. As it
             * stands, we have to actually scan the pages in the index.
             */
            {

               final BaseIndexStats stats = ndx.dumpPages(
                     dumpPages/* recursive */, dumpPages/* visitLeaves */);

                out.println("\t" + stats);

                pageStats.put(name, stats);

                if (dumpIndices) {

                    if (ndx instanceof AbstractBTree) {
                    
                        /*
                         * TODO GIST : dumpTuples for HTree.
                         */
                        
                        DumpIndex.dumpIndex((AbstractBTree) ndx, showTuples);
                        
                    }

                }

            }

        } // while(itr) (next index)

        // Write out the statistics table.
        BaseIndexStats.writeOn(out, pageStats);
//        {
//
//            /*
//             * Write out the header.
//             */
//            boolean first = true;
//
//            for (Map.Entry<String, BaseIndexStats> e : pageStats.entrySet()) {
//
//                final String name = e.getKey();
//
//                final BaseIndexStats stats = e.getValue();
//
//                if (stats == null) {
//
//                    /*
//                     * Something for which we did not extract the PageStats.
//                     */
//
//                    final ICheckpointProtocol tmp = journal
//                            .getIndexWithCommitRecord(name, commitRecord);
//
//                    out.println("name: " + name + ", class="
//                            + tmp.getClass() + ", checkpoint="
//                            + tmp.getCheckpoint());
//
//                    continue;
//                    
//                }
//
//                if (first) {
//
//                    out.println(stats.getHeaderRow());
//
//                    first = false;
//
//                }
//                
//                /*
//                 * Write out the stats for this index.
//                 */
//
//                out.println(stats.getDataRow());
//
//            }
//
//        }
        
    } // dumpNamedIndicesMetadata

    /**
     * Utility method dumps the data associated with an address on the backing
     * store. A variety of methods are attempted.
     * 
     * @param addr
     *            The address.
     * 
     * @return
     */
    private String dumpRawRecord(final long addr) {

        if (journal.getBufferStrategy() instanceof IRWStrategy) {
            /**
             * TODO When we address this issue, do this test for all stores.
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/555">
             *      Support PSOutputStream/InputStream at IRawStore </a>
             */
            final IStore store = ((IRWStrategy) journal.getBufferStrategy())
                    .getStore();
            try {
                final InputStream is = store.getInputStream(addr);
                try {
                    // TODO Could dump the stream.
                } finally {
                    try {
                        is.close();
                    } catch (IOException e) {
                        // Ignore.
                    }
                }
                return "Address is stream: addr=" + addr;
            } catch (RuntimeException ex) {
                // ignore.
            }
        }
        
        final ByteBuffer buf;
        try {
        
            buf = journal.read(addr);
            
        } catch (Throwable t) {
            
            final String msg = "Could not read: addr=" + addr + ", ex=" + t;
            
            log.error(msg, t);
            
            return msg;
        }
 
        if (buf == null)
            throw new IllegalArgumentException("Nothing at that address");
        
        final Object obj = decodeData(buf);
        
        if (obj == null) {

            return "Could not decode: addr=" + addr;
            
        } else {

            return obj.toString();
            
        }

    }

    /**
     * Attempt to decode data read from some address using a variety of
     * mechanisms.
     * 
     * @param b
     *            The data.
     *            
     * @return The decoded object -or- <code>null</code> if the object could not
     *         be decoded.
     */
    private Object decodeData(final ByteBuffer buf) {

        if(buf == null)
            throw new IllegalArgumentException();
      
        /*
         * Note: Always use buf.duplicate() to avoid a side-effect on the
         * ByteBuffer that we are trying to decode!
         */
        
        try {
            /**
             * Note: This handles a lot of cases, including:
             * 
             * Checkpoint, IndexMetadata
             */
            return SerializerUtil.deserialize(buf.duplicate());
        } catch (RuntimeException ex) {
            // fall through
        }

        /*
         * TODO Root blocks and what else?
         */
        
        /*
         * Try to decode an index node/leaf.
         */
        {
            final long commitTime = journal.getLastCommitTime();
            
            final Iterator<String> nitr = journal.indexNameScan(
                    null/* prefix */, commitTime);

            while (nitr.hasNext()) {

                // a registered index.
                final String name = nitr.next();

                final ICheckpointProtocol ndx = journal.getIndexLocal(name,
                        commitTime);

                final IndexTypeEnum indexType = ndx.getCheckpoint()
                        .getIndexType();
                
                switch (indexType) {
                case BTree: {
                    
                    final AbstractBTree btree = (AbstractBTree) ndx;

                    final com.bigdata.btree.NodeSerializer nodeSer = btree
                            .getNodeSerializer();
                    
                    try {
                    
                        final com.bigdata.btree.data.IAbstractNodeData nodeOrLeaf = nodeSer
                                .decode(buf.duplicate());

                        log.warn("Record decoded from index=" + name);
                        
                        return nodeOrLeaf;
                        
                    } catch (Throwable t) {
                        // ignore.
                        continue;
                    }
                }
                case HTree: {

                    final AbstractHTree htree = (AbstractHTree)ndx;
                    
                    final com.bigdata.htree.NodeSerializer nodeSer = htree.getNodeSerializer();

                    try {

                        final com.bigdata.btree.data.IAbstractNodeData nodeOrLeaf = nodeSer
                                .decode(buf.duplicate());

                        log.warn("Record decoded from index=" + name);

                        return nodeOrLeaf;
                        
                    } catch (Throwable t) {
                        // Ignore.
                        continue;
                    }
                }
                case Stream:
                    @SuppressWarnings("unused")
                    final Stream stream = (Stream) ndx;
                    /*
                     * Note: We can't do anything here with a Stream, but we do
                     * try to read on the address as a stream in the caller.
                     */
                    continue;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown indexType=" + indexType);
                }

            }

        }

        // Could not decode.
        return null;

    }

}
