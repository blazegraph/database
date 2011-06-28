package com.bigdata.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.Name2Addr.EntrySerializer;

/**
 * Task returns an array of the named indices on the {@link DataService} to
 * which it is submitted. The array is in the visitation order for the index
 * names (alpha, given the collator choices in force).
 * <p>
 * Note that {@link MetadataService} extends {@link DataService} so this
 * task can also be used to enumerate the scale-out indices in an
 * {@link IBigdataFederation}. However, when enumerating metadata index
 * names note that they all use a prefix to place them into their own
 * namespace.
 * 
 * @see MetadataService#METADATA_INDEX_NAMESPACE
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ListIndicesTask extends DataServiceCallable<String[]> {

    /**
     * 
     */
    private static final long serialVersionUID = -831267313813825903L;

    transient protected static final Logger log = Logger
            .getLogger(ListIndicesTask.class);

    transient protected static final boolean INFO = log.isInfoEnabled();

    /** The timestamp for which the data will be reported. */
    private final long ts;

    /**
     * The namespace prefix for the indices to be returned (may be an empty
     * string to return the names of all registered indices).
     */
    private final String namespace;

//    private transient DataService dataService;

    /**
     * 
     * @param ts
     *            The timestamp for which the data will be reported.
     * @param namespace
     *            The namespace prefix for the indices to be returned (may be an
     *            empty string to return the names of all registered indices).
     */
    public ListIndicesTask(final long ts, final String namespace) {

        this.ts = ts;
        
        this.namespace = namespace;

    }

//    public void setDataService(DataService dataService) {
//
//        this.dataService = dataService;
//
//    }

    public String[] call() throws Exception {

//        if (dataService == null)
//            throw new IllegalStateException("DataService not set.");

        final AbstractJournal journal = getDataService().getResourceManager()
                .getJournal(ts);

        // @todo possible problem if [ts] is a read-write tx.
        final IIndex name2Addr = journal.getName2Addr(ts);

        if (name2Addr == null) {

            // No commit points, no registered indices.
            return new String[0];
            
        }
        
        /*
         * When the namespace prefix was given, generate the fromKey using the
         * namespace.
         * 
         * FIXME There is something wrong with this.  The key for an index entry
         * in name2addr does not appear to be merely the result of appending the
         * name of the index.
         * 
         * First, the actual key for a given Entry:

    key=[65, 49, 79, 41, 47, 41, 79, 41, 7, 144, 81, 38, 124, 38, 122, 45, 8, 29, 63, 49, 87, 8, 29, 57, 47, 38, 126, 79, 49, 75, 65, 1, 29, 1, 125, 143, 126, 143, 143, 133, 143, 143, 143, 143]

Entry{name=metadata-U10c.lex.ID2TERM,checkpointAddr=706455011483,commitTime=1233851302069}

           Second, the generated fromKey.  You can plainly see that it XXX differs where 
           those XXXs are in the line above.  While the toKey is the fixed length successor
           of the fromKey, the fromKey simply does not correspond to the actual key for
           the Entry.

fromKey=[65, 49, 79, 41, 47, 41, 79, 41, 7, 144, 81, 38, 124, 38, 122, 1, 16, 1, 125, 143, 6]

  toKey=[65, 49, 79, 41, 47, 41, 79, 41, 7, 144, 81, 38, 124, 38, 122, 1, 16, 1, 125, 143, 7]
  
         */
        final byte[] fromKey;
        final byte[] toKey;
//        if (namespace.length() > 0) {
//
//            /*
//             * When the namespace prefix was given, generate the toKey as the
//             * fixed length successor of the fromKey.
//             * 
//             * FIXME This is dependent on the manner in which Name2Addr encodes
//             * its key and on the rules for forming the successor of a Unicode
//             * sort key as described in the IKeyBuilder documentation. It should
//             * be easier to do this, probably by raising the successor method
//             * directly into the ITupleSerializer API.
//             */
//
//            final IKeyBuilder keyBuilder = name2Addr.getIndexMetadata()
//                    .getTupleSerializer().getKeyBuilder();
//
//            fromKey = keyBuilder.reset().append(namespace).getKey();
//
//            toKey = keyBuilder.reset().append(namespace).appendNul().getKey();
//
//            if (log.isDebugEnabled()) {
//            
//                log.debug("fromKey=" + BytesUtil.toString(fromKey));
//                
//                log.debug("toKey=" + BytesUtil.toString(toKey));
//                
//            }
//            
//        } else {

            fromKey = null;
            toKey = null;
            
// }

        final int n = (int) name2Addr.rangeCount(fromKey, toKey);

        if (INFO)
            log.info("Will read " + n + " index names within namespace="
                    + namespace + " from "
                    + getDataService().getClass().getSimpleName());

        final List<String> names = new ArrayList<String>(n);

        final ITupleIterator itr = name2Addr.rangeIterator(fromKey, toKey);

        while (itr.hasNext()) {

            final ITuple tuple = itr.next();

            final byte[] val = tuple.getValue();

            final Name2Addr.Entry entry = EntrySerializer.INSTANCE
                    .deserialize(new DataInputBuffer(val));

            if (INFO) {

                if (log.isDebugEnabled()) {

                    log.debug("key=" + BytesUtil.toString(tuple.getKey()));
                    
                }

                log.info(entry.toString());
                
            }

            /*
             * FIXME For the moment, the filter is hacked by examining the
             * de-serialized Entrys.
             */
            if (entry.name.startsWith(namespace)) {

                // acceptable.
                names.add(entry.name);

            }

        }

        return names.toArray(new String[] {});

    }

}
