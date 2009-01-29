package com.bigdata.service;

import java.io.Serializable;
import java.util.Vector;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

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
public class ListIndicesTask implements Callable<String[]>,
        IDataServiceAwareProcedure, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -831267313813825903L;

    transient protected static final Logger log = Logger
            .getLogger(ListIndicesTask.class);

    transient protected static final boolean INFO = log.isInfoEnabled();

    private final long ts;

    private transient DataService dataService;

    /**
     * 
     * @param ts
     *            The timestamp for which the data will be reported.
     */
    public ListIndicesTask(final long ts) {

        this.ts = ts;

    }

    public void setDataService(DataService dataService) {

        this.dataService = dataService;

    }

    public String[] call() throws Exception {

        if (dataService == null)
            throw new IllegalStateException("DataService not set.");

        final AbstractJournal journal = dataService.getResourceManager()
                .getJournal(ts);

        // @todo possible problem if [ts] is a read-write tx.
        final IIndex name2Addr = journal.getName2Addr(ts);

        final int n = (int) name2Addr.rangeCount();

        if (INFO)
            log.info("Will read " + n + " index names from "
                    + dataService.getClass().getSimpleName());

        final Vector<String> names = new Vector<String>(n);

        final ITupleIterator itr = name2Addr.rangeIterator();

        while (itr.hasNext()) {

            final ITuple tuple = itr.next();

            final byte[] val = tuple.getValue();

            final Name2Addr.Entry entry = EntrySerializer.INSTANCE
                    .deserialize(new DataInputBuffer(val));

            if (INFO) {

                log.info(entry.toString());

            }

            names.add(entry.name);

        }

        return names.toArray(new String[] {});

    }

}
