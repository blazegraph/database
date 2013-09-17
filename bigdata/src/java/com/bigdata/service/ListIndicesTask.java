package com.bigdata.service;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractJournal;

/**
 * Task returns an array of the named indices on the {@link DataService} to
 * which it is submitted. The array is in the visitation order for the index
 * names (alpha, given the collator choices in force).
 * <p>
 * Note that {@link MetadataService} extends {@link DataService} so this
 * task can also be used to enumerate the scale-out indices in an
 * {@link IBigdataFederation}. However, when enumerating metadata index
 * names, note that they all use a prefix to place them into their own
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
     *            <p>
     *            Note: This SHOULD include a "." if you want to restrict the
     *            scan to only those indices in a given namespace. Otherwise you
     *            can find indices in <code>kb2</code> if you provide the prefix
     *            <code>kb</code> and both kb and kb2 are namespaces since the
     *            indices spanned by <code>kb</code> would include both
     *            <code>kb.xyz</code> and <code>kb2.xyx</code>.
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

//        // @todo possible problem if [ts] is a read-write tx.
//        final IIndex name2Addr = journal.getName2Addr(ts);
//
//        if (name2Addr == null) {
//
//            // No commit points, no registered indices.
//            return new String[0];
//            
//        }

        final List<String> names = new LinkedList<String>();
        
        final Iterator<String> itr = journal.indexNameScan(namespace, ts);

        while (itr.hasNext()) {

            final String name = itr.next();

            names.add(name);
            
        }

        return names.toArray(new String[] {});

    }

}
