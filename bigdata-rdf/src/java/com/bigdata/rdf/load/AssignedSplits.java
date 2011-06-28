package com.bigdata.rdf.load;

import java.util.UUID;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.service.IBigdataFederation;

/**
 * Class permits specification of a pre-assigned index partition splits onto
 * data services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AssignedSplits {

    /**
     * The array of separator keys. Each separator key is interpreted as an
     * <em>unsigned byte[]</em>. The first entry MUST be an empty byte[]. The
     * entries MUST be in sorted order.
     */
    final byte[][] separatorKeys;

    /**
     * The array of data services onto which each partition defined by a
     * separator key will be mapped (optional). When given, the #of entries in
     * this array MUST agree with the #of entries in the <i>separatorKeys</i>
     * array and all entries must be non-<code>null</code>. When not given,
     * the index partitions will be auto-assigned to the discovered data
     * services.
     */
    final UUID[] dataServiceUUIDs;

    /**
     * Register the index against the federation using the index partitions and
     * data services described by this instance. if the {@link IIndexManager} is
     * NOT an {@link IBigdataFederation} then the indices will be registered
     * normally using {@link IIndexManager#registerIndex(IndexMetadata)}.
     * 
     * @param indexManager
     *            The {@link IIndexManager} and ideally an
     *            {@link IBigdataFederation}.
     * @param indexMetadata
     *            The metadata describing the index to be registered.
     */
    public void registerIndex(final IIndexManager indexManager,
            final IndexMetadata indexMetadata) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        if (indexMetadata == null)
            throw new IllegalArgumentException();

        if (indexManager instanceof IBigdataFederation) {

            ((IBigdataFederation) indexManager).registerIndex(indexMetadata,
                    separatorKeys, dataServiceUUIDs);

        } else {

            indexManager.registerIndex(indexMetadata);

        }

    }

    /**
     * @param separatorKeys
     *            The array of separator keys. Each separator key is interpreted
     *            as an <em>unsigned byte[]</em>. The first entry MUST be an
     *            empty byte[]. The entries MUST be in sorted order.
     * @param dataServiceUUIDs
     *            The array of data services onto which each partition defined
     *            by a separator key will be mapped (optional). When given, the
     *            #of entries in this array MUST agree with the #of entries in
     *            the <i>separatorKeys</i> array and all entries must be non-<code>null</code>.
     *            When not given, the index partitions will be auto-assigned to
     *            the discovered data services.
     */
    public AssignedSplits(final byte[][] separatorKeys,
            final UUID[] dataServiceUUIDs) {

        if (separatorKeys == null)
            throw new IllegalArgumentException();

//        if (dataServiceUUIDs == null)
//            throw new IllegalArgumentException();

        if (dataServiceUUIDs != null
                && separatorKeys.length != dataServiceUUIDs.length)
            throw new IllegalArgumentException();

        if (separatorKeys.length == 0)
            throw new IllegalArgumentException();

        for (int i = 0; i < separatorKeys.length; i++) {

            if (separatorKeys[i] == null)
                throw new IllegalArgumentException();

            if (dataServiceUUIDs != null && dataServiceUUIDs[i] == null)
                throw new IllegalArgumentException();

            if(i == 0 && separatorKeys[i].length != 0)
                throw new IllegalArgumentException();
            
            if (i > 0
                    && BytesUtil.compareBytes(separatorKeys[i - 1],
                            separatorKeys[i]) >= 0) {
            
                throw new IllegalArgumentException();
                
            }
            
        }

        this.separatorKeys = separatorKeys;

        this.dataServiceUUIDs = dataServiceUUIDs;

    }

}