package com.bigdata.service.ndx.pipeline;

import com.bigdata.service.AbstractFederation;

/**
 * Statistics for asynchronous index writes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexAsyncWriteStats<L, HS extends IndexPartitionWriteStats> extends
        AbstractRunnableMasterStats<L, HS> {

    public IndexAsyncWriteStats(final AbstractFederation<?> fed) {

        super(fed);

    }

}
