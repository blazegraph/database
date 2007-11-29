/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
 * Created on Sep 20, 2007
 */

package com.bigdata.service.mapred;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.journal.ITx;
import com.bigdata.rawstore.Bytes;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IDataService;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.RangeQueryIterator;

/**
 * A service for {@link IReduceTask} processing. Those tasks are distributed by
 * the {@link Master}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ReduceService
    extends AbstractJobAndTaskService<ReduceJobMetadata, AbstractReduceTask>
    implements IServiceShutdown, IReduceService
{

    public static final transient Logger log = Logger
            .getLogger(ReduceService.class);

    /**
     * Options for the {@link ReduceService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Options extends com.bigdata.service.mapred.AbstractJobAndTaskService.Options {
        
    }
    
    /**
     * @param properties
     *            See {@link Options}.
     */
    public ReduceService(Properties properties) {

        super( properties );
        
    }

    /**
     * Returns instances of {@link ReduceTaskWorker}.
     */
    protected AbstractTaskWorker<ReduceJobMetadata, AbstractReduceTask> 
        newTaskWorker(JobState<ReduceJobMetadata> jobState, AbstractReduceTask task) {
        
        return new ReduceTaskWorker(jobState, task);
        
    }

    /**
     * A worker for a reduce task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class ReduceTaskWorker extends AbstractTaskWorker<ReduceJobMetadata, AbstractReduceTask> {

        public ReduceTaskWorker(JobState<ReduceJobMetadata> jobState, AbstractReduceTask task) {

            super(jobState,task);

        }
        
        /**
         * Run the {@link IReduceTask}.
         * <p>
         * This does a key scan, invoking
         * {@link IReduceTask#reduce(byte[], Iterator)} for each distinct
         * application key. The keys in the index are comprised of an
         * application key, followed by the map task UUID and then an int32
         * tuple counter. Since the prefix is variable length, we have to count
         * backwards from the end of the key to extract the application key. Any
         * sequence of entries having the same application key will be processed
         * by a single invocation of
         * {@link IReduceTask#reduce(byte[], Iterator)}
         * 
         * @exception Exception
         *                if something goes wrong.
         */
        protected void run() throws Exception {

            log.info("Now running: job=" + jobState.getUUID() + ", task="
                    + task.getUUID());

            UUID dataService = task.getDataService();

            log.info("Reading from dataService="+dataService);
            
            assert jobState.client != null;
            
            IDataService ds = jobState.client.getDataService(dataService);

            if (ds == null)
                throw new RuntimeException("Could not locate dataService: "
                        + dataService);
            
            // the name of the index that we will read from.
            String name = task.getUUID().toString();

            // @todo make this a configuration parameter.
            final int capacity = 1000;

            // get both keys and values.
            final int flags = IDataService.KEYS | IDataService.VALS;

            // index scan.
            IEntryIterator itr = new RangeQueryIterator(ds, name, ITx.UNISOLATED,
                    null/* fromKey */, null/* toKey */, capacity, flags);

            long ntuples = 0L;
            
            // the last distinct key prefix.
            byte[] lastDistinctKey = null;
            
            // buffers all values for a distinct key prefix.
            List<byte[]> vals = new LinkedList<byte[]>();
            
            while(itr.hasNext()) {
                
                byte[] val = (byte[])itr.next();
                
                byte[] key = itr.getKey();
                
                int len = key.length;
                
                // last byte of the application key.
                int end = len - Bytes.SIZEOF_UUID - Bytes.SIZEOF_INT;
                
                if (lastDistinctKey == null
                        || BytesUtil.compareBytesWithLenAndOffset(0,
                                lastDistinctKey.length, lastDistinctKey, 0, end, key) != 0) {
                    
                    if(!vals.isEmpty()) {
                        
                        // emit to the reduce task.
                        task.reduce(lastDistinctKey, vals.iterator());
                        
                        vals.clear();
                        
                    }
                    
                    // copy the new distinct prefix.
                    lastDistinctKey = new byte[end];
                    
                    System.arraycopy(key, 0, lastDistinctKey, 0, end);
                    
                }

                // buffer values.
                vals.add(val);

                ntuples++;
                
            }
            
            log.info("Read " + ntuples + " tuples on dataService="
                    + dataService + " for reduceTask=" + task.getUUID());
            
        }
        
    }

    /**
     * A local (in process) {@link ReduceService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class EmbeddedReduceService extends ReduceService {
        
        final private UUID serviceUUID;
        
        final public IBigdataClient client;
        
        public EmbeddedReduceService(UUID serviceUUID, Properties properties, IBigdataClient client) {
            
            super(properties);
            
            if (serviceUUID == null)
                throw new IllegalArgumentException();
        
            if (client == null)
                throw new IllegalArgumentException();
            
            this.serviceUUID = serviceUUID;
            
            this.client = client;
            
        }

        public UUID getServiceUUID() throws IOException {

            return serviceUUID;
            
        }

        public IBigdataClient getBigdataClient() {

            return client;
            
        }

    }

}
