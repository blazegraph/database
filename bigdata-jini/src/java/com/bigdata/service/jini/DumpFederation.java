/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Sep 20, 2008
 */

package com.bigdata.service.jini;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.DumpJournal;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceAwareProcedure;
import com.bigdata.service.MetadataService;
import com.bigdata.util.InnerCause;

/**
 * A client utility that connects to and dumps various interesting aspects of a
 * live federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see DumpJournal
 * 
 * @todo dump by data service as well, showing the indices on the ds and the
 *       disk space allocated to the resource manager for the ds (depending on
 *       the release age there may be historical views preserved).
 */
public class DumpFederation {

    protected static final Logger log = Logger.getLogger(DumpFederation.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * Dumps interesting things about the federation.
     * <p>
     * <strong>Jini MUST be running</strong>
     * <p>
     * <strong>You MUST specify a sufficiently lax security policy</strong>,
     * e.g., using <code>-Djava.security.policy=policy.all</code>, where
     * <code>policy.all</code> is the name of a policy file.
     * 
     * @param args
     *            The name of the configuration file for the jini client that
     *            will be used to connect to the federation.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws IOException
     * @throws TimeoutException
     *             if no {@link DataService} can be discovered.
     */
    static public void main(String[] args) throws InterruptedException,
            ExecutionException, IOException, TimeoutException {

        if (args.length == 0) {

            System.err.println("usage: <client-config-file>");

            System.exit(1);
            
        }
        
        final JiniClient client;
        
        JiniServicesHelper helper = null;
        if (true) {// @todo command line option for this.
         
            helper = new JiniServicesHelper(new File(args[0]).getParent()
                    .toString()+File.separator);
            
            helper.start();
        
            client = helper.client;
            
        } else {

            client = JiniClient.newInstance(args);

        }

        final JiniFederation fed = client.connect();

        try {

            fed.awaitServices(1/* minDataServices */, 10000L/* timeout(ms) */);

            final long lastCommitTime = fed.getLastCommitTime();

            final long timestamp = TimestampUtility.asHistoricalRead(lastCommitTime);
            
            // the names of all registered scale-out indices.
            final String[] names = (String[]) fed.getMetadataService().submit(
                    new ListIndicesTask()).get();

            System.out.println("Found " + names.length + " indices: "
                    + Arrays.toString(names));

            final boolean dumpIndexLocators = true;

            if (dumpIndexLocators) {

                for (String name : names) {

                    // strip off the prefix to get the scale-out index name.
                    final String scaleOutIndexName = name.substring("metadata-"
                            .length());

                    final IMetadataIndex ndx;
                    try {
                    
                        ndx = fed.getMetadataIndex(scaleOutIndexName, timestamp);
                    
                    } catch (Throwable t) {
                        
                        final Throwable t2 = InnerCause.getInnerCause(t,
                                ClassNotFoundException.class);
                        
                        if (t2 != null) {
                        
                            log.error("CODEBASE/CLASSPATH problem:", t2);
                            
                            continue;
                        
                        }
                        
                        throw new RuntimeException(t);
                        
                    }

                    dumpIndexLocators(fed, scaleOutIndexName, ndx);

                }

            }

            System.out.println("Done.");

        } finally {

            if (helper != null) {

                helper.shutdown();
                
            } else {
                
                client.disconnect(true/* immediateShutdown */);
                
            }

        }
        
    }

    static Map<UUID, String> service2Hostname = new HashMap<UUID, String>();
    
    static synchronized String getServiceHostName(IBigdataFederation fed, UUID uuid) {

        String hostname = service2Hostname.get(uuid);

        if (hostname != null)
            return hostname;

        IDataService service = fed.getDataService(uuid);

        if (service == null) {

            return "<ServiceNotFound>";

        }
        
        try {

            hostname = service.getHostname();

        } catch (IOException ex) {

            return ex.getLocalizedMessage();

        }

        service2Hostname.put(uuid, hostname);

        return hostname;
        
    }
    
    /**
     * Dumps the locators for a scale-out index.
     * 
     * @param ndx
     *            The scale-out index.
     */
    static public void dumpIndexLocators(IBigdataFederation fed, String name,
            IMetadataIndex ndx) {

        System.out.println("\n\nname=" + name);

        final ITupleIterator<PartitionLocator> itr = ndx.rangeIterator();

        while (itr.hasNext()) {

            final PartitionLocator locator = itr.next().getObject();

            System.err.println("\t"+locator.toString());

            for (UUID uuid : locator.getDataServices()) {

                System.err.println("\t\tDataService: " + uuid + " on "
                        + getServiceHostName(fed, uuid));

            }

        }

    }

    /**
     * Task returns an ordered list of the named indices on the
     * {@link DataService} to which it is submitted. Note that
     * {@link MetadataService} extends {@link DataService} so this task can also
     * be used to enumerate the scale-out indices in an
     * {@link IBigdataFederation}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ListIndicesTask implements Callable<String[]>,
            IDataServiceAwareProcedure, Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = -831267313813825903L;

        transient protected static final Logger log = Logger.getLogger(DumpFederation.class);
        
        transient protected static final boolean INFO = log.isInfoEnabled();
        
        private transient DataService dataService;

        public ListIndicesTask() {
            
        }
        
        public void setDataService(DataService dataService) {
        
            this.dataService = dataService;
            
        }
        
        /**
         * @todo to be robust this should temporarily disable overflow for the
         *       data service otherwise it is possible that the live journal
         *       will be concurrently closed out.
         */
        public String[] call() throws Exception {
            
            if (dataService == null)
                throw new IllegalStateException("DataService not set.");
            
            final AbstractJournal journal = dataService.getResourceManager()
                    .getLiveJournal();
            
            final long lastCommitTime = journal.getLastCommitTime();
            
            final IIndex name2Addr = journal.getName2Addr(lastCommitTime);
            
            final int n = (int) name2Addr.rangeCount();
            
            if (INFO)
                log.info("Will read " + n + " index names from "
                        + dataService.getClass().getSimpleName());
            
            final Vector<String> names = new Vector<String>( n );
            
            final ITupleIterator itr = name2Addr.rangeIterator();
            
            while(itr.hasNext()) {
                
                final ITuple tuple = itr.next();
                
                final byte[] val = tuple.getValue();
                
                final Name2Addr.Entry entry = EntrySerializer.INSTANCE
                        .deserialize(new DataInputBuffer(val));
                
                if(INFO) {

                    log.info(entry.toString());
                    
                }
                
                names.add(entry.name);
                
            }
            
            return names.toArray(new String[] {});
            
        }

    }
    
}
