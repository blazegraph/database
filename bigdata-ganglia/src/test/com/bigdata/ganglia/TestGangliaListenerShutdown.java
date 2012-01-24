/*
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
/*
 * Created on Oct 14, 2006
 */

package com.bigdata.ganglia;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase;

/**
 * Unit test for shutdown of the {@link GangliaService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestGangliaListenerShutdown extends TestCase {

    /**
     * 
     */
    public TestGangliaListenerShutdown() {
    }

    /**
     * @param name
     */
    public TestGangliaListenerShutdown(String name) {
        super(name);
    }

    /**
     * The {@link GangliaListener} can block awaiting a datagram packet. If no
     * packet arrives, then it could hang there since
     * {@link DatagramSocket#receive(java.net.DatagramPacket)} does not notice
     * an interrupt. NIO for multicast is not available in JDK 6 (it was added
     * in JDK 7). This test verifies that an interrupt is noticed and that the
     * listener terminates in a timely manner.
     */
    public void test_gangliaListener_shutdown() throws UnknownHostException,
            InterruptedException {

        final IGangliaMessageHandler handler = new IGangliaMessageHandler() {
            
            @Override
            public void accept(IGangliaMessage msg) {
                // Ignore.
            }
        };
        
        final GangliaListener gangliaListener = new GangliaListener(
                InetAddress.getByName(IGangliaDefaults.DEFAULT_GROUP),//
                IGangliaDefaults.DEFAULT_PORT, //
                new GangliaMessageDecoder31(),//
                handler//
                );
        
        ExecutorService executorService = null;

        FutureTask<Void> ft = null;

        try {

            executorService = Executors.newSingleThreadExecutor();

            ft = new FutureTask<Void>(gangliaListener);

            // Run the listener.
            executorService.submit(ft);

            Thread.sleep(1000/* ms */);

            assertTrue(gangliaListener.isListening());

            ft.cancel(true/* mayInterruptIfRunning */);

            Thread.sleep(1000/* ms */);

            assertFalse(gangliaListener.isListening());
            
        } finally {

            /*
             * Stop host/application metric collection here.
             */
            if (executorService != null)
                executorService.shutdownNow();

        }

    }
    
}
