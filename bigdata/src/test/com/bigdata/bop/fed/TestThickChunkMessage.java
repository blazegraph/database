/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 11, 2010
 */

package com.bigdata.bop.fed;

import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.Var;
import com.bigdata.bop.engine.HaltOpMessage;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.IQueryDecl;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.bop.engine.StartOpMessage;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.striterator.Dechunkerator;

/**
 * Unit tests for {@link ThickChunkMessage}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestThickChunkMessage extends TestCase2 {

    /**
     * 
     */
    public TestThickChunkMessage() {
    }

    /**
     * @param name
     */
    public TestThickChunkMessage(String name) {
        super(name);
    }

    /**
     * Unit test for a message with a single chunk of binding sets.
     */
    public void test_oneChunk() {

        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");

        final List<IBindingSet> data = new LinkedList<IBindingSet>();
        {
            IBindingSet bset = null;
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("John"));
                bset.set(y, new Constant<String>("Mary"));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Mary"));
                bset.set(y, new Constant<String>("Paul"));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Mary"));
                bset.set(y, new Constant<String>("Jane"));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Paul"));
                bset.set(y, new Constant<String>("Leon"));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Paul"));
                bset.set(y, new Constant<String>("John"));
                data.add(bset);
            }
            {
                bset = new HashBindingSet();
                bset.set(x, new Constant<String>("Leon"));
                bset.set(y, new Constant<String>("Paul"));
                data.add(bset);
            }

        }
        
        final IQueryClient queryController = new MockQueryController();
        final long queryId = 0L;
        final int bopId = 1;
        final int partitionId = 2;
        final IBlockingBuffer<IBindingSet[]> source = new BlockingBuffer<IBindingSet[]>(
                10);

        // populate the source.
        source.add(data.toArray(new IBindingSet[0]));
        
        // close the source.
        source.close();
        
        // build the chunk.
        final IChunkMessage<IBindingSet> msg = new ThickChunkMessage<IBindingSet>(
                queryController, queryId, bopId, partitionId, source);

        assertTrue(queryController == msg.getQueryController());

        assertEquals(queryId, msg.getQueryId());
        
        assertEquals(bopId, msg.getBOpId());
        
        assertEquals(partitionId, msg.getPartitionId());

        // the data is inline with the message.
        assertTrue(msg.isMaterialized());
        
        assertSameIterator(data.toArray(new IBindingSet[0]),
                new Dechunkerator<IBindingSet>(msg.getChunkAccessor()
                        .iterator()));

    }

    /**
     * Mock object.
     */
    private static class MockQueryController implements IQueryClient {

        public void haltOp(HaltOpMessage msg) throws RemoteException {
        }

        public void startOp(StartOpMessage msg) throws RemoteException {
        }

        public void bufferReady(IChunkMessage<IBindingSet> msg)
                throws RemoteException {
        }

        public void declareQuery(IQueryDecl queryDecl) {
        }

        public UUID getServiceUUID() throws RemoteException {
            return null;
        }

        public BindingSetPipelineOp getQuery(long queryId)
                throws RemoteException {
            return null;
        }
        
		@Override
		public RunningQuery eval(long queryId, BindingSetPipelineOp query)
				throws Exception, RemoteException {
			return null;
		}

    }
    
}
