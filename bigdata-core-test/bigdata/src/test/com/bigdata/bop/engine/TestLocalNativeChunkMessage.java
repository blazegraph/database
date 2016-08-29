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
 * Created on Feb 10, 2012
 */

package com.bigdata.bop.engine;

import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.bigdata.bop.Constant;
import com.bigdata.bop.DefaultQueryAttributes;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IQueryContext;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.rwstore.sector.MemoryManager;
import com.bigdata.striterator.Dechunkerator;

import junit.framework.TestCase2;

/**
 * Test suite for {@link LocalNativeChunkMessage}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestLocalNativeChunkMessage extends TestCase2 {

    /**
     * 
     */
    public TestLocalNativeChunkMessage() {
    }

    /**
     * @param name
     */
    public TestLocalNativeChunkMessage(String name) {
        super(name);
    }

    private MemoryManager mmgr;
    private IQueryContext queryContext;
    private IQueryClient queryController;
    private UUID queryId;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        queryController = new MockQueryController();
        queryId = UUID.randomUUID();
        mmgr = new MemoryManager(DirectBufferPool.INSTANCE, 1/* nsectors */);
        queryContext = new IQueryContext() {

            private final IQueryAttributes queryAttributes = new DefaultQueryAttributes();
            
            @Override
            public UUID getQueryId() {
                return queryId;
            }

            @Override
            public IMemoryManager getMemoryManager() {
                return mmgr;
            }

            @Override
            public IQueryAttributes getAttributes() {
                return queryAttributes;
            }
            
        };
    }

    @Override
    protected void tearDown() throws Exception {
        queryController = null;
        queryId = null;
        if (mmgr != null) {
            mmgr.close();
            mmgr = null;
        }
        queryContext = null;
        super.tearDown();
    }
    
    /**
     * Unit test for a message with a single chunk containing a single empty
     * binding set.
     */
    public void test_oneChunkWithEmptyBindingSet() {

        final List<IBindingSet> data = new LinkedList<IBindingSet>();
        {
            data.add(new ListBindingSet());
        }
        
        final int bopId = 1;
        final int partitionId = 2;
        
        final IBindingSet[] source = data.toArray(new IBindingSet[0]);
        
        // build the chunk.
        final IChunkMessage<IBindingSet> msg = new LocalNativeChunkMessage(
                queryController, queryId, bopId, partitionId, queryContext, source);

        assertTrue(queryController == msg.getQueryController());

        assertEquals(queryId, msg.getQueryId());
        
        assertEquals(bopId, msg.getBOpId());
        
        assertEquals(partitionId, msg.getPartitionId());

        // the data is inline with the message.
        assertTrue(msg.isMaterialized());

        // verify the iterator.
        assertSameIterator(data.toArray(new IBindingSet[0]),
                new Dechunkerator<IBindingSet>(msg.getChunkAccessor()
                        .iterator()));

    }

    /**
     * Unit test for a message with a single chunk of binding sets.
     */
    @SuppressWarnings("rawtypes")
    public void test_oneChunk() {

        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");

        final List<IBindingSet> data = new LinkedList<IBindingSet>();
        {
            IBindingSet bset = null;
            {
                bset = new ListBindingSet();
                bset.set(x, new Constant<IV>(new FullyInlineTypedLiteralIV("John")));
                bset.set(y, new Constant<IV>(new FullyInlineTypedLiteralIV("Mary")));
                data.add(bset);
            }
            {
                bset = new ListBindingSet();
                bset.set(x, new Constant<IV>(new FullyInlineTypedLiteralIV("Mary")));
                bset.set(y, new Constant<IV>(new FullyInlineTypedLiteralIV("Paul")));
                data.add(bset);
            }
            {
                bset = new ListBindingSet();
                bset.set(x, new Constant<IV>(new FullyInlineTypedLiteralIV("Mary")));
                bset.set(y, new Constant<IV>(new FullyInlineTypedLiteralIV("Jane")));
                data.add(bset);
            }
            {
                bset = new ListBindingSet();
                bset.set(x, new Constant<IV>(new FullyInlineTypedLiteralIV("Paul")));
                bset.set(y, new Constant<IV>(new FullyInlineTypedLiteralIV("Leon")));
                data.add(bset);
            }
            {
                bset = new ListBindingSet();
                bset.set(x, new Constant<IV>(new FullyInlineTypedLiteralIV("Paul")));
                bset.set(y, new Constant<IV>(new FullyInlineTypedLiteralIV("John")));
                data.add(bset);
            }
            {
                bset = new ListBindingSet();
                bset.set(x, new Constant<IV>(new FullyInlineTypedLiteralIV("Leon")));
                bset.set(y, new Constant<IV>(new FullyInlineTypedLiteralIV("Paul")));
                data.add(bset);
            }

        }
        
        final int bopId = 1;
        final int partitionId = 2;

        final IBindingSet[] source = data.toArray(new IBindingSet[0]);
        
        // build the chunk.
        final IChunkMessage<IBindingSet> msg = new LocalNativeChunkMessage(
                queryController, queryId, bopId, partitionId, queryContext, source);

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

        @Override
        public void haltOp(IHaltOpMessage msg) throws RemoteException {
        }

        @Override
        public void startOp(IStartOpMessage msg) throws RemoteException {
        }

        @Override
        public void bufferReady(IChunkMessage<IBindingSet> msg)
                throws RemoteException {
        }

        @Override
        public void declareQuery(IQueryDecl queryDecl) {
        }

        @Override
        public UUID getServiceUUID() throws RemoteException {
            return null;
        }

        @Override
        public PipelineOp getQuery(UUID queryId)
                throws RemoteException {
            return null;
        }

        @Override
        public void cancelQuery(UUID queryId, Throwable cause)
                throws RemoteException {
        }

        @Override
        public UUID[] getRunningQueries() {
            return null;
        }

    }

}
