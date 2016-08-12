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
 * Created on Sep 11, 2010
 */

package com.bigdata.bop.fed;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IHaltOpMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.IQueryDecl;
import com.bigdata.bop.engine.IQueryPeer;
import com.bigdata.bop.engine.IStartOpMessage;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.striterator.Dechunkerator;

/**
 * Unit tests for {@link ThickChunkMessage}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
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

    private String namespace;
    private BigdataValueFactory valueFactory;
    private long nextId = 1;
    
    @Override
    protected void setUp() throws Exception {
        
        super.setUp();
        
        this.namespace = getName();
        
        this.valueFactory = BigdataValueFactoryImpl.getInstance(namespace);
        
    }
    
    @Override
    protected void tearDown() throws Exception {

        this.namespace = null;
        
        this.valueFactory.remove();
        
        super.tearDown();
        
    }

    /**
     * Create an {@link IV} for a {@link BigdataLiteral}, set the
     * {@link IVCache} association, and wrap it as an {@link IConstant}.
     * 
     * @param s
     *            The literal value.
     *            
     * @return The {@link IConstant}.
     */
    private IConstant<IV<?, ?>> makeLiteral(final String s) {

        final BigdataLiteral value = valueFactory.createLiteral(s);

        final TermId<BigdataLiteral> termId = new TermId<BigdataLiteral>(
                VTE.LITERAL, nextId++);

        termId.setValue(value);

        return new Constant<IV<?, ?>>(termId);
        
    }
    
    /**
     * Correct rejection test for an empty chunk (no solutions, not even an
     * empty solution).
     */
    public void test_emptyChunk() {

        final IQueryClient queryController = new MockQueryController();
        final UUID queryId = UUID.randomUUID();
        final int bopId = 1;
        final int partitionId = 2;
        
        final IBindingSet[] source = new IBindingSet[]{};

        // build the chunk.
        try {
            new ThickChunkMessage<IBindingSet>(queryController, queryId, bopId,
                    partitionId, source);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

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
        
        final IQueryClient queryController = new MockQueryController();
        final UUID queryId = UUID.randomUUID();
        final int bopId = 1;
        final int partitionId = 2;
        
        final IBindingSet[] source = data.toArray(new IBindingSet[0]);

        // build the chunk.
        final IChunkMessage<IBindingSet> msg1 = new ThickChunkMessage<IBindingSet>(
                queryController, queryId, bopId, partitionId, source);

        // same reference.
        assertTrue(queryController == msg1.getQueryController());

        // encode/decode.
        final IChunkMessage<IBindingSet> msg = (IChunkMessage<IBindingSet>) SerializerUtil
                .deserialize(SerializerUtil.serialize(msg1));

        // equals()
        assertEquals(queryController, msg.getQueryController());

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
    public void test_oneChunk() {

        final Var<?> x = Var.var("x");
        final Var<?> y = Var.var("y");

        final List<IBindingSet> data = new LinkedList<IBindingSet>();
        {
            IBindingSet bset = null;
            {
                bset = new ListBindingSet();
                bset.set(x, makeLiteral("John"));
                bset.set(y, makeLiteral("Mary"));
                data.add(bset);
            }
            {
                bset = new ListBindingSet();
                bset.set(x, makeLiteral("Mary"));
                bset.set(y, makeLiteral("Paul"));
                data.add(bset);
            }
            {
                bset = new ListBindingSet();
                bset.set(x, makeLiteral("Mary"));
                bset.set(y, makeLiteral("Jane"));
                data.add(bset);
            }
            {
                bset = new ListBindingSet();
                bset.set(x, makeLiteral("Paul"));
                bset.set(y, makeLiteral("Leon"));
                data.add(bset);
            }
            {
                bset = new ListBindingSet();
                bset.set(x, makeLiteral("Paul"));
                bset.set(y, makeLiteral("John"));
                data.add(bset);
            }
            {
                bset = new ListBindingSet();
                bset.set(x, makeLiteral("Leon"));
                bset.set(y, makeLiteral("Paul"));
                data.add(bset);
            }

        }
        
        final IQueryClient queryController = new MockQueryController();
        final UUID queryId = UUID.randomUUID();
        final int bopId = 1;
        final int partitionId = 2;
//        
        final IBindingSet[] source = data.toArray(new IBindingSet[0]);
        
        // build the chunk.
        final IChunkMessage<IBindingSet> msg1 = new ThickChunkMessage<IBindingSet>(
                queryController, queryId, bopId, partitionId, source);
        
        // same reference.
        assertTrue(queryController == msg1.getQueryController());

        // encode/decode.
        final IChunkMessage<IBindingSet> msg = (IChunkMessage<IBindingSet>) SerializerUtil
                .deserialize(SerializerUtil.serialize(msg1));

        // equals()
        assertEquals(queryController, msg.getQueryController());

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
     * <p>
     * Note: This needs to be serializable since we are sending the proxy with
     * the {@link IChunkMessage}.  There is an issue open to change that. See
     * {@link IChunkMessage#getQueryController()}.
     */
    private static class MockQueryController implements IQueryClient,
            Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        final private UUID serviceId = UUID.randomUUID();
        
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
            return serviceId;
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

        /**
         * Note: Assume equal as long as same class. This is used by the code
         * which tests the serialization of the {@link ThickChunkMessage}. In a
         * real federation, the query controller is a proxy. But also see
         * {@link IChunkMessage#getQueryController()}, which is an issue to
         * remove the {@link IQueryPeer} proxy from the {@link IChunkMessage}.
         */
        @Override
        public boolean equals(Object o) {
            final MockQueryController x = (MockQueryController) o;
            return true;
		}
		
    }
    
}
