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
 * Created on Nov 11, 2007
 */

package com.bigdata.rdf.spo;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase2;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTestCase;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Test suite for {@link SPOBlockingBuffer}.
 * 
 * @todo write a performance test comparing the rate at which we can read from
 *       an {@link SPOArrayIterator} with the rate at which we can write on an
 *       {@link SPOBlockingBuffer} and read from its
 *       {@link SPOBlockingBuffer#iterator()}
 * 
 * @todo consider modifying the {@link SPOBlockingBuffer#iterator()}
 *       implementation to obtain its lock in the ctor and release it in
 *       close().
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSPOBlockingBuffer extends TestCase2 {

    /**
     * 
     */
    public TestSPOBlockingBuffer() {
    }

    /**
     * @param name
     */
    public TestSPOBlockingBuffer(String name) {
        super(name);
    }

    /**
     * Correct rejection tests for the constructor.
     *
     */
    public void test_ctor() {

        // this is legal since the store is OPTIONAL.
        new SPOBlockingBuffer(null,ExplicitSPOFilter.INSTANCE,100);

        // this is legal since the filter is OPTIONAL.
        new SPOBlockingBuffer(null,null,100);

        // legal.
        new SPOBlockingBuffer(null,ExplicitSPOFilter.INSTANCE,100);

        // illegal - capacity must be positive.
        try {
            new SPOBlockingBuffer(null,ExplicitSPOFilter.INSTANCE,0);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            log.info("Ignoring expected exception: "+ex);
        }
        
        // illegal - capacity must be positive.
        try {
            new SPOBlockingBuffer(null,ExplicitSPOFilter.INSTANCE,-1);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            log.info("Ignoring expected exception: "+ex);
        }
        
    }

    /**
     * Test when nothing is written on the buffer and it is closed immediately.
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public void test_iterator_01() throws InterruptedException, ExecutionException {
        
        final int capacity = 2;
        
        // data to be written on the buffer and read from the iterator.
        final SPO[] expected = new SPO[]{
                
        };
        
        doIteratorTest(capacity, expected);

    }

    /**
     * Test when the buffer is filled exactly once.
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public void test_iterator_02() throws InterruptedException, ExecutionException {
        
        final int capacity = 2;
        
        // data to be written on the buffer and read from the iterator.
        final SPO[] expected = new SPO[]{

                new SPO(1,2,3,StatementEnum.Explicit),
                new SPO(2,2,3,StatementEnum.Explicit),
                
        };
        
        doIteratorTest(capacity, expected);

    }

    /**
     * Test when the buffer is filled multiple times.
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public void test_iterator_03() throws InterruptedException, ExecutionException {
        
        final int capacity = 2;
        
        // data to be written on the buffer and read from the iterator.
        final SPO[] expected = new SPO[]{

                new SPO(1,2,3,StatementEnum.Explicit),
                new SPO(2,2,3,StatementEnum.Explicit),
                
                new SPO(3,2,3,StatementEnum.Explicit),
                new SPO(4,2,3,StatementEnum.Explicit),

                new SPO(5,2,3,StatementEnum.Explicit),
        
        };
        
        doIteratorTest(capacity, expected);

    }

    /**
     * Test of reading from {@link SPOBlockingBuffer#iterator()}.
     * 
     * @param capacity
     *            The capacity of the buffer.
     * @param expected
     *            The {@link SPO}s to be written on the buffer and read from
     *            the iterator.
     * @throws ExecutionException
     * @throws InterruptedException
     * 
     * @todo do a variant that tests {@link ISPOIterator#nextChunk()}.
     * @todo verify chunk size == capacity if filter == null.
     * @todo verify when iterator uses a filter.
     */
    public void doIteratorTest(final int capacity, final SPO[] expected)
            throws InterruptedException, ExecutionException {
        
        // buffer of a known capacity.
        final SPOBlockingBuffer buffer = new SPOBlockingBuffer(null/* store */,
                null/* filter */, capacity);
        
        // service used to run the writer and the reader.
        ExecutorService service = Executors.newFixedThreadPool(2,
                DaemonThreadFactory.defaultThreadFactory());
        
        // the writer.
        Future f1 = service.submit(new Runnable() {

            public void run() {

                for(SPO spo : expected ) {
                    
                    buffer.add(spo);
                    
                }
            
                buffer.close();
                
            }
            
        });
        
        Future f2 = service.submit(new Runnable() {

            public void run() {

                // iterator that will read on that buffer.
                ISPOIterator actual = buffer.iterator();
                
                try {

                    AbstractTestCase.assertSameSPOs(expected, actual);
                    
                } finally {
                    
                    actual.close();
                    
                }
                
            }
            
        });

        f1.get();
        
        f2.get();
        
        service.shutdownNow();
        
    }
    
}
