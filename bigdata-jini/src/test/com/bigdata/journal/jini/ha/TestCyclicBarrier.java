package com.bigdata.journal.jini.ha;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

/**
 * This test demonstrates that {@link CyclicBarrier} does not adhere to its
 * documentation for {@link CyclicBarrier#await(long, TimeUnit)}. This means
 * that we can not use this variant in the release time consensus protocol since
 * we must also watch for service leaves, etc.
 * <p>
 * Note: We can still use the {@link CyclicBarrier#await()} as long as we
 * <em>interrupt</em> one of the threads that is blocked in
 * {@link CyclicBarrier#await()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestCyclicBarrier extends TestCase {

    public TestCyclicBarrier() {
        
    }
    
    public TestCyclicBarrier(String name) {
        super(name);
    }

    public void test_cyclicBarrier_awaitTimeout() throws InterruptedException,
            BrokenBarrierException, TimeoutException {

        final CyclicBarrier b = new CyclicBarrier(2);

        assertFalse(b.isBroken());

        try {
 
            b.await(1000, TimeUnit.MILLISECONDS);
            
            fail("Barrier should not be broken");
            
        } catch (TimeoutException ex) {

            // The barrier should not be broken.
            assertFalse("barrier broke with timeout.", b.isBroken());

        }

    }

}
