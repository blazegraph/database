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
 * Created on Jun 21, 2010
 */

package com.bigdata.rdf.load;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.TestCase2;

import com.bigdata.io.SerializerUtil;
import com.bigdata.service.ClientService;

/**
 * This is a test suite for the {@link ReentrantLock} deserialization pattern
 * used by the {@link MappedRDFFileLoadTask} when it executes on a remote
 * {@link ClientService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLockDeserialization extends TestCase2 {

    /**
     * 
     */
    public TestLockDeserialization() {
    }

    /**
     * @param name
     */
    public TestLockDeserialization(String name) {
        super(name);
    }

    /**
     * A {@link Serializable} class with a {@link ReentrantLock}, a
     * {@link Condition}s, and a boolean condition variable. Only the boolean
     * condition variable state is actually serialized. The rest of the state of
     * the class is restored when it is deserialized.
     */
    private static class C implements Serializable {

        /**
         * Some mock state.
         */
        private final int state;

        /**
         * The lock protecting the {@link #cond Condition} and the
         * {@link #condVar condition variable}.
         */
        private transient Lock lock;
        
        private transient Condition cond;
        
        /**
         * The condition variable.
         */
        private boolean condVar = false;
        
        public C(final int state) {
            this.state = state;
        }
        
        private void readObject(final ObjectInputStream in) throws IOException,
                ClassNotFoundException {
            log.info("Overriding readObject.");
            in.defaultReadObject();
            lock = new ReentrantLock();
            cond = lock.newCondition();
        }

    }

    public void test_serialization() {
        
        final C expected = new C(1);

        final C actual = (C) SerializerUtil.deserialize(SerializerUtil
                .serialize(expected));

        assertTrue(expected != actual);
        
        assertEquals("state", expected.state, actual.state);

        assertEquals("condVar", expected.condVar, actual.condVar);

        assertNotNull(actual.lock);

        assertNotNull(actual.cond);
        
        assertTrue(expected.lock != actual.lock);

    }
    
}
