/**
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
package cutthecrap.utils.striterators;

import junit.framework.TestCase2;

public class TestArrayIterator extends TestCase2 {

    public TestArrayIterator() {
    }

    public TestArrayIterator(String name) {
        super(name);
    }

    public void test_ctor() {

        try {
            new ArrayIterator<String>(null);
            fail();
        } catch (NullPointerException ex) {
            // ignore
        }
 
        try {
            new ArrayIterator<String>(null, 0, 0);
            fail();
        } catch (NullPointerException ex) {
            // ignore
        }

        try {
            new ArrayIterator<String>(new String[] {}, 0, 1);
            fail();
        } catch (IllegalArgumentException ex) {
            // ignore
        }

        try {
            new ArrayIterator<String>(new String[] {}, 0, -1);
            fail();
        } catch (IllegalArgumentException ex) {
            // ignore
        }

        try {
            new ArrayIterator<String>(new String[] {}, -1, 0);
            fail();
        } catch (IllegalArgumentException ex) {
            // ignore
        }

        new ArrayIterator<String>(new String[] {}, 0, 0);
        
        new ArrayIterator<String>(new String[] {"1"}, 0, 1);
        
        new ArrayIterator<String>(new String[] {"1"}, 1, 0);

        try {
            new ArrayIterator<String>(new String[] {"1"}, 1, 1);
            fail();
        } catch (IllegalArgumentException ex) {
            // ignore
        }
        
    }

    public void test_iterator() {

        assertSameIterator(new String[] {}, new ArrayIterator<String>(
                new String[] {}, 0, 0));

        assertSameIterator(new String[] {}, new ArrayIterator<String>(
                new String[] { "1" }, 1, 0));

        assertSameIterator(new String[] { "1" }, new ArrayIterator<String>(
                new String[] { "1" }, 0, 1));

        assertSameIterator(new String[] { "1" }, new ArrayIterator<String>(
                new String[] { "1", "2" }, 0, 1));

        assertSameIterator(new String[] { "1", "2" },
                new ArrayIterator<String>(new String[] { "1", "2" }, 0, 2));

        assertSameIterator(new String[] { "2" }, new ArrayIterator<String>(
                new String[] { "1", "2" }, 1, 1));

    }
    
}
