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
 * Created on Jan 19, 2008
 */

package com.bigdata.repo;

/**
 * Unit tests for random block writes.
 * 
 * @todo can you write any block regardless of whether or not it already exists?
 * 
 * @todo tests where a partial block is replaced, including by an empty block,
 *       another partial block, or a full block.
 * 
 * @todo The next append should add another block after that (partial) block.
 *       The caller should be able to re-write the (partial) block, changing its
 *       size to any size between [0:64k] bytes (including 0? delete the block
 *       if length is zero?) - that is you can do random access read/write by
 *       block.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAtomicRandomBlockWrite extends AbstractRepositoryTestCase {

    /**
     * 
     */
    public TestAtomicRandomBlockWrite() {
        // TODO Auto-generated constructor stub
    }

    /**
     * @param arg0
     */
    public TestAtomicRandomBlockWrite(String arg0) {
        super(arg0);
        // TODO Auto-generated constructor stub
    }

    public void test_something() {
        
        fail("write tests");
        
    }
    
}
