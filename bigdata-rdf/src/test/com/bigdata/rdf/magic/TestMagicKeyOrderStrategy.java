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
 * Created on Apr 28, 2009
 */

package com.bigdata.rdf.magic;

import junit.framework.TestCase;

import com.bigdata.rdf.magic.MagicKeyOrderStrategy.CharFormatter;

public class TestMagicKeyOrderStrategy extends TestCase {

    /**
     * 
     */
    public TestMagicKeyOrderStrategy() {
        super();
    }

    /**
     * @param name
     */
    public TestMagicKeyOrderStrategy(String name) {
        super(name);
    }

    public void testMagicKeyOrderStrategy() {

        for (int i = 1; i <= 5; i++) {
            System.out.println(i + ": " + MagicKeyOrderStrategy.calculateNumIndices(i));
        }
        
        for (int i = 1; i <= 8; i++) {
            int[][] indices = MagicKeyOrderStrategy.calculateKeyOrderArrays(i);
            System.out.println(i + ": " + MagicKeyOrderStrategy.format(indices, CharFormatter.INSTANCE) + ", " + "num indices = " + indices.length);
            boolean correct = checkIndices(indices, i);
            assertTrue(correct);
        }
        
    }
    
    public static boolean checkIndices(int[][] indices, int arity) {
        for (int i = 1; i <= arity; i++) {
            int[][] allCombos = MagicKeyOrderStrategy.allCombinations(arity, i);
            for (int j = 0; j < allCombos.length; j++) {
                boolean match = false;
                for (int k = 0; k < indices.length; k++) {
                    match |= MagicKeyOrderStrategy.match(allCombos[j], indices[k]);
                }
                if (match == false) {
                    return false;
                }
            }
        }
        return true;
    }
    
}
