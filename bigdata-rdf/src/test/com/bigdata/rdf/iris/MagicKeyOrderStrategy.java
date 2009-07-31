/**

Copyright (C) SYSTAP, LLC 2006-2009.  All rights reserved.

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

package com.bigdata.rdf.iris;

import java.math.BigInteger;
import java.util.Arrays;

import org.apache.log4j.Logger;

public class MagicKeyOrderStrategy {
    
    protected static final int NULL = -1;
    
    protected static final Logger log = 
        Logger.getLogger(MagicKeyOrderStrategy.class);
    
    public static String format(int[][] indices) {
        return format(indices, null);
    }
     
    public static String format(int[][] indices, Formatter formatter) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        for (int i = 0; i < indices.length; i++) {
            sb.append(format(indices[i], formatter));
            sb.append(",\n");
        }
        if (sb.length() > 1) {
            sb.setLength(sb.length()-2);
        }
        sb.append("\n}");
        return sb.toString();
    }
    
    public static String format(int[] index) {
        return format(index, null);
    }
    
    public static String format(int[] index, Formatter formatter) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < index.length; i++) {
            sb.append(formatter != null ? formatter.format(index[i]) : index[i]);
        }
        return sb.toString();
    }
    
    public static interface Formatter {
        String format(int i);
    }
    
    public static class CharFormatter implements Formatter {

        public static final CharFormatter INSTANCE = new CharFormatter();
        
        public static final char[] alphabet = new char[] {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 
            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' 
        };
        
        public String format(int i) {
            if (i < 0) return ".";
            if (i > alphabet.length) return "?";
            return String.valueOf(alphabet[i]);
        }
        
    }
    
    public static int calculateNumIndices(int arity) {
        
        final int half = arity / 2;
        
        return countCombinations(arity, half);
        
    }
    
    public static int countCombinations(int x, int y) {
        
        final int z = x - y;
        
        long numerator = 1;
        for (int i = z+1; i <= x; i++) {
            numerator *= i;           
        }
        
        long denominator = 1;
        for (int i = 1; i <= y; i++) {
            denominator *= i;           
        }
        
        return (int) (numerator / denominator);
        
    }
    
    public static int[][] allCombinations(int x, int y) {

        int[][] combos = new int[countCombinations(x, y)][];
        CombinationGenerator gen = new CombinationGenerator(x, y);
        for (int i = 0; i < combos.length; i++) {
            combos[i] = Arrays.copyOf(gen.getNext(), y);
        }
        return combos;

    }
    
    public static MagicKeyOrder[] calculateKeyOrders(int arity) {
        
        int[][] arrays = calculateKeyOrderArrays(arity);
        MagicKeyOrder[] keyOrders = new MagicKeyOrder[arrays.length];
        for (int i = 0; i < arrays.length; i++) {
            StringBuilder indexName = new StringBuilder();
            for (int j = 0; j < arity; j++) {
                indexName.append(arrays[i][j]);
            }
            keyOrders[i] = new MagicKeyOrder(indexName.toString(), arrays[i]);
        }
        
        return keyOrders;
        
    }
    
    public static int[][] calculateKeyOrderArrays(int arity) {
        
        final int[][] indices = new int[calculateNumIndices(arity)][]; 
                    
        for (int i = 0; i < indices.length; i++) {
            indices[i] = new int[arity];
            for (int j = 0; j < arity; j++) {
                indices[i][j] = NULL;
            }
        }

        //System.out.println("generating initial scramble...");
        for (int i = 0; i < arity; i++) {
            for (int j = 0; j < arity; j++) {
                indices[i][j] = (i+j)%arity;
            }
        }
        //System.out.println("done.");
        
        for (int i = 2; i < arity-1; i++) {
            //System.out.println("calculating all combinations of length " + i + "...");
            int[][] combos = allCombinations(arity, i);
           // System.out.println("done.");
            for (int j = 0; j < combos.length; j++) {
                //System.out.println("checking match for combo # " + j + "...");
                boolean match = false;
                for (int k = 0; k < indices.length; k++) {
                    match |= match(combos[j], indices[k]);
                }
                if (match == false) {
                    ///System.out.println("fitting a match for combo # " + j + "...");
                    for (int k = 0; k < indices.length; k++) {
                        if (makeMatch(combos[j], indices[k])) {
                            break;
                        }
                    }
                }
                //System.out.println("done with combo #" + j + ".");
            }
        }
        
        for (int i = 0; i < indices.length; i++) {
            //System.out.println("filling in the gaps for index # " + i + "...");
            for (int j = 0; j < arity; j++) {
                for (int k = 0; k < arity; k++) {
                    if (indices[i][k] == NULL) {
                        indices[i][k] = j;
                        break;
                    } else if (indices[i][k] == j) {
                        break;
                    }
                }
            }
            //System.out.println("done.");
        }
        
        return indices;
        
    }
    
    public static boolean match(int[] small, int[] big) {
        if (small.length > big.length) {
            throw new IllegalArgumentException();
        }
        boolean matchAll = true;
        for (int i = 0; i < small.length; i++) {
            boolean matchOne = false;
            for (int j = 0; j < small.length; j++) {
                matchOne |= small[i] == big[j];
                if (matchOne) {
                    break;
                }
            }
            matchAll &= matchOne;
        }
        return matchAll;
    }
    
    public static boolean makeMatch(int[] small, int[] big) {
        if (small.length > big.length) {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < big.length; i++) {
            if (big[i] == NULL) {
                continue;
            }
            boolean match = false;
            for (int j = 0; j < small.length; j++) {
                match |= big[i] == small[j];
            }
            if (match == false) {
                return false;
            }
        }
        for (int i = 0; i < small.length; i++) {
            boolean match = false;
            for (int j = 0; j < big.length; j++) {
                match |= small[i] == big[j];
            }
            if (match == false) {
                for (int j = 0; j < big.length; j++) {
                    if (big[j] == NULL) {
                        big[j] = small[i];
                        break;
                    }
                }
            }
        }
        return true;
    }
    
    public static class CombinationGenerator {
        private int[] a;

        private int n;

        private int r;

        private BigInteger numLeft;

        private BigInteger total;

        //------------
        // Constructor
        //------------
        public CombinationGenerator(int n, int r) {
            if (r > n) {
                throw new IllegalArgumentException();
            }
            if (n < 1) {
                throw new IllegalArgumentException();
            }
            this.n = n;
            this.r = r;
            a = new int[r];
            BigInteger nFact = getFactorial(n);
            BigInteger rFact = getFactorial(r);
            BigInteger nminusrFact = getFactorial(n - r);
            total = nFact.divide(rFact.multiply(nminusrFact));
            reset();
        }

        //------
        // Reset
        //------
        public void reset() {
            for (int i = 0; i < a.length; i++) {
                a[i] = i;
            }
            numLeft = new BigInteger(total.toString());
        }

        //------------------------------------------------
        // Return number of combinations not yet generated
        //------------------------------------------------
        public BigInteger getNumLeft() {
            return numLeft;
        }

        //-----------------------------
        // Are there more combinations?
        //-----------------------------
        public boolean hasMore() {
            return numLeft.compareTo(BigInteger.ZERO) == 1;
        }

        //------------------------------------
        // Return total number of combinations
        //------------------------------------
        public BigInteger getTotal() {
            return total;
        }

        //------------------
        // Compute factorial
        //------------------
        private static BigInteger getFactorial(int n) {
            BigInteger fact = BigInteger.ONE;
            for (int i = n; i > 1; i--) {
                fact = fact.multiply(new BigInteger(Integer.toString(i)));
            }
            return fact;
        }

        //--------------------------------------------------------
        // Generate next combination (algorithm from Rosen p. 286)
        //--------------------------------------------------------
        public int[] getNext() {
            if (numLeft.equals(total)) {
                numLeft = numLeft.subtract(BigInteger.ONE);
                return a;
            }
            int i = r - 1;
            while (a[i] == n - r + i) {
                i--;
            }
            a[i] = a[i] + 1;
            for (int j = i + 1; j < r; j++) {
                a[j] = a[i] + j - i;
            }
            numLeft = numLeft.subtract(BigInteger.ONE);
            return a;
        }
    }
    
}
