/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Oct 9, 2015
 */
package com.bigdata.bop.join;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;

/**
 * Hash index supporting pipelined hash joins. Going beyond the capabilities of
 * the normal {@link JVMHashIndex}, this index supports efficient removal of
 * entries from the hash index and is thread-safe.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class JVMPipelinedHashIndex implements IJVMHashIndex {

   private static final Logger log = Logger.getLogger(JVMPipelinedHashIndex.class);

   /**
    * Note: If joinVars is an empty array, then the solutions will all hash to
    * ONE (1).
    */
   private static final int ONE = 1;


   /**
    * The join variables (required, but may be empty). The order of the entries
    * is used when forming the as-bound keys for the hash table. Duplicate
    * elements and null elements are not permitted. If no join variables are
    * specified, then the join will consider the N x M cross product, filtering
    * for solutions which join. This is very expensive when compared to a hash
    * join. Whenever possible you should identify one or more variables which
    * must be bound for the join and specify those as the join variables.
    */
   private final IVariable<?>[] keyVars;

   /**
    * When <code>true</code>, we allow solutions to be stored in the hash index
    * that have unbound variables for the {@link #keyVars}. When
    * <code>false</code>, such solutions are dropped.
    * <p>
    * Note: This must be <code>true</code> for DISTINCT, OPTIONAL, and NOT
    * EXISTS / MINUS since in each case we do not want to drop solutions
    * lacking a binding for some {@link #keyVars}. For DISTINCT, this is
    * because we want to project all solutions, regardless of unbound
    * variables. For OPTIONAL and NOT EXISTS / MINUS, this is because we must
    * index all solutions since we will report only those solutions that do not
    * join. Once all solutions that do join have been identified, the solutions
    * that do not join are identified by a scan of the hash index looking for
    * {@link SolutionHit#nhits} equals ZERO (0L).
    */
   private final boolean indexSolutionsHavingUnboundJoinVars;

   /**
    * The backing map, which is thread safe.
    */
   private final Map<Key, Bucket> map;

   /**
    * @param keyVars
    *            The variables that are used to form the keys in the hash index
    *            (required, but may be empty). The order of the entries is used
    *            when forming the as-bound keys for the hash table. Duplicate
    *            elements and null elements are not permitted. If no join
    *            variables are specified, then the join will consider the N x M
    *            cross product, filtering for solutions which join. This is
    *            very expensive when compared to a hash join. Whenever possible
    *            you should identify one or more variables which must be bound
    *            for the join and specify those as the join variables.
    * @param indexSolutionsHavingUnboundJoinVars
    *            When <code>true</code>, we allow solutions to be stored in the
    *            hash index that have unbound variables for the
    *            {@link #keyVars}. When <code>false</code>, such solutions are
    *            dropped (they are not added to the index).
    * @param map
    *            The backing map. A {@link HashMap} should be faster for insert
    *            and search. A {@link LinkedHashMap} should be faster for
    *            scans. Some join patterns do not require us to use scans, in
    *            which case {@link HashMap} is the clear winner. (For example,
    *            a non-optional hash join against an access path never uses the
    *            iterator over the hash index.)
    */
   public JVMPipelinedHashIndex(final IVariable<?>[] keyVars,
           final boolean indexSolutionsHavingUnboundJoinVars,
           final Map<Key, Bucket> map) {

       if (keyVars == null) {
      
           /*
            * A ZERO LENGTH joinVars[] means that all solutions will be in the
            * same hash bucket. This can arise due to poor assignment of join
            * variables or simply because there are no available join variables
            * (full cross product join). Such joins are very expensive.
            */
           
           throw new IllegalArgumentException();

       }
       
       if (map == null) {
       
           throw new IllegalArgumentException();
           
       }

       this.map = Collections.synchronizedMap(map);

       this.indexSolutionsHavingUnboundJoinVars = indexSolutionsHavingUnboundJoinVars;

       this.keyVars = keyVars;

   }

   @Override
   public Key add(final IBindingSet bset) {

       final Key key = 
          JVMHashIndex.makeKey(keyVars, bset, indexSolutionsHavingUnboundJoinVars);

       if (key == null) {

           // Drop solution.
           return null;

       }

       synchronized (map) {
          
          Bucket b = map.get(key);
          if (b == null) {
   
              map.put(key, b = new Bucket(key.hash, bset));
   
          } else {
   
              b.add(bset);
   
          }
          
       }

       return key;

   }

   @Override
   public boolean addDistinct(final IBindingSet bset) {

       final Key key = 
          JVMHashIndex.makeKey(keyVars, bset, indexSolutionsHavingUnboundJoinVars);

       assert key != null;

       synchronized (map) {
          
          Bucket b = map.get(key);
   
          if (b == null) {
   
              // New bucket holding just this solution.
              map.put(key, b = new Bucket(key.hash, bset));
   
              return true;
   
          } else {
   
              if (b.addDistinct(bset)) {
   
                  // Existing bucket not having this solution.
                  return true;
   
              }
   
              // Existing bucket with duplicate solution.
              return false;
   
          }
       }

   }

   @Override
   public Bucket getBucket(final IBindingSet left) {

       final Key key = 
          JVMHashIndex.makeKey(keyVars, left, indexSolutionsHavingUnboundJoinVars);

       if (key == null) {

           return null;

       }

       // Probe the hash map : May return [null]!
       return map.get(key);

   }

   @Override
   public Iterator<Bucket> buckets() {

      synchronized (map) {
         
         return map.values().iterator();
         
      }

   }

   @Override
   public int bucketCount() {

       return map.size();

   }

   @Override
   public Bucket[] toArray() {

      synchronized (map) {
         
          // source.
          final Iterator<Bucket> bucketIterator = map.values()
                  .iterator();
   
          final Bucket[] a = new Bucket[map.size()];
   
          int i = 0;
   
          while (bucketIterator.hasNext()) {
   
              a[i++] = bucketIterator.next();
   
          }
   
          return a;
          
      }

   }
}
