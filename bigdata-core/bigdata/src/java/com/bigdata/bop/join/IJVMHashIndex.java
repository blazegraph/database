package com.bigdata.bop.join;
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
 * Created on Oct 15, 2015
 */
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.counters.CAT;

/**
 * Interface for JVM based hash indices, including static classes representing
 * keys, solution hits, and buckets (previously, these classes resided in
 * {@link JVMHashIndex} and were copied over as part of the pipelined hash join
 * implementation.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public interface IJVMHashIndex {
   
   /**
    * Add the solution to the index.
    * 
    * @param bset
    *           The {@link IBindingSet}.
    * 
    * @return The {@link Key} iff the solution was added to the index and
    *         <code>null</code> iff the solution was not added (because a
    *         {@link Key} could not be formed for the solution given the
    *         specified {@link #keyVars}).
    */
   public Key add(final IBindingSet bset);

   /**
    * Add the solution to the index iff the solution is not already present in
    * the index.
    * 
    * @param bset
    *           The solution.
    * 
    * @return <code>true</code> iff the index was modified by this operation.
    */
   public boolean addDistinct(final IBindingSet bset);

   /**
    * Return the hash {@link Bucket} into which the given solution is mapped.
    * <p>
    * Note: The caller must apply an appropriate join constraint in order to
    * correctly reject solutions that (a) violate the join contract; and (b)
    * that are present in the hash bucket due to a hash collection rather than
    * because they have the same bindings for the join variables.
    * 
    * @param left
    *           The probe.
    * 
    * @return The hash {@link Bucket} into which the given solution is mapped
    *         -or- <code>null</code> if there is no such hash bucket.
    */
   public Bucket getBucket(final IBindingSet left);

   /**
    * Visit all buckets in the hash index.
    */
   public Iterator<Bucket> buckets();

   /**
    * The #of buckets in the hash index. Each bucket has a distinct hash code.
    * Hash collisions can cause solutions that are distinct in their
    * {@link #keyVars} to nevertheless be mapped into the same hash bucket.
    * 
    * @return The #of buckets in the hash index.
    */
   public int bucketCount();

   /**
    * Export the {@link Bucket}s as an array.
    */
   public Bucket[] toArray();
   
   /**
    * Wrapper for the keys in the hash table. This is necessary for the hash
    * table to compare the keys as equal and also provides efficiencies in the
    * hash code and equals() methods.
    */
   public static class Key {

      protected final int hash;

      protected final IConstant<?>[] vals;

      protected Key(final int hashCode, final IConstant<?>[] vals) {
         this.vals = vals;
         this.hash = hashCode;
      }

      @Override
      public int hashCode() {
         return hash;
      }

      @Override
      public boolean equals(final Object o) {
         if (this == o)
            return true;
         if (!(o instanceof Key)) {
            return false;
         }
         final Key t = (Key) o;
         if (vals.length != t.vals.length)
            return false;
         for (int i = 0; i < vals.length; i++) {
            if (vals[i] == t.vals[i])
               continue;
            if (vals[i] == null)
               return false;
            if (!vals[i].equals(t.vals[i]))
               return false;
         }
         return true;
      }
   }

   /**
    * An solution and a hit counter as stored in the {@link JVMHashIndex}.
    */
   public static class SolutionHit {

      /**
       * The input solution.
       */
      final public IBindingSet solution;

      /**
       * The #of hits on that solution. This may be used to detect solutions
       * that did not join. E.g., by scanning and reporting out all solutions
       * where {@link #nhits} is ZERO (0L).
       */
      public final CAT nhits = new CAT();

      protected SolutionHit(final IBindingSet solution) {

         if (solution == null)
            throw new IllegalArgumentException();

         this.solution = solution;

      }

      @Override
      public String toString() {

         return getClass().getName() + "{nhits=" + nhits + ",solution="
               + solution + "}";

      }

   } // class SolutionHit
   
   /**
    * A group of solutions having the same as-bound values for the join vars.
    * Each solution is paired with a hit counter so we can support OPTIONAL
    * semantics for the join.
    */
   public static class Bucket implements Iterable<SolutionHit>,
         Comparable<Bucket> {

      /** The hash code for this collision bucket. */
      private final int hashCode;

      /**
       * A set of solutions (and their hit counters) which have the same
       * as-bound values for the join variables.
       */
      private final List<SolutionHit> solutions = new LinkedList<SolutionHit>();

      @Override
      public String toString() {
         return super.toString()
               + //
               "{hashCode=" + hashCode + ",#solutions=" + solutions.size()
               + "}";
      }

      protected Bucket(final int hashCode, final IBindingSet solution) {

         this.hashCode = hashCode;

         add(solution);

      }

      public void add(final IBindingSet solution) {

         if (solution == null)
            throw new IllegalArgumentException();

         solutions.add(new SolutionHit(solution));

      }

      /**
       * Add the solution to the bucket iff the solutions is not already present
       * in the bucket.
       * <p>
       * Note: There is already a hash index in place on the join variables when
       * we are doing a DISTINCT filter. Further, only the "join" variables are
       * "selected" and participate in a DISTINCT filter. Therefore, if we have
       * a hash collision such that two solutions would be directed into the
       * same {@link Bucket} then we can not improve matters but must simply
       * scan the solutions in the bucket to decide whether the new solution
       * duplicates a solution which is already present.
       * 
       * @param solution
       *           The solution.
       * 
       * @return <code>true</code> iff the bucket was modified by this
       *         operation.
       */
      public boolean addDistinct(final IBindingSet solution) {

         if (solutions.isEmpty()) {

            // First solution.
            solutions.add(new SolutionHit(solution));

            return true;

         }

         final Iterator<SolutionHit> itr = solutions.iterator();

         while (itr.hasNext()) {

            final SolutionHit aSolution = itr.next();

            if (aSolution.solution.equals(solution)) {

               // Solution already in this bucket.
               return false;

            }

         }

         // This is a distinct solution.
         solutions.add(new SolutionHit(solution));

         return true;

      }

      @Override
      final public Iterator<SolutionHit> iterator() {

         // return Collections.unmodifiableList(solutions).iterator();
         return solutions.iterator();

      }

      // @SuppressWarnings("unchecked")
      // public Iterator<IBindingSet> bindingSetIterator() {
      //
      // return new Striterator(solutions.iterator()).addFilter(new Resolver()
      // {
      //
      // @Override
      // protected Object resolve(Object obj) {
      // return ((SolutionHit)obj).solution;
      // }
      // });
      //
      // }

      /**
       * Orders the buckets based on their hash codes.
       */
      @Override
      final public int compareTo(final Bucket o) {
         if (hashCode > o.hashCode)
            return 1;
         if (hashCode < o.hashCode)
            return -1;
         return 0;
      }

      @Override
      final public int hashCode() {

         return hashCode;

      }

      /**
       * Return <code>true</code> iff this {@link Bucket} is empty (if there are
       * no solutions in the bucket).
       */
      final public boolean isEmpty() {

         return solutions.isEmpty();

      }

   } // Bucket

}
