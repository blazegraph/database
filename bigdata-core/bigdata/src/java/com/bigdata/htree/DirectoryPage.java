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
package com.bigdata.htree;

import java.io.PrintStream;
import java.lang.ref.Reference;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.Node;
import com.bigdata.htree.AbstractHTree.ChildMemoizer;
import com.bigdata.htree.AbstractHTree.LoadChildRequest;
import com.bigdata.htree.data.IDirectoryData;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.BytesUtil;
import com.bigdata.util.concurrent.Memoizer;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * An {@link HTree} directory page (node). Each directory page will hold one or
 * more "buddy" hash tables. The #of buddy hash tables on the page depends on
 * the globalDepth of the page and the addressBits as computed by
 * {@link DirectoryPage#getNumBuddies()}.
 */
class DirectoryPage extends AbstractPage implements IDirectoryData {
    
    /**
     * The depth of a bucket page which overflows is always <i>addressBits</i>.
     */
    final int getOverflowPageDepth() {
     
        return htree.addressBits;
        
    }
	
	/**
	 * Transient references to the children.
	 */
	// Note: cleared by copyOnWrite when we steal the array.
	/*final*/ Reference<AbstractPage>[] childRefs;

	/**
	 * Persistent data.
	 */
	IDirectoryData data;

	/**
	 * Get the child indexed by the key.
	 * <p>
	 * Note: The recursive descent pattern requires the caller to separately
	 * compute the buddy index before each descent into a child.
	 * 
	 * @param hashBits
	 *            The relevant bits from the key.
	 * @param buddyOffset
	 *            The offset into the child of the first slot for the buddy hash
	 *            table or buddy hash bucket.
	 * 
	 * @return The child indexed by the key.
	 * 
	 * @see HTreeUtil#getBuddyOffset(int, int, int)
	 */
	protected AbstractPage getChild(final int hashBits, final int buddyOffset) {

		// width of a buddy hash table in pointer slots.
		final int tableWidth = (1 << globalDepth);

		// index position of the start of the buddy hash table in the page.
		final int tableOffset = (tableWidth * buddyOffset);

		// index of the slot in the buddy hash table for the given hash
		// bits.
		final int index = tableOffset + hashBits;

		return getChild(index);

	}

	/**
	 * Split the child {@link BucketPage}.
	 * <p>
	 * Rather than immediately creating 2 buckets, we want first to just
	 * fill half the original slots (by testing the first key value) and
	 * leaving the other half as null.
	 * <p>
	 * This will result in a lazy creation of the second BucketPage
	 * if required.  Such that we could end up introducing several
	 * new layers without the needless creation of empty BucketPages.
	 */
	void _splitBucketPage(final BucketPage bucketPage) {

        // Note: this.getChildCount() is less direct.
		final byte[] tstkey = bucketPage.getFirstKey();
		final int bucketSlot = getLocalHashCode(tstkey, getPrefixLength());
				
		final int slotsOnPage = 1 << htree.addressBits;
		int start = 0;
		for (int s = 0; s < slotsOnPage; s++) {
			if (bucketPage.self == childRefs[s]) {
				start = s;
				break;
			}
		}
		int last = start;
		for (int s = start + 1; s < slotsOnPage; s++) {
			if (bucketPage.self == childRefs[s]) {
				last++;
			} else {
				break;
			}
		}
		final int npointers = last - start + 1;

		assert npointers > 1;
		assert npointers % 2 == 0;
		assert npointers == 1 << (htree.addressBits - bucketPage.globalDepth);

		final int crefs = npointers >> 1; // half references for each new child
		final int newDepth = bucketPage.globalDepth + 1;
		final boolean fillLowerSlots = bucketSlot < start + crefs;
		assert bucketSlot >= start && bucketSlot <= last;
		
		final BucketPage newPage = createPage(newDepth);
		
		final Reference<AbstractPage> a = (Reference<AbstractPage>) (fillLowerSlots ? newPage.self : null);
		for (int s = start; s < start + crefs; s++) {
			childRefs[s] = (Reference<AbstractPage>) a;
		}
		
		final Reference<AbstractPage> b = (Reference<AbstractPage>) (!fillLowerSlots ? newPage.self : null);
		for (int s = start + crefs; s <= last; s++) {
			childRefs[s] = (Reference<AbstractPage>) b;
		}
		
		assert !newPage.isPersistent();
		((HTree) htree).nleaves++; // only definitely add one


        // remove original page and correct leaf count
        bucketPage.delete();
		((HTree) htree).nleaves--;

		// insert raw values from original page
		final int bucketSlotsPerPage = bucketPage.slotsOnPage();
        for (int i = 0; i < bucketSlotsPerPage; i++) {
            ((HTree) htree).insertRawTuple(bucketPage, i);
        }
        
        assert ((BucketPage) childRefs[bucketSlot].get()).data.getKeyCount() > 0;
        
        // ...and finally delete old page data
        if (bucketPage.isPersistent()) {
            htree.deleteNodeOrLeaf(bucketPage.getIdentity());
        }
        
	}

	private BucketPage createPage(final int depth) {
		final BucketPage ret =  new BucketPage((HTree) htree, depth);
		ret.parent = (Reference<DirectoryPage>) self;
		
		return ret;
	}
	
	/**
	 * This is a lazy creation of a BucketPage to fill the relevant number of slots
	 * based on the number of empty slots surrounding it.
	 * 
	 * If all slots are empty, then they will all be filled with references to the
	 * new page.
	 *
	 * The regions tested follow the pattern below:
	 * [A A A A A A A A]
	 * [A A A A B B B B]
	 * [A A B B C C D D]
	 * [A B C D E F G H]
	 *
	 * Examples with 8 slots, test slot 'T'
	 * [- - - - T - - -] // all empty
	 * [S S S S S S S S] // set ALL 8
	 * 
	 * [F F - - T - - -] // other half not empty
	 * [F F - - S S S S] // set 4 slots
	 * 
	 * [F F - T - - - -] // same half not empty
	 * [F F S S - - - -] // set 2 slots
	 * 
	 * @param slot - the target slot to be filled
	 * @param bucketPage - the page to replicate
	 */
	private void fillEmptySlots(final int slot, final BucketPage bucketPage) {
		// create with initial max depth
		int slots = fillEmptySlots(slot, bucketPage, 0, 1 << htree.addressBits); 
		
		assert bucketPage.globalDepth == htree.addressBits;
		
		bucketPage.globalDepth = htree.addressBits;
		while (slots > 1) {
			slots >>= 1;
			bucketPage.globalDepth--;
		}
		
		// assert (1 << (htree.addressBits - bucketPage.globalDepth)) == countChildRefs(bucketPage);
	}
	
	int countChildRefs(AbstractPage pge) {
		int count = 0;
		for (int i = 0; i < childRefs.length; i++) {
			if (childRefs[i] == pge.self) {
				count++;
			}
		}
		return count;
	}

	private int fillEmptySlots(final int slot, final BucketPage bucketPage, final int from, final int to) {
		if (from > slot | to < slot) {
			throw new IllegalArgumentException();
		}
		
		assert !bucketPage.isPersistent();
		
		if (from == slot && to == slot+1) {
			childRefs[slot] = (Reference<AbstractPage>) bucketPage.self;
			
			return 1;
		}
		
		// test for full range null
		for (int i = from; i < to; i++) {
			if (childRefs[i] != null || getChildAddr(i) != IRawStore.NULL) {
				// recurse and return
				final int childlen = (to-from) >> 1; // half
				if (slot < (from + childlen)) {
					return fillEmptySlots(slot, bucketPage, from, from+childlen);
				} else {
					return fillEmptySlots(slot, bucketPage, to-childlen, to);
				}
			}
		}
		
		// assign to validated null range
		for (int i = from; i < to; i++) {
			assert childRefs[i] == null;
			
			childRefs[i] = (Reference<AbstractPage>) bucketPage.self;
		}
		
		return to - from;
	}
	
	/**
	 * Return the {@link Reference} for the child at that index.
	 * 
	 * @param index
	 *            The index
	 * 
	 * @return The {@link Reference}.
	 */
	Reference<AbstractPage> getChildRef(final int index) {

		return childRefs[index];

	}

	/**
	 * This method must be invoked on a parent to notify the parent that the
	 * child has become persistent. The method scans the weak references for the
	 * children, finds the index for the specified child, and then sets the
	 * corresponding index in the array of child addresses.
	 * 
	 * @param child
	 *            The child.
	 * 
	 * @exception IllegalStateException
	 *                if the child is not persistent.
	 * @exception IllegalArgumentException
	 *                if the child is not a child of this node.
	 */
	void setChildAddr(final AbstractPage child) {

		assert !isReadOnly();

		if (!child.isPersistent()) {

			// The child does not have persistent identity.
			throw new IllegalStateException();

		}

		final int slotsPerPage = 1 << htree.addressBits;
		
		if (childRefs == null)
			throw new IllegalStateException("childRefs must not be NULL");

		boolean found = false;
		for (int i = 0; i < slotsPerPage; i++) {

			if (childRefs[i] == child.self) {

				((MutableDirectoryPageData) data).childAddr[i] = child
						.getIdentity();

                found = true;

            } else if (found) {

                // no more pointers to that child.
                break;
                
            }

		}

	}

	/**
	 * Return the child at the specified index in the {@link DirectoryPage}.
	 * 
	 * @param index
	 *            The index of the slot in the {@link DirectoryPage}. If the
	 *            child must be materialized, the buddyOffset will be computed
	 *            based on the globalDepth and the #of pointers to that child
	 *            will be computed so its depth may be set.
	 * 
	 * @return The child at that index.
	 */
	AbstractPage getChild(final int index) {
		
        // See BLZG-1657 (Add BTreeCounters for cache hit and cache miss)
        htree.getBtreeCounters().cacheTests.increment();
        
		/**
		 * FIXME: Need to check whether we are using unnecessary synchronization
		 */
		AbstractPage ret = checkLazyChild(index);
		
		if (ret != null) {
			return ret;
		}

        if (htree.memo == null) {

            /*
             * Optimization for the mutable B+Tree.
             * 
             * Note: This optimization depends on the assumption that concurrent
             * operations are never submitted to the mutable B+Tree. In fact,
             * the UnisolatedReadWriteIndex *DOES* allow concurrent readers (it
             * uses a ReentrantReadWriteLock). Therefore this code path is now
             * expressed conditionally on whether or not the Memoizer object is
             * initialized by AbstractBTree.
             * 
             * Note: Since the caller is single-threaded for the mutable B+Tree
             * we do not need to use the Memoizer, which just delegates to
             * _getChild(index). This saves us some object creation and overhead
             * for this case.
             */

            // See BLZG-1657 (Add BTreeCounters for cache hit and cache miss)
            htree.getBtreeCounters().cacheMisses.increment();
            
            return _getChild(index, null/* req */);
            
        }

        /*
         * If we can resolve a hard reference to the child then we do not need
         * to look any further.
         */
//        synchronized (childRefs) 
        {

            /*
             * Note: we need to synchronize on here to ensure visibility for
             * childRefs[index] (in case it was updated in another thread). This
             * is true even for the mutable B+Tree since the caller could use
             * different threads for different operations. However, this
             * synchronization will never be contended for the mutable B+Tree.
             */
      	
            final Reference<AbstractPage> childRef = childRefs[index];

            final AbstractPage child = childRef == null ? null : childRef.get();

            if (child != null) {

                // Already materialized.
        		htree.touch(child);
        		
                return child;

            }

        }

        /*
         * Otherwise we need to go through the Memoizer pattern to achieve
         * non-blocking access. It will wind up delegating to _getChild(int),
         * which is immediately below. However, it will ensure that one and only
         * one thread executes _getChild(int) for a given parent and child
         * index. That thread will update childRefs[index]. Any concurrent
         * requests for the same child will wait for the FutureTask inside of
         * the Memoizer and then return the new value of childRefs[index].
         */

        /*
         * See BLZG-1657 (Add BTreeCounters for cache hit and cache miss)
         * 
         * Note: This is done in the caller rather than _getChild() since the
         * latter may be called from the memoizer, in which case only one thread
         * will actually invoke _getChild() while the others will just obtain
         * the child through the memoized Future.
         */
        htree.getBtreeCounters().cacheMisses.increment();
        
        return htree.loadChild(this, index);

    }

    private AbstractPage checkLazyChild(final int index) {

      final AbstractPage child;
      synchronized (childRefs) {

         /*
          * Note: we need to synchronize on here to ensure visibility for
          * childRefs[index] (in case it was updated in another thread).
          */
         final Reference<AbstractPage> childRef = childRefs[index];

         child = childRef == null ? null : childRef.get();

         if (child != null) {

            // Already materialized.
            htree.touch(child);

            return child;

         }

      }

      // Check for lazy BucketPage creation
      if (getChildAddr(index) == IRawStore.NULL) {

         final DirectoryPage copy = (DirectoryPage) copyOnWrite(IRawStore.NULL);

         return copy.setLazyChild(index);
         
      }

      return null;

    }
    
    private BucketPage setLazyChild(final int index) {

       final DirectoryPage copy = (DirectoryPage) copyOnWrite(IRawStore.NULL);
    	
       assert copy == this;

     	assert childRefs[index] == null;
    	
    	final BucketPage lazyChild = new BucketPage((HTree) htree, htree.addressBits);
    	lazyChild.parent = (Reference<DirectoryPage>) self;
    	((HTree) htree).nleaves++;
    	
    	fillEmptySlots(index, lazyChild);
    	
    	htree.touch(lazyChild);
    	
    	return lazyChild;
    	
    }

	/**
     * Method conditionally reads the child at the specified index from the
     * backing store and sets its reference on the appropriate element of
     * {@link #childRefs}. This method assumes that external mechanisms
     * guarantee that no other thread is requesting the same child via this
     * method at the same time. For the mutable B+Tree, that guarantee is
     * trivially given by its single-threaded constraint. For the read-only
     * B+Tree, {@link AbstractHTree#loadChild(DirectoryPage, int)} provides this
     * guarantee using a {@link Memoizer} pattern. This method explicitly
     * handshakes with the {@link ChildMemoizer} to clear the {@link FutureTask}
     * from the memoizer's internal cache as soon as the reference to the child
     * has been set on the appropriate element of {@link #childRefs}.
     * 
     * @param index
     *            The index of the child.
     * @param req
     *            The key we need to remove the request from the
     *            {@link ChildMemoizer} cache (and <code>null</code> if this
     *            method is not invoked by the memoizer pattern).
     * 
     * @return The child and never <code>null</code>.
     */
    AbstractPage _getChild(final int index, final LoadChildRequest req) {

		/*
		 * Make sure that the child is not reachable. It could have been
		 * concurrently set even if the caller had tested this and we do not
		 * want to read through to the backing store unless we need to.
		 * 
		 * Note: synchronizing on childRefs[] should not be necessary. For a
		 * read-only B+Tree, the synchronization is provided by the Memoizer
		 * pattern. For a mutable B+Tree, the synchronization is provided by the
		 * single-threaded contract for mutation and by the requirement to use a
		 * construct, such as a Queue or the UnisolatedReadWriteIndex, which
		 * imposes a memory barrier when passing a B+Tree instance between
		 * threads.
		 * 
		 * See http://www.cs.umd.edu/~pugh/java/memoryModel/archive/1096.html
		 */
        AbstractPage child;
        synchronized (childRefs) {

            /*
             * Note: we need to synchronize on here to ensure visibility for
             * childRefs[index] (in case it was updated in another thread).
             */
            final Reference<AbstractPage> childRef = childRefs[index];

            child = childRef == null ? null : childRef.get();

            if (child != null) {

                // Already materialized.
        		htree.touch(child);
        		
                return child;

            }

        }

        /*
         * The child needs to be read from the backing store.
         */

		// width of a buddy hash table (#of pointer slots).
		final int tableWidth = 1 << globalDepth;

		// offset in [0:nbuddies-1] to the start of the buddy spanning that
		// index.
		final int tableOffset = index / tableWidth;

		/*
		 * We need to get the address of the child, figure out the local depth
		 * of the child (by counting the #of points in the buddy bucket to that
		 * child), and then materialize the child from its address.
		 */
		final long addr = data.getChildAddr(index);

        if (addr == IRawStore.NULL) {

            // dump(Level.DEBUG, System.err);
            /*
             * Note: It appears that this can be triggered by a full disk, but I
             * am not quite certain how a full disk leads to this condition.
             * Presumably the full disk would cause a write of the child to
             * fail. In turn, that should cause the thread writing on the B+Tree
             * to fail. If group commit is being used, the B+Tree should then be
             * discarded and reloaded from its last commit point.
             */
            throw new AssertionError(
                    "Child does not have persistent identity: this=" + this
                            + ", index=" + index);

        }

		/*
		 * Scan to count pointers to child within the buddy hash table.
		 */
		final int npointers;
		{

			int n = 0; // npointers

			final int lastIndex = (tableOffset + tableWidth);

			for (int i = tableOffset; i < lastIndex; i++) {

				if (data.getChildAddr(i) == addr)
					n++;

			}

			assert n > 0;

			npointers = n;

		}

        /*
         * Find the local depth of the child within this node. this becomes the
         * global depth of the child.
         * 
         * Note: This winds up being invoked to compute the depth of an overflow
         * directory page when we first descend from a normal directory page.
         */
        final int localDepth = HTreeUtil.getLocalDepth(htree.addressBits,
                globalDepth, npointers);
        assert npointers == 1 << (htree.addressBits - localDepth);
        /*
         * Read the child from the backing store (potentially reads through to
         * the disk).
         * 
         * Note: This is guaranteed to not do duplicate reads. There are two
         * cases. (A) The mutable B+Tree. Since the mutable B+Tree is single
         * threaded, this case is trivial. (B) The read-only B+Tree. Here our
         * guarantee is that the caller is in ft.run() inside of the Memoizer,
         * and that ensures that only one thread is executing for a given
         * LoadChildRequest object (the input to the Computable). Note that
         * LoadChildRequest MUST meet the criteria for a hash map for this
         * guarantee to obtain.
         */

		child = htree.readNodeOrLeaf(addr);
		
        // set the depth on the child.
        if (!child.isLeaf() && ((DirectoryPage) child).isOverflowDirectory()) {
            /*
             * Note: The global depth of an overflow page is always set to this
             * constant. It should be ignored for code paths which maintain the
             * overflow directory and overflow bucket pages.
             */
            child.globalDepth = getOverflowPageDepth();
        } else {
            child.globalDepth = localDepth;
        }

		/*
		 * Set the reference for each slot in the buddy bucket which pointed at
		 * that child. There will be [npointers] such slots.
         * 
         * Note: This code block is synchronized in order to facilitate the safe
         * publication of the change in childRefs[index] to other threads.
         */
        synchronized (childRefs) {

			int n = 0;

			final int lastIndex = (tableOffset + tableWidth);

			for (int i = tableOffset; i < lastIndex; i++) {

				if (data.getChildAddr(i) == addr) {

		            /*
		             * Since the childRefs[index] element has not been updated we do so
		             * now while we are synchronized.
		             * 
		             * Note: This paranoia test could be tripped if the caller allowed
		             * concurrent requests to enter this method for the same child. In
		             * that case childRefs[index] could have an uncleared reference to
		             * the child. This would indicate a breakdown in the guarantee we
		             * require of the caller.
		             */
		            assert childRefs[i] == null || childRefs[i].get() == null : "Child is already set: this="
		                    + this + ", index=" + i;

		            childRefs[i] = (Reference) child.self;

					n++;

				}

			}

			// patch parent reference since loaded from store.
			child.parent = (Reference) this.self;

			assert n == npointers;

		}

        /*
         * Clear the future task from the memoizer cache.
         * 
         * Note: This is necessary in order to prevent the cache from retaining
         * a hard reference to each child materialized for the B+Tree.
         * 
         * Note: This does not depend on any additional synchronization. The
         * Memoizer pattern guarantees that only one thread actually call
         * ft.run() and hence runs this code.
         */
        if (req != null) {

            htree.memo.removeFromCache(req);

        }

		htree.touch(child);
		
        return child;

    }
    
//    private AbstractPage fixme(final int index) {
//    	
//		/*
//		 * Look at the entry in the buddy hash table. If there is a reference to
//		 * the child and that reference has not been cleared, then we are done
//		 * and we can return the child reference and the offset of the buddy
//		 * table or bucket within the child.
//		 */
//		final Reference<AbstractPage> ref = childRefs[index];
//
//		AbstractPage child = ref == null ? null : ref.get();
//
//		if (child != null) {
//
//			return child;
//
//		}
//
//		// width of a buddy hash table (#of pointer slots).
//		final int tableWidth = 1 << globalDepth;
//
//		// offset in [0:nbuddies-1] to the start of the buddy spanning that
//		// index.
//		final int tableOffset = index / tableWidth;
//
//		/*
//		 * We need to get the address of the child, figure out the local depth
//		 * of the child (by counting the #of points in the buddy bucket to that
//		 * child), and then materialize the child from its address.
//		 * 
//		 * fixme MEMORIZER : This all needs to go through a memoizer pattern.
//		 * The hooks for that should be on AbstractHTree(.memo), but I have not
//		 * yet ported that code.
//		 */
//		final long addr = data.getChildAddr(index);
//
//		/*
//		 * Scan to count pointers to child within the buddy hash table.
//		 */
//		final int npointers;
//		{
//
//			int n = 0; // npointers
//
//			final int lastIndex = (tableOffset + tableWidth);
//
//			for (int i = tableOffset; i < lastIndex; i++) {
//
//				if (data.getChildAddr(i) == addr)
//					n++;
//
//			}
//
//			assert n > 0;
//
//			npointers = n;
//
//		}
//
//		/*
//		 * Find the local depth of the child within this node. this becomes the
//		 * global depth of the child.
//		 */
//		final int localDepth = HTreeUtil.getLocalDepth(htree.addressBits,
//				globalDepth, npointers);
//
//		child = htree.readNodeOrLeaf(addr, localDepth/* globalDepthOfChild */);
//
//		/*
//		 * Set the reference for each slot in the buddy bucket which pointed at
//		 * that child. There will be [npointers] such slots.
//		 */
//		{
//
//			int n = 0;
//
//			final int lastIndex = (tableOffset + tableWidth);
//
//			for (int i = tableOffset; i < lastIndex; i++) {
//
//				if (data.getChildAddr(i) == addr) {
//
//					childRefs[i] = (Reference) child.self;
//
//					n++;
//
//				}
//
//			}
//
//			assert n == npointers;
//
//		}
//
//		return child;
//
//	}

	public AbstractFixedByteArrayBuffer data() {
		return data.data();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Note: This method can only be used once you have decoded the hash bits
	 * from the key and looked up the offset of the buddy hash table on the page
	 * and the offset of the slot within the buddy hash table for the desired
	 * key. You must also know the localDepth of the child when it is
	 * materialized so that information can be set on the child, where is
	 * becomes the globalDepth of the child.
	 * 
	 * @see #getChildAddrByHashCode(int, int)
	 */
	public long getChildAddr(int index) {
		return data.getChildAddr(index);
	}

	public int getChildCount() {
		return data.getChildCount();
	}

	public long getMaximumVersionTimestamp() {
		return data.getMaximumVersionTimestamp();
	}

	public long getMinimumVersionTimestamp() {
		return data.getMinimumVersionTimestamp();
	}

	public boolean hasVersionTimestamps() {
		return data.hasVersionTimestamps();
	}

	public boolean isCoded() {
		return data.isCoded();
	}

	public boolean isLeaf() {
		return data.isLeaf();
	}

    /**
     * The result depends on the backing {@link IDirectoryData} implementation.
     * The {@link DirectoryPage} will be mutable when it is first created and is
     * made immutable when it is persisted. If there is a mutation operation,
     * the backing {@link IDirectoryData} is automatically converted into a
     * mutable instance.
     */
	public boolean isReadOnly() {
		return data.isReadOnly();
	}

    /**
     * @param htree
     *            The owning hash tree.
     * @param overflowDirectory
     *            <code>true</code> iff this is an overflow directory page.
     * @param globalDepth
     *            The size of the address space (in bits) for each buddy hash
     *            table on a directory page. The global depth of a node is
     *            defined recursively as the local depth of that node within its
     *            parent. The global/local depth are not stored explicitly.
     *            Instead, the local depth is computed dynamically when the
     *            child will be materialized by counting the #of pointers to the
     *            the child in the appropriate buddy hash table in the parent.
     *            This local depth value is passed into the constructor when the
     *            child is materialized and set as the global depth of the
     *            child.
     */
	@SuppressWarnings("unchecked")
    public DirectoryPage(final HTree htree, final byte[] overflowKey,
            final int globalDepth) {

		super(htree, true/* dirty */, globalDepth);

		childRefs = new Reference[(1 << htree.addressBits)];

		data = new MutableDirectoryPageData(overflowKey, htree.addressBits,
				htree.versionTimestamps);

	}

	/**
	 * Deserialization constructor - {@link #globalDepth} MUST be set by the
	 * caller.
	 * 
	 * @param htree
	 * @param addr
	 * @param data
	 */
	DirectoryPage(final HTree htree, final long addr, final IDirectoryData data) {

		super(htree, false/* dirty */, 0/*unknownGlobalDepth*/);

        setIdentity(addr);

		this.data = data;

        childRefs = new Reference[(1 << htree.addressBits)];

	}

    /**
     * Copy constructor.
     * 
     * @param src
     *            The source node (must be immutable).
     * 
     * @param triggeredByChildId
     *            The persistent identity of the child that triggered the copy
     *            constructor. This should be the immutable child NOT the one
     *            that was already cloned. This information is used to avoid
     *            stealing the original child since we already made a copy of
     *            it. It is {@link #NULL} when this information is not
     *            available, e.g., when the copyOnWrite action is triggered by a
     *            join() and we are cloning the sibling before we redistribute a
     *            key to the node/leaf on which the join was invoked.
     * 
     * @todo We could perhaps replace this with the conversion of the
     *       INodeData:data field to a mutable field since the code which
     *       invokes copyOnWrite() no longer needs to operate on a new Node
     *       reference. However, I need to verify that nothing else depends on
     *       the new Node, e.g., the dirty flag, addr, etc.
     * 
     * @todo Can't we just test to see if the child already has this node as its
     *       parent reference and then skip it? If so, then that would remove a
     *       troublesome parameter from the API.
     */
	protected DirectoryPage(final DirectoryPage src,
			final long triggeredByChildId) {

        super(src);

        assert !src.isDirty();

        assert src.isReadOnly();
        // assert src.isPersistent();

		/*
		 * Steal/clone the data record.
		 * 
		 * Note: The copy constructor is invoked when we need to begin mutation
		 * operations on an immutable node or leaf, so make sure that the data
		 * record is mutable.
		 */
		final int slotsOnPage = 1 << htree.addressBits;

        assert src.data != null;
		this.data = src.isReadOnly() ? new MutableDirectoryPageData(
				htree.addressBits, src.data) : src.data;
        assert this.data != null;

        // clear reference on source.
        src.data = null;

        /*
         * Steal strongly reachable unmodified children by setting their parent
         * fields to the new node. Stealing the child means that it MUST NOT be
         * used by its previous ancestor (our source node for this copy).
         */

        childRefs = src.childRefs;
        src.childRefs = null;
        
        // childLocks = src.childLocks; src.childLocks = null;

        for (int i = 0; i < slotsOnPage; i++) {
        	
            final AbstractPage child = deref(i);

			/*
			 * Note: Both child.identity and triggeredByChildId will always be
			 * 0L for a transient B+Tree since we never assign persistent
			 * identity to the nodes and leaves. Therefore [child.identity !=
			 * triggeredByChildId] will fail for ALL children, including the
			 * trigger, and therefore fail to set the parent on any of them. The
			 * [btree.store==null] test handles this condition and always steals
			 * the child, setting its parent to this new node.
			 * 
			 * FIXME It is clear that testing on child.identity is broken in
			 * some other places for the transient store. [This comment is
			 * carried over from the B+Tree code.]
			 */
            if (child != null
                    && (htree.store == null || child.getIdentity() != triggeredByChildId)) {

                /*
                 * Copy on write should never trigger for a dirty node and only
                 * a dirty node can have dirty children.
                 */
                assert !child.isDirty();

                // Steal the child.
                child.parent = (Reference) this.self;
                // child.parent = btree.newRef(this);

                // // Keep a reference to the clean child.
                // childRefs[i] = new WeakReference<AbstractNode>(child);

            }

        }

    }

    /**
	 * Iterator visits children, recursively expanding each child with a
	 * post-order traversal of its children and finally visits this node itself.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Iterator<AbstractPage> postOrderNodeIterator(
			final boolean dirtyNodesOnly, final boolean nodesOnly) {

		if (dirtyNodesOnly && !dirty) {

			return EmptyIterator.DEFAULT;

		}

		/*
		 * Iterator append this node to the iterator in the post-order position.
		 */

		return new Striterator(postOrderIterator1(dirtyNodesOnly, nodesOnly))
				.append(new SingleValueIterator(this));

	}

	/**
	 * Iterator visits children recursively expanding each child with a
	 * post-order traversal of its children and finally visits this node itself.
	 */
	@SuppressWarnings("unchecked")
	public Iterator<AbstractPage> postOrderIterator() {

		/*
		 * Iterator append this node to the iterator in the post-order position.
		 */

		return new Striterator(postOrderIterator2())
				.append(new SingleValueIterator(this));

	}

	/**
	 * Visits the children (recursively) using post-order traversal, but does
	 * NOT visit this node.
	 */
	@SuppressWarnings("unchecked")
	private Iterator<AbstractPage> postOrderIterator2() {

		/*
		 * Iterator visits the direct children, expanding them in turn with a
		 * recursive application of the post-order iterator.
		 */

		return new Striterator(childIterator()).addFilter(new Expander() {

			private static final long serialVersionUID = 1L;

			/*
			 * Expand each child in turn.
			 */
			protected Iterator expand(final Object childObj) {

				/*
				 * A child of this node.
				 */

				final AbstractPage child = (AbstractPage) childObj;

				if (!child.isLeaf()) {

					/*
					 * The child is a Node (has children).
					 * 
					 * Visit the children (recursive post-order traversal).
					 */

					// BTree.log.debug("child is node: " + child);
					final Striterator itr = new Striterator(
							((DirectoryPage) child).postOrderIterator2());

					// append this node in post-order position.
					itr.append(new SingleValueIterator(child));

					return itr;

				} else {

					/*
					 * The child is a leaf.
					 */

					// BTree.log.debug("child is leaf: " + child);

					// Visit the leaf itself.
					return new SingleValueIterator(child);

				}
			}
		});

	}

	/**
	 * Iterator visits the direct child nodes in the external key ordering.
	 */
	Iterator<AbstractPage> childIterator() {

		return new ChildIterator();

	}

	/**
	 * Visit the distinct children exactly once (if there are multiple pointers
	 * to a given child, that child is visited just once).
	 */
	private class ChildIterator implements Iterator<AbstractPage> {

		final private int slotsPerPage = 1 << htree.addressBits;
		private int slot = 0;
		private AbstractPage child = null;

		private ChildIterator() {
			nextChild(); // materialize the first child.
		}

        /**
         * Advance to the next distinct child, materializing it if necessary.
         * The first time, this will always return the child in slot zero on the
         * page. Thereafter, it will skip over pointers to the same child and
         * return the next distinct child.
         * <p>
         * Note: For the special case of an overflow directory we allow a
         * <code>null</code> pointer for a child at a given index.
         * 
         * @return <code>true</code> iff there is another distinct child
         *         reference.
         */
		private boolean nextChild() {
//		    final boolean isOverflowDirectory = isOverflowDirectory();
			for (; slot < slotsPerPage; slot++) {
			    AbstractPage tmp = deref(slot);
                if (tmp == null
                        && data.getChildAddr(slot) == NULL) {
			        // Null pointers allowed either in an overflow or with lazy child creation.
			        continue;
			    }
                tmp = tmp == null ? getChild(slot) : tmp;
				if (tmp != child) {
					child = tmp;
					return true;
				}
			}
			return false;
		}

		@Override
		public boolean hasNext() {
			/*
			 * Return true if there is another child to be visited.
			 * 
			 * Note: This depends on nextChild() being invoked by the
			 * constructor and by next().
			 */
			return slot < slotsPerPage;
		}

		@Override
		public AbstractPage next() {
			if (!hasNext())
				throw new NoSuchElementException();
			final AbstractPage tmp = child;
			nextChild(); // advance to the next child (if any).
			return tmp;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	/**
	 * Human readable representation of the {@link Node}.
	 */
	@Override
	public String toString() {

		final StringBuilder sb = new StringBuilder();

		// sb.append(getClass().getName());
		sb.append(super.toString());

		sb.append("{ isDirty=" + isDirty());

		sb.append(", isDeleted=" + isDeleted());

		sb.append(", addr=" + identity);

		final DirectoryPage p = (parent == null ? null : parent.get());

		sb.append(", parent=" + (p == null ? "N/A" : p.toShortString()));

		sb.append(", isRoot=" + (htree.root == this));

		if (data == null) {

			// No data record? (Generally, this means it was stolen by copy on
			// write).
			sb.append(", data=NA}");

			return sb.toString();

		}
		sb.append(", globalDepth=" + getGlobalDepth());
		sb.append(", nbuddies=" + (1 << htree.addressBits) / (1 << globalDepth));
		sb.append(", slotsPerBuddy=" + (1 << globalDepth));
		// sb.append(", minKeys=" + minKeys());
		//
		// sb.append(", maxKeys=" + maxKeys());

		toString(this, sb);

		// indicate if each child is loaded or unloaded.
		{

			final int nchildren = getChildCount();

			sb.append(", children=[");

			for (int i = 0; i < nchildren; i++) {

				if (i > 0)
					sb.append(", ");

				final AbstractPage child = childRefs[i] == null ? null
						: childRefs[i].get();

				sb.append(child == null ? "U" : "L");

			}

			sb.append("]");

		}

		sb.append("}");

		return sb.toString();

	}

	/**
	 * Visits the children (recursively) using post-order traversal, but does
	 * NOT visit this node.
	 */
	@SuppressWarnings("unchecked")
	private Iterator<AbstractPage> postOrderIterator1(
			final boolean dirtyNodesOnly, final boolean nodesOnly) {

		/*
		 * Iterator visits the direct children, expanding them in turn with a
		 * recursive application of the post-order iterator.
		 * 
		 * When dirtyNodesOnly is true we use a child iterator that makes a best
		 * effort to only visit dirty nodes. Especially, the iterator MUST NOT
		 * force children to be loaded from disk if the are not resident since
		 * dirty nodes are always resident.
		 * 
		 * The iterator must touch the node in order to guarantee that a node
		 * will still be dirty by the time that the caller visits it. This
		 * places the node onto the hard reference queue and increments its
		 * reference counter. Evictions do NOT cause IO when the reference is
		 * non-zero, so the node will not be made persistent as a result of
		 * other node touches. However, the node can still be made persistent if
		 * the caller explicitly writes the node onto the store.
		 */

		// BTree.log.debug("node: " + this);
		return new Striterator(childIterator(dirtyNodesOnly))
				.addFilter(new Expander() {

					private static final long serialVersionUID = 1L;

					/*
					 * Expand each child in turn.
					 */
					protected Iterator expand(final Object childObj) {

						/*
						 * A child of this node.
						 */

						final AbstractPage child = (AbstractPage) childObj;

						if (dirtyNodesOnly && !child.isDirty()) {

							return EmptyIterator.DEFAULT;

						}

						if (child instanceof DirectoryPage) {

							/*
							 * The child is a Node (has children).
							 */

							// visit the children (recursive post-order
							// traversal).
							final Striterator itr = new Striterator(
									((DirectoryPage) child).postOrderIterator1(
											dirtyNodesOnly, nodesOnly));

							// append this node in post-order position.
							itr.append(new SingleValueIterator(child));

							return itr;

						} else {

							/*
							 * The child is a leaf.
							 */

							// Visit the leaf itself.
							if (nodesOnly)
								return EmptyIterator.DEFAULT;

							return new SingleValueIterator(child);

						}
					}
				});

	}

	/**
	 * Iterator visits the direct child nodes in the external key ordering.
	 * 
	 * @param dirtyNodesOnly
	 *            When true, only the direct dirty child nodes will be visited.
	 */
	public Iterator<AbstractPage> childIterator(final boolean dirtyNodesOnly) {

		if (dirtyNodesOnly) {

			return new DirtyChildIterator(this);

		} else {

			return new ChildIterator();

		}

	}

	/**
	 * TODO We should dump each bucket page once. This could be done either by
	 * dumping each buddy bucket on the page separately or by skipping through
	 * the directory page until we get to the next bucket page and then dumping
	 * that.
	 * 
	 * TODO The directory page validation should include checks on the bucket
	 * references and addresses. For a given buddy hash table, the reference and
	 * address should pairs should be consistent if either the reference or the
	 * address appears in another slot of that table. Also, there can not be
	 * "gaps" between observations of a reference to a given bucket - once you
	 * see another bucket reference a previously observed reference can not then
	 * appear.
	 * 
	 * @see HTree#validatePointersInParent(DirectoryPage, int, AbstractPage)
	 */
	@Override
	protected boolean dump(final Level level, final PrintStream out, final int height,
			final boolean recursive, final boolean materialize) {

		// True iff we will write out the node structure.
		final boolean debug = level.toInt() <= Level.DEBUG.toInt();

		// Set true iff an inconsistency is detected.
		boolean ok = true;

		// final int branchingFactor = this.getBranchingFactor();
		// final int nkeys = getKeyCount();
		// final int minKeys = this.minKeys();
		// final int maxKeys = this.maxKeys();

		if (this == htree.root) {
			if (parent != null) {
				out.println(indent(height)
						+ "ERROR: this is the root, but the parent is not null.");
				ok = false;
			}
		} else {
			/*
			 * Note: there is a difference between having a parent reference and
			 * having the parent be strongly reachable. However, we actually
			 * want to maintain both -- a parent MUST always be strongly
			 * reachable ... UNLESS you are doing a fast forward or reverse leaf
			 * scan since the node hierarchy is not being traversed in that
			 * case.
			 */
			if (parent == null) {
				out.println(indent(height)
						+ "ERROR: the parent reference MUST be defined for a non-root node.");
				ok = false;
			} else if (parent.get() == null) {
				out.println(indent(height)
						+ "ERROR: the parent is not strongly reachable.");
				ok = false;
			}
		}

		if (debug) {
			out.println(indent(height) + toString());
		}

		/*
		 * Look for inconsistencies for children. A dirty child MUST NOT have an
		 * entry in childAddr[] since it is not persistent and MUST show up in
		 * dirtyChildren. Likewise if a child is NOT dirty, then it MUST have an
		 * entry in childAddr and MUST NOT show up in dirtyChildren.
		 * 
		 * This also verifies that all entries beyond nchildren (nkeys+1) are
		 * unused.
		 */
		for (int i = 0; i < (1 << htree.addressBits); i++) {

			/*
			 * Scanning a valid child index.
			 * 
			 * Note: This is not fetching the child if it is not in memory --
			 * perhaps it should using its persistent id?
			 */

			final AbstractPage child = (childRefs[i] == null ? null
					: childRefs[i].get());

			if (child != null) {

				if (child.parent == null || child.parent.get() == null) {
					/*
					 * the reference to the parent MUST exist since the we are
					 * the parent and therefore the parent is strongly
					 * reachable.
					 */
					out.println(indent(height) + "  ERROR child[" + i
							+ "] does not have parent reference.");
					ok = false;
				}

				if (child.parent.get() != this) {
					out.println(indent(height) + "  ERROR child[" + i
							+ "] has wrong parent.");
					ok = false;
				}

				if (child.isDirty()) {
					/*
					 * Dirty child. The parent of a dirty child MUST also be
					 * dirty.
					 */
					if (!isDirty()) {
						out.println(indent(height) + "  ERROR child[" + i
								+ "] is dirty, but its parent is clean");
						ok = false;
					}
					if (childRefs[i] == null) {
						out.println(indent(height) + "  ERROR childRefs[" + i
								+ "] is null, but the child is dirty");
						ok = false;
					}
					if (getChildAddr(i) != NULL) {
						out.println(indent(height) + "  ERROR childAddr[" + i
								+ "]=" + getChildAddr(i) + ", but MUST be "
								+ NULL + " since the child is dirty");
						ok = false;
					}
				} else {
					/*
					 * Clean child (ie, persistent). The parent of a clean child
					 * may be either clear or dirty.
					 */
					if (getChildAddr(i) == NULL) {
						out.println(indent(height) + "  ERROR childKey[" + i
								+ "] is " + NULL + ", but child is not dirty");
						ok = false;
					}
				}

			}

		}

		if (!ok && !debug) {

			// @todo show the node structure with the errors since we would not
			// have seen it otherwise.

		}

		if (recursive) {

			/*
			 * Dump children using pre-order traversal.
			 */

			final Set<AbstractPage> dirty = new HashSet<AbstractPage>();

			for (int i = 0; i < (1 << htree.addressBits); i++) {

				if (childRefs[i] == null && !isReadOnly()
						&& ((MutableDirectoryPageData) data).childAddr[i] == 0) {

					/*
					 * This let's us dump a tree with some kinds of structural
					 * problems (missing child reference or key).
					 */

					out.println(indent(height + 1)
							+ "ERROR can not find child at index=" + i
							+ ", skipping this index.");

					ok = false;

					continue;

				}

				/*
				 * Note: this works around the assert test for the index in
				 * getChild(index) but is not able/willing to follow a childKey
				 * to a child that is not memory resident.
				 */
				// AbstractNode child = getChild(i);
				final AbstractPage child = childRefs[i] == null ? null
						: childRefs[i].get();

				if (child != null) {

					if (child.parent == null) {

						out.println(indent(height + 1)
								+ "ERROR child does not have parent reference at index="
								+ i);

						ok = false;

					}

					if (child.parent.get() != this) {

						out.println(indent(height + 1)
								+ "ERROR child has incorrect parent reference at index="
								+ i);

						ok = false;

					}

					if (child.isDirty()) {

						dirty.add(child);

					}

					if (!child.dump(level, out, height + 1, true, materialize)) {

						ok = false;

					}

				}

			}

		}

		return ok;

	}

   /**
    * {@inheritDoc}
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1050" > pre-heat the journal
    *      on startup </a>
    */
   @Override
   public void dumpPages(final boolean recursive, final boolean visitLeaves,
         final HTreePageStats stats) {

      stats.visit(htree, this);

      if (!recursive)
         return;

      // materialize children.
      final Iterator<AbstractPage> itr = childIterator();

      while (itr.hasNext()) {

         final AbstractPage child = itr.next();

         if (!visitLeaves && child instanceof BucketPage) {

            /*
             * Note: This always reads the child and then filters out leaves.
             * 
             * Note: It might not be possible to lift this constraint into the
             * childIterator() due to the HTree design (per Martyn's
             * recollection).
             */
            continue;
            
         }

         // recursion into children.
         child.dumpPages(recursive, visitLeaves, stats);

      }

   }

	/**
	 * Utility method formats the {@link IDirectoryData}.
	 * 
	 * @param data
	 *            A data record.
	 * @param sb
	 *            The representation will be written onto this object.
	 * 
	 * @return The <i>sb</i> parameter.
	 */
	static public StringBuilder toString(final IDirectoryData data,
			final StringBuilder sb) {

		final int nchildren = data.getChildCount();

		sb.append(", nchildren=" + nchildren);

		// sb.append(", spannedTupleCount=" + data.getSpannedTupleCount());
		//
		// sb.append(",\nkeys=" + data.getKeys());

		{

			sb.append(",\nchildAddr=[");

			for (int i = 0; i < nchildren; i++) {

				if (i > 0)
					sb.append(", ");

				sb.append(data.getChildAddr(i));

			}

			sb.append("]");

		}

		// {
		//
		// sb.append(",\nchildEntryCount=[");
		//
		// for (int i = 0; i < nchildren; i++) {
		//
		// if (i > 0)
		// sb.append(", ");
		//
		// sb.append(data.getChildEntryCount(i));
		//
		// }
		//
		// sb.append("]");
		//
		// }

		if (data.hasVersionTimestamps()) {

			sb.append(",\nversionTimestamps={min="
					+ data.getMinimumVersionTimestamp() + ",max="
					+ data.getMaximumVersionTimestamp() + "}");

		}

		return sb;

	}

	@Override
	public void PP(final StringBuilder sb, final boolean showBinary) {

		sb.append(PPID() + " [" + globalDepth + "] " + indent(getLevel()));

		sb.append("("); // start of address map.

		// #of buddy tables on a page.
		final int nbuddies = (1 << htree.addressBits) / (1 << globalDepth);

		// #of address slots in each buddy hash table.
		final int slotsPerBuddy = (1 << globalDepth);

		for (int i = 0; i < nbuddies; i++) {

			if (i > 0) // buddy boundary marker
				sb.append(";");

			for (int j = 0; j < slotsPerBuddy; j++) {

				final int slot = i * slotsPerBuddy + j;

				if (j > 0) // slot boundary marker.
					sb.append(",");

				final AbstractPage child = getChildIfPresent(slot);

				sb.append(child == null ? "-" : child.PPID());

			}

		}

		sb.append(")"); // end of address map.

		sb.append("\n");

		final Iterator<AbstractPage> itr = childIterator();

		while (itr.hasNext()) {

			final AbstractPage child = itr.next();

			child.PP(sb, showBinary);

		}

	}

	/**
	 * Tests the slot for content
	 * 
	 * @param slot - the slot to check
	 * @return an existing AbstractPage if present and null otherwise
	 */
	AbstractPage getChildIfPresent(final int slot) {
		if (childRefs[slot] == null && data.getChildAddr(slot) == IRawStore.NULL) {
			return null;
		} else {
			return getChild(slot);
		}
	}

    /**
     * Invoked by {@link #copyOnWrite()} to clear the persistent address for a
     * child on a cloned parent and set the reference to the cloned child.
     * 
     * @param oldChildAddr
     *            The persistent address of the old child. The entries to be
     *            updated are located based on this argument. It is an error if
     *            this address is not found in the list of child addresses for
     *            this {@link Node}.
     * @param newChild
     *            The reference to the new child.
     */
	// FIXME Reconcile two versions of replaceChildRef
	// FIXME Reconcile pattern for deleting a persistent object (htree AND btree)
    void replaceChildRef(final long oldChildAddr, final AbstractPage newChild) {

        assert oldChildAddr != NULL || htree.store == null;
        assert newChild != null;

        // This node MUST have been cloned as a pre-condition, so it can not
        // be persistent.
        assert !isPersistent();
        assert !isReadOnly();

        // The newChild MUST have been cloned and therefore MUST NOT be
        // persistent.
        assert !newChild.isPersistent();

        assert !isReadOnly();

		final MutableDirectoryPageData data = (MutableDirectoryPageData) this.data;

		final int slotsOnPage = 1 << htree.addressBits;

		// Scan for location in weak references.
		int npointers = 0;
		boolean found = false;
		for (int i = 0; i < slotsOnPage; i++) {

            if (data.childAddr[i] == oldChildAddr) {

                found = true;
                
                // remove from cache and free the oldChildAddr if the Strategy
                // supports it.
                // TODO keep this in case we add in the store cache again.
//				if (htree.storeCache != null) {
//					// remove from cache.
//					htree.storeCache.remove(oldChildAddr);
//				}
                // free the oldChildAddr if the Strategy supports it
            	// - and only if not already deleted!
            	if (npointers == 0)
            		htree.deleteNodeOrLeaf(oldChildAddr);
                // System.out.println("Deleting " + oldChildAddr);

                // Clear the old key.
                data.childAddr[i] = NULL;

                // Stash reference to the new child.
                // childRefs[i] = btree.newRef(newChild);
                childRefs[i] = (Reference) newChild.self;
                if (newChild.isPersistent()) {
                	data.childAddr[i] = newChild.getIdentity();
                }

                // // Add the new child to the dirty list.
                // dirtyChildren.add(newChild);

                // Set the parent on the new child.
                // newChild.parent = btree.newRef(this);
                newChild.parent = (Reference) this.self;

                npointers++;
                
            } else if (found) {

                // No more pointers to that child.
                break;
                
            }

        }

		if (npointers == 0)
			throw new IllegalArgumentException("Not our child : oldChildAddr="
					+ oldChildAddr);

    }
    
    int replaceChildRef(final Reference<?> oldRef, final AbstractPage newChild) {
    	
        final int slotsOnPage = 1 << htree.addressBits;

		final MutableDirectoryPageData data = (MutableDirectoryPageData) this.data;
		
		// Scan for location in weak references.
		int firstSlot = -1;
		int npointers = 0;
		for (int i = 0; i < slotsOnPage; i++) {

            if (childRefs[i] == oldRef) {
            	
            	if (firstSlot == -1) firstSlot = i; 

                // Clear the old key.
                data.childAddr[i] = NULL;

                // Stash reference to the new child.
                // childRefs[i] = btree.newRef(newChild);
                if (newChild != null) {
	                childRefs[i] = (Reference) newChild.self;
	                
	                if (newChild.isPersistent()) {
	                	data.childAddr[i] = newChild.getIdentity();
	                }
	
	                newChild.parent = (Reference) this.self;
                } else {
                	childRefs[i] = null;
                }

                npointers++;
            }

        }
		
		assert npointers > 0;
		
		return firstSlot;
    	
    }

	void _addLevel(final BucketPage bucketPage) {
		assert !isReadOnly();
		assert !isOverflowDirectory();

		/**
		 * TBD: Since for _addLevel to be called there should only be a single reference to
		 * bucketPage, this directory MUST be at global depth.  BUT, rather than
		 * replacing the only the old bucket page with the reference to the new
		 * directory, we should/could create a directory of half this directory's depth
		 * and insert the requisite references
		 */
		
        // Create new directory to insert
        final DirectoryPage ndir = new DirectoryPage((HTree) htree,
                null, // overflowKey
                htree.addressBits/* globalDepth */);

        ((HTree) htree).nnodes++;

		// And new bucket page for the new directory
        // only add one initially - to cover the first key
		final BucketPage newPage = createPage(1);
		newPage.parent = (Reference<DirectoryPage>) ndir.self;

		final byte[] tstkey = bucketPage.getFirstKey();
		final int bucketSlot = getLocalHashCode(tstkey, getPrefixLength() + globalDepth);
		final int bucketRefs = (1 << htree.addressBits) >> 1; // half total number of directory slots
		final boolean fillLowerSlots = bucketSlot < bucketRefs;

		final Reference<AbstractPage> a =  (Reference<AbstractPage>) (fillLowerSlots ? newPage.self : null);
		final Reference<AbstractPage> b =  (Reference<AbstractPage>) (fillLowerSlots ? null : newPage.self);
		
		// nleaves is unchanged since we will create one new page and delete the old
		// ((HTree) htree).nleaves++; // Note: only +1 since we will delete the oldPage.
		
		// Link the new bucket pages into the new parent directory page.
		for (int i = 0; i < bucketRefs; i++) {
			ndir.childRefs[i] = a;
			ndir.childRefs[i+bucketRefs] = b;
		}
		
		// now replace the reference to the old bucket page with the new directory
		replaceChildRef(bucketPage.self, ndir);
		
		// ensure that the bucket page doesn't evict		
		bucketPage.delete();
		
		// insert old tuples
		final int bucketSlotsPerPage = bucketPage.slotsOnPage();
		for (int i = 0; i < bucketSlotsPerPage; i++) {
			((HTree) htree).insertRawTuple(bucketPage, i);
		}

		// ...and finally delete old data
		if (bucketPage.isPersistent()) {
			htree.deleteNodeOrLeaf(bucketPage.getIdentity());
		}
		
	}

	/**
	 * This method should only be called when using a DirectoryPage as a BucketPage
	 * Blob.  The final child in the blob is used for insert by default.
	 * 
	 * @param child - the child to be added
	 */
	void _addChild(final AbstractPage child) {
		assert isOverflowDirectory();
		assert !isReadOnly();
		
		// find available slot
		final MutableDirectoryPageData pdata = (MutableDirectoryPageData) data;
		
		for (int i = 0; i < pdata.childAddr.length; i++) {

		    final AbstractPage aChild = childRefs[i] == null ? null
                    : childRefs[i].get();
            
		    if (aChild == null && pdata.childAddr[i] == NULL) {

				childRefs[i] = (Reference<AbstractPage>) child.self;
				
				assert !child.isPersistent();
				
				child.parent = (Reference<DirectoryPage>) self;
				
				return;
			
		    }
		    
		}
		
		// else insert new level above this one and add child to that
		final DirectoryPage pd = getParentDirectory();
		if (pd.isOverflowDirectory()) { // already handles blobs
			assert false; // unsure for now
		} else {
            final DirectoryPage blob = new DirectoryPage((HTree) htree,
                    getOverflowKey(),
                    getOverflowPageDepth());
			blob._addChild(this);
			blob._addChild(child);
			
			pd.replaceChildRef(this.self, blob);
			
			if (INFO)
				log.info("New Overflow Level: " + getLevel());
		}
	}

    /**
     * Return the last non-<code>null</code> child of an overflow directory
     * page.
     */
	BucketPage lastChild() {
        assert isOverflowDirectory();
        for (int i = data.getChildCount() - 1; i >= 0; i--) {
            final BucketPage aChild = (BucketPage) deref(i);
            if (aChild != null) {
                // htree.touch(aChild);
                return aChild;
            }
            if (data.getChildAddr(i) != NULL)
                return (BucketPage) getChild(i);
        }
		
		throw new AssertionError();
	}

    /**
     * Double indirection dereference for the specified index. If the
     * {@link Reference} is <code>null</code>, returns <code>null</code>.
     * Otherwise returns {@link Reference#get()}.
     */
	protected AbstractPage deref(final int index) {

	    return childRefs[index] == null ? null  : childRefs[index].get();

	}
	
	@Override
	public boolean isOverflowDirectory() {
	    
	    return data.isOverflowDirectory();
	    
	}
	
	/**
	 * If this is an overflow directory then the depth-based hashCode is irrelevant
	 * since it is used as a blob container for BucketPage references.
	 */
	@Override
	public int getLocalHashCode(final byte[] key, final int prefixLength) {
		if (isOverflowDirectory()) {
			/*
			 * Shouldn't need to check the key, this will be handled when
			 * the BucketPage is checked for a precise match
			 */
			return 0;
		}
		
		return super.getLocalHashCode(key,  prefixLength);

	}

	/**
	 * This method is never called at present since DirectoryPages are
	 * always created at maximum depth.  Whether there is any advantage
	 * in supporting pages of lesser depths is yet to be determined.
	 * 
	 * @param buddyOffset
	 * @param oldChild
	 */
    public void split(final int buddyOffset, final DirectoryPage oldChild) {

        if (true) {
            /*
             * FIXME We need to update this code to handle the directory page
             * not being at maximum depth to work without the concept of buddy
             * buckets.
             */
            throw new UnsupportedOperationException();
        }
        
        if (parent == null)
            throw new IllegalArgumentException();
        if (oldChild == null)
            throw new IllegalArgumentException();
        if (oldChild.globalDepth >= globalDepth) {
            /*
             * In this case we have to introduce a new directory level instead
             * (increasing the height of the tree at that point).
             */
            throw new IllegalStateException();
        }
        if (buddyOffset < 0)
            throw new IllegalArgumentException();
        if (buddyOffset >= (1 << htree.addressBits)) {
            /*
             * Note: This check is against the maximum possible slot index. The
             * actual max buddyOffset depends on parent.globalBits also since
             * (1<<parent.globalBits) gives the #of slots per buddy and the
             * allowable buddyOffset values must fall on an buddy hash table
             * boundary.
             */
            throw new IllegalArgumentException();
        }
        if(isReadOnly()) // must be mutable.
            throw new IllegalStateException();
        if(oldChild.isReadOnly()) // must be mutable.
            throw new IllegalStateException();
        if (oldChild.parent != self) // must be same Reference.
            throw new IllegalStateException();
        
        if (DEBUG)
            log.debug("parent=" + toShortString() + ", buddyOffset="
                    + buddyOffset + ", child=" + oldChild.toShortString());

        final int oldDepth = oldChild.globalDepth;
        final int newDepth = oldDepth + 1;

        // Allocate a new bucket page (globalDepth is increased by one).
        final DirectoryPage newChild = new DirectoryPage((HTree) htree,
                null, // overflowKey
                newDepth//
                );

        ((HTree) htree).nnodes++; // One more directory page in the hash tree. 
        
        assert newChild.isDirty();
        
        // Set the parent reference on the new bucket.
        newChild.parent = (Reference) self;
        
        // Increase global depth on the old page also.
        oldChild.globalDepth = newDepth;

        // update the pointers
        updatePointers(buddyOffset, oldDepth, oldChild, newChild);
        
        // redistribute buddy buckets between old and new pages.
        redistributeBuddyTables(oldDepth, newDepth, oldChild, newChild);

        // TODO assert invariants?
        
    }
	
	private void updatePointers(
			final int buddyOffset, final int oldDepth,
			final AbstractPage oldChild, final AbstractPage newChild) {

		// #of address slots in the parent buddy hash table.
		final int slotsPerBuddy = (1 << globalDepth);

		// #of pointers in the parent buddy hash table to the old child.
		final int npointers = 1 << (globalDepth - oldDepth);
		
		// Must be at least two slots since we will change at least one.
        if (slotsPerBuddy <= 1)
            throw new AssertionError("slotsPerBuddy=" + slotsPerBuddy);

		// Must be at least two pointers since we will change at least one.
		assert npointers > 1 : "npointers=" + npointers;
		
		// The first slot in the buddy hash table in the parent.
		final int firstSlot = buddyOffset;
		
		// The last slot in the buddy hash table in the parent.
		final int lastSlot = buddyOffset + slotsPerBuddy;

        /*
         * Count pointers to the old child page. There should be [npointers] of
         * them and they should be contiguous.
         * 
         * Note: We can test References here rather than comparing addresses
         * because we know that the parent and the old child are both mutable.
         * This means that their childRef is defined and their storage address
         * is NULL.
         */
		int firstPointer = -1;
		int nfound = 0;
		boolean discontiguous = false;
		for (int i = firstSlot; i < lastSlot; i++) {
			if (childRefs[i] == oldChild.self) {
				if (firstPointer == -1)
					firstPointer = i;
				nfound++;
				if (((MutableDirectoryPageData) data).childAddr[i] != IRawStore.NULL) {
					throw new RuntimeException(
							"Child address should be NULL since child is dirty");
				}
			} else {
				if (firstPointer != -1 && nfound != npointers) {
					discontiguous = true;
				}
			}
		}
		if (firstPointer == -1)
			throw new RuntimeException("No pointers to child");
		if (nfound != npointers)
			throw new RuntimeException("Expected " + npointers
					+ " pointers to child, but found=" + nfound);
		if (discontiguous)
			throw new RuntimeException(
					"Pointers to child are discontiguous in parent's buddy hash table.");

		// Update the upper 1/2 of the pointers to the new bucket.
		for (int i = firstPointer + (npointers >> 1); i < npointers; i++) {

			if (childRefs[i] != oldChild.self)
				throw new RuntimeException("Does not point to old child.");
			
			// update the references to the new bucket.
			childRefs[i] = (Reference) newChild.self;
			
			assert !newChild.isPersistent();
			
		}
			
	} // updatePointersInParent
	
	/**
	 * Redistribute the buddy hash tables in a {@link DirectoryPage}.
	 * <p>
	 * Note: We are not changing the #of hash tables, just their size and the
	 * page on which they are found. Any reference in a source buddy hash table
	 * will wind up in the "same" buddy hash table afterwards, but the page and
	 * offset on the page of the buddy hash table may have been changed and the
	 * size of the buddy hash table will have doubled.
	 * <p>
	 * When a {@link DirectoryPage} is split, the size of each buddy hash table
	 * is doubled. The additional slots in each buddy hash table are filled in
	 * by (a) spacing out the old slot entries in each buddy hash table; and (b)
	 * filling in the uncovered slot with a copy of the previous slot.
	 * <p>
	 * We proceed backwards, moving the upper half of the buddy hash tables to
	 * the new directory page first and then spreading out the lower half of the
	 * source page among the new buddy hash table boundaries on the source page.
	 * 
	 * @param oldDepth
	 *            The depth of the old {@link DirectoryPage} before the split.
	 * @param newDepth
	 *            The depth of the old and new {@link DirectoryPage} after the
	 *            split (this is just oldDepth+1).
	 * @param oldDir
	 *            The old {@link DirectoryPage}.
	 * @param newDir
	 *            The new {@link DirectoryPage}.
	 * 
	 * @deprecated with
	 *             {@link #splitDirectoryPage(DirectoryPage, int, DirectoryPage)}
	 */
    private void redistributeBuddyTables(final int oldDepth,
            final int newDepth, final DirectoryPage oldDir,
            final DirectoryPage newDir) {

        assert oldDepth + 1 == newDepth;
        
        // #of slots on the directory page (invariant given addressBits).
        final int slotsOnPage = (1 << htree.addressBits);

        // #of address slots in each old buddy hash table.
        final int slotsPerOldBuddy = (1 << oldDepth);

        // #of address slots in each new buddy hash table.
        final int slotsPerNewBuddy = (1 << newDepth);

        // #of buddy tables on the old bucket directory.
        final int oldBuddyCount = slotsOnPage / slotsPerOldBuddy;

        // #of buddy tables on the directory page after the split.
        final int newBuddyCount = slotsOnPage / slotsPerNewBuddy;

        final DirectoryPage srcPage = oldDir;
        final long[] srcAddrs = ((MutableDirectoryPageData) oldDir.data).childAddr;
        final Reference<AbstractPage>[] srcRefs = oldDir.childRefs;

        /*
         * Move top 1/2 of the buddy hash tables from the child to the new page.
         */
        {

            // target is the new page.
            final DirectoryPage dstPage = newDir;
            final long[] dstAddrs = ((MutableDirectoryPageData) dstPage.data).childAddr;
            final Reference<AbstractPage>[] dstRefs = dstPage.childRefs;

            // index (vs offset) of first buddy in upper half of src page.
            final int firstSrcBuddyIndex = (oldBuddyCount >> 1);

            // exclusive upper bound for index (vs offset) of last buddy in
            // upper half of src page.
            final int lastSrcBuddyIndex = oldBuddyCount;

            // exclusive upper bound for index (vs offset) of last buddy in
            // upper half of target page.
            final int lastDstBuddyIndex = newBuddyCount;

            // work backwards over buddies to avoid stomping data!
            for (int srcBuddyIndex = lastSrcBuddyIndex - 1, dstBuddyIndex = lastDstBuddyIndex - 1; //
            srcBuddyIndex >= firstSrcBuddyIndex; //
            srcBuddyIndex--, dstBuddyIndex--//
            ) {

                final int firstSrcSlot = srcBuddyIndex * slotsPerOldBuddy;

                final int lastSrcSlot = (srcBuddyIndex + 1) * slotsPerOldBuddy;

                final int firstDstSlot = dstBuddyIndex * slotsPerNewBuddy;

                for (int srcSlot = firstSrcSlot, dstSlot = firstDstSlot; srcSlot < lastSrcSlot; srcSlot++, dstSlot += 2) {

                    if (TRACE)
                        log.trace("moving: page(" + srcPage.toShortString()
                                + "=>" + dstPage.toShortString() + ")"
                                + ", buddyIndex(" + srcBuddyIndex + "=>"
                                + dstBuddyIndex + ")" + ", slot(" + srcSlot
                                + "=>" + dstSlot + ")");

                    for (int i = 0; i < 2; i++) {
                        // Copy data to slot
                        dstAddrs[dstSlot + i] = srcAddrs[srcSlot];
                        dstRefs[dstSlot + i] = srcRefs[srcSlot];
                    }

                }

            }

        }

        /*
         * Reposition the bottom 1/2 of the buddy buckets on the old page.
         * 
         * Again, we have to move backwards through the buddy tables on the
         * source page to avoid overwrites of data which has not yet been
         * copied. Also, notice that the buddy table at index ZERO does not
         * move - it is already in place even though it's size has doubled.
         */
        {

            // target is the old page.
            final DirectoryPage dstPage = oldDir;
            final long[] dstAddrs = ((MutableDirectoryPageData) dstPage.data).childAddr;
            final Reference<AbstractPage>[] dstRefs = dstPage.childRefs;

            // index (vs offset) of first buddy in lower half of src page.
            final int firstSrcBuddyIndex = 0;

            // exclusive upper bound for index (vs offset) of last buddy in
            // lower half of src page.
            final int lastSrcBuddyIndex = (oldBuddyCount >> 1);

            // exclusive upper bound for index (vs offset) of last buddy in
            // upper half of target page (which is also the source page).
            final int lastDstBuddyIndex = newBuddyCount;

            /*
             * Work backwards over buddy buckets to avoid stomping data!
             * 
             * Note: Unlike with a BucketPage, we have to spread out the data in
             * the slots of the first buddy hash table on the lower half of the
             * page as well to fill in the uncovered slots. This means that we
             * have to work backwards over the slots in each source buddy table
             * to avoid stomping our data.
             */
            for (int srcBuddyIndex = lastSrcBuddyIndex - 1, dstBuddyIndex = lastDstBuddyIndex - 1; //
            srcBuddyIndex >= firstSrcBuddyIndex; // 
            srcBuddyIndex--, dstBuddyIndex--//
            ) {

                final int firstSrcSlot = srcBuddyIndex * slotsPerOldBuddy;

                final int lastSrcSlot = (srcBuddyIndex + 1) * slotsPerOldBuddy;

//                final int firstDstSlot = dstBuddyIndex * slotsPerNewBuddy;

                final int lastDstSlot = (dstBuddyIndex + 1) * slotsPerNewBuddy;

                for (int srcSlot = lastSrcSlot-1, dstSlot = lastDstSlot-1; srcSlot >= firstSrcSlot; srcSlot--, dstSlot -= 2) {

                    if (TRACE)
                        log.trace("moving: page(" + srcPage.toShortString()
                                + "=>" + dstPage.toShortString() + ")"
                                + ", buddyIndex(" + srcBuddyIndex + "=>"
                                + dstBuddyIndex + ")" + ", slot(" + srcSlot
                                + "=>" + dstSlot + ")");

                    for (int i = 0; i < 2; i++) {
                        // Copy data to slot.
                        dstAddrs[dstSlot - i] = srcAddrs[srcSlot];
                        dstRefs[dstSlot - i] = srcRefs[srcSlot];
                    }
                    
                }

            }

        }

    }

    /**
	 * Use Striterator Expander to optimize iteration
	 */
	public ITupleIterator getTuples() {
		// start with child nodes
		final Striterator tups = new Striterator(childIterator());
		
		// expand child contents
		tups.addFilter(new Expander() {

			protected Iterator expand(Object obj) {
				if (obj instanceof BucketPage) {
					return ((BucketPage) obj).tuples();
				} else {
					return ((DirectoryPage) obj).getTuples();
				}					
			}
			
		});
		
		// wrap striterator for return type
		return new ITupleIterator() {
			
			public ITuple next() {
				return (ITuple) tups.next();
			}

			public boolean hasNext() {
				return tups.hasNext();
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
		
	}

	@Override
	boolean isClean() {
		for (int i = 0; i < childRefs.length; i++) {
			final AbstractPage node = deref(i);
			if (node != null && !node.isClean()) {
				return false;
			}
		}
		
		return !isDirty();
	}

	public byte[] getOverflowKey() {
		return data.getOverflowKey();
	}

	/**
	 * Called on the condition that an attempt is made to insert a key/value
	 * into an overflow directory with a different key.
	 * 
	 * In this case, this directory has been created to extend the bit resolution
	 * so that the new key/value is resolved to a different slot and a new BucketPage
	 * 
	 * @param overflowKey
	 * @param child
	 */
	DirectoryPage _addLevelForOverflow(final DirectoryPage current) {
		
		if (isReadOnly()) {
			DirectoryPage copy = (DirectoryPage) copyOnWrite(current.getIdentity());
			return copy._addLevelForOverflow(current);
		}
		
		// EvictionProtection ep = new EvictionProtection(this);
		// try {		
			final DirectoryPage newdir = new DirectoryPage((HTree) htree, null /*overflowKey*/, htree.addressBits);
			
			if (isReadOnly()) // TBD: Remove debug point
				assert !isReadOnly();
			
			replaceChildRef(current.self, newdir);
			
			
			// do not want or need to push all bucket values to redistribute, just want to
			// set the directory to relevant key.
			// Logically we want to split the bucket pages until there is only one at the key provided
			final byte[] overflowKey = current.getOverflowKey();
			
			final int slot = newdir.getLocalHashCode(overflowKey, newdir._getPrefixLength());
			
			newdir.childRefs[slot] = (Reference<AbstractPage>) current.self;
			((MutableDirectoryPageData) newdir.data).childAddr[slot] = current.identity;
			current.parent = (Reference<DirectoryPage>) newdir.self;
			
			try {
				current._protectFromEviction();
				
				// now fill others with BucketPage references
				newdir._fillChildSlots(slot, 0, childRefs.length, 0);
			} finally {
				current._releaseProtection();
			}
			
			// System.out.println("_addLevelForOverflow, level: " + newdir.getLevel());
			
			return newdir;
		// } finally {
		//	ep.release();
		// }
	}
	
	private void _touchHierarchy() {
		DirectoryPage dp = this;
		while (dp != null) {
			htree.touch(dp);
			dp = dp.getParentDirectory();
		}
	}

	/**
	 * crawl up the hierarchy decrementing the eviction count
	 * if any hit zero then explicitly evict
	 */
	private void _releaseProtection() {
		DirectoryPage dp = this;
		while (dp != null) {
			if (--dp.referenceCount == 0) {
				htree.writeNodeRecursive(dp);
			}
			dp = dp.getParentDirectory();
		}
	}

	/**
	 * crawl up the hierarchy incrementing the eviction count
	 */
	private void _protectFromEviction() {
		DirectoryPage dp = this;
		while (dp != null) {
			dp.referenceCount++;
			dp = dp.getParentDirectory();
		}
	}

	/**
	 * This method distributes new BucketPages in the child slots around a specified slot
	 * 
	 * The purpose is to create the minimum number of pages to enable the specified slot
	 * to have unique contents. Examples where X indicates specified slot:
	 * 
	 * 2bits
	 * X211
	 * 112X
	 * 
	 * 3bits
	 * X3221111
	 * 3X221111
	 * 22X31111
	 * 1111X322
	 * 
	 * @param slot - the slot to NOT fill
	 * @param offset from where to fill
	 * @param length of range
	 * @param depth of pages
	 */
	private void _fillChildSlots(final int slot, final int offset, final int length, final int depth) {
		assert !isReadOnly();
		
		if (slot == offset && length == 1) {
			assert childRefs[offset] != null;
			
			return;
		}
		
		if (slot >= offset && slot < (offset + length)) {
			final int delta = length/2;
			_fillChildSlots(slot, offset, delta, depth+1);
			_fillChildSlots(slot, offset+delta, delta, depth+1);
		} else {
			final BucketPage bp = new BucketPage((HTree) htree, depth);
			bp.parent = (Reference<DirectoryPage>) self;
			((HTree) htree).nleaves++;

			for (int s = 0; s < length; s++) {
				if (isReadOnly())
					assert !isReadOnly();
				
				assert childRefs[offset+s] == null;
				
				childRefs[offset+s] = (Reference<AbstractPage>) bp.self;
			}
		}
	}

	int _getPrefixLength() {
		int prefixLength = 0;
		DirectoryPage p = getParentDirectory();
		while (p != null) {
			prefixLength += p.globalDepth;
			p = p.getParentDirectory();
		}
		
		return prefixLength;
	}


	/**
     * Ensure their is a single reference to the BucketPage. This assumption is
     * required in the case of overflow, since an overflow Bucket will hold
     * values for a single key.
     * 
     * @param key
     * @param bp
     */
	void _ensureUniqueBucketPage(final byte[] key,
		Reference<? extends AbstractPage> bp) {
		final int prefixLength = _getPrefixLength();
	    final int hashBits = getLocalHashCode(key, prefixLength);
	    assert childRefs[hashBits] == bp;
	    
	    int start = -1;
	    int refs = 0;
	    for (int i = 0; i < childRefs.length; i++) {
	    	if (childRefs[i] == bp) {
	    		start = start == -1 ? i : start;
	    		refs++;
	    		if (i != hashBits) {
	    			childRefs[i] = null;
	    			((MutableDirectoryPageData) data).childAddr[i] = IRawStore.NULL;
	    		}
	    	}
	    }
	    
	    assert Integer.bitCount(refs) == 1; // MUST be power of 2
	    
	    // Don't fill slots!
	    // _fillChildSlots(hashBits, start, refs, 0);
	}

   final public int removeAll(final byte[] key) {
	   if (!isOverflowDirectory())
    	throw new UnsupportedOperationException("Only valid if page is an overflow directory");
	   
		if (isReadOnly()) {
			DirectoryPage copy = (DirectoryPage) copyOnWrite(getIdentity());
			return copy.removeAll(key);
		}
		
		// call removeAll on each child, then htree.remove on existing child store
		int ret = 0;
		for (int i = 0; i < childRefs.length; i++) {
		    AbstractPage tmp = deref(i);
		    if (tmp == null && data.getChildAddr(i) != IRawStore.NULL) {
		    	tmp = getChild(i);
		    }
		    if (tmp == null) {
		    	break;
		    }
		    ret += tmp.removeAll(key);
		}
		
		// Now remove all childAddrs IFF there is a persistent identity
		for (int i = 0; i < childRefs.length; i++) {
		    final long addr = data.getChildAddr(i);
		    if (addr != IRawStore.NULL)
		    	htree.deleteNodeOrLeaf(addr);
		}
		
		return ret;
    }

    final public byte[] removeFirst(final byte[] key) {
 	   if (!isOverflowDirectory())
 	    	throw new UnsupportedOperationException("Only valid if page is an overflow directory");
 	   
 	   /**
 	    * We should check the overflowKey to see if we need to ask the last
 	    * BucketPage to remove the key.
 	    * 
 	    * If so, then we need to ensure the BucketPage is not empty after removal
 	    * and, if so, to remove the reference and mark as deleted.
 	    * 
 	    * This is further complicated if the previous sibling is a DirectoryPage.
 	    * In this case, the original parent directory should be replaced
 	    */
 	   if (BytesUtil.compareBytes(key, getOverflowKey()) == 0) {
 		   
			final AbstractPage last = lastChild();
			
			if (last.isLeaf()) {
				final BucketPage lastbp = (BucketPage) last.copyOnWrite();
				final byte[] ret = lastbp.removeFirst(key);
				
				if (lastbp.getValues().isEmpty()) {
					DirectoryPage dp = lastbp.getParentDirectory();
					
					final int slot = dp.replaceChildRef(lastbp.self, null);
					lastbp.delete();
					if (slot == 1) { // check first child for OverflowDirectory
						AbstractPage odc = dp.getChild(0);
						if (!odc.isLeaf()) {
							DirectoryPage od = (DirectoryPage) odc;
							
							assert od.isOverflowDirectory();
							
							// Right then, since this (dp) only has a single child
							// AND that child is already an overflowDirectory, then
							// its parent must replace the dp.self with the
							// odc.self
							DirectoryPage gdp = dp.getParentDirectory();
							assert gdp != null;
							gdp.replaceChildRef(dp.self, od);
							dp.delete();
							if (DEBUG) {
								log.debug("Promoted child overflowDirectory after remove from last BucketPage");
							}
						}
					} 
				}
				
				return ret;
			} else {
				return last.removeFirst(key);
			}
 	   } else {
 		   return null;
 	   }
    }

	public void removeAll() {
		Iterator<AbstractPage> chs = childIterator(false);
		while (chs.hasNext()) {
			final AbstractPage ch = chs.next();
			if (ch instanceof BucketPage) {
				if (TRACE)
					log.trace("Removing bucket page: " + ch.PPID());
				
				if (ch.isPersistent())
					htree.store.delete(ch.getIdentity());		
			} else {
				if (TRACE)
					log.trace("Removing child directory page: " + ch.PPID());
				
				((DirectoryPage) ch).removeAll();
			}
		}
		if (identity != IRawStore.NULL)
			htree.store.delete(identity);		
	}

}
