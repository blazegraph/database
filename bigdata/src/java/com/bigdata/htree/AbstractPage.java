package com.bigdata.htree;

import java.io.PrintStream;
import java.lang.ref.Reference;
import java.util.Iterator;

import org.apache.log4j.Level;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.PO;
import com.bigdata.btree.data.IAbstractNodeData;
import com.bigdata.cache.HardReferenceQueue;

/**
 * Persistence capable abstract base class for HTree pages.
 * 
 * @author thompsonbry
 */
abstract class AbstractPage extends PO implements // IAbstractNode?,
		IAbstractNodeData {

	@Override
	public String toShortString() {

		return super.toShortString() + "{d=" + globalDepth + "}";

	}

	/**
	 * The HTree.
	 * 
	 * Note: This field MUST be patched when the node is read from the store.
	 * This requires a custom method to read the node with the HTree reference
	 * on hand so that we can set this field.
	 */
	final transient protected AbstractHTree htree;

	/**
	 * The parent of this node. This is null for the root node. The parent is
	 * required in order to set the persistent identity of a newly persisted
	 * child node on its parent. The reference to the parent will remain
	 * strongly reachable as long as the parent is either a root (held by the
	 * {@link HTree}) or a dirty child (held by the {@link DirectoryPage}). The
	 * parent reference is set when a node is attached as the child of another
	 * node.
	 * <p>
	 * Note: When a node is cloned by {@link #copyOnWrite()} the parent
	 * references for its <em>clean</em> children are set to the new copy of the
	 * node. This is referred to in several places as "stealing" the children
	 * since they are no longer linked back to their old parents via their
	 * parent reference.
	 */
	transient protected Reference<DirectoryPage> parent = null;

	/**
	 * <p>
	 * A {@link Reference} to this {@link AbstractPage}. This is created when
	 * the node is created and is reused by a children of the node as the
	 * {@link Reference} to their parent. This results in few {@link Reference}
	 * objects in use by the HTree since it effectively provides a canonical
	 * {@link Reference} object for any given {@link AbstractPage}.
	 * </p>
	 */
	transient protected final Reference<? extends AbstractPage> self;

	/**
	 * The #of times that this node is present on the {@link HardReferenceQueue}
	 * . This value is incremented each time the node is added to the queue and
	 * is decremented each time the node is evicted from the queue. On eviction,
	 * if the counter is zero(0) after it is decremented then the node is
	 * written on the store. This mechanism is critical because it prevents a
	 * node entering the queue from forcing IO for the same node in the edge
	 * case where the node is also on the tail on the queue. Since the counter
	 * is incremented before it is added to the queue, it is guaranteed to be
	 * non-zero when the node forces its own eviction from the tail of the
	 * queue. Preventing this edge case is important since the node can
	 * otherwise become immutable at the very moment that it is touched to
	 * indicate that we are going to update its state, e.g., during an insert,
	 * split, or remove operation. This mechanism also helps to defer IOs since
	 * IO can not occur until the last reference to the node is evicted from the
	 * queue.
	 * <p>
	 * Note that only mutable {@link BTree}s may have dirty nodes and the
	 * {@link BTree} is NOT thread-safe for writers so we do not need to use
	 * synchronization or an AtomicInteger for the {@link #referenceCount}
	 * field.
	 */
	transient protected int referenceCount = 0;

	/**
	 * The size of the address space (in bits) for each buddy hash table on a
	 * directory page. The global depth of a node is defined recursively as the
	 * local depth of that node within its parent. The global/local depth are
	 * not stored explicitly. Instead, the local depth is computed dynamically
	 * when the child will be materialized by counting the #of pointers to the
	 * the child in the appropriate buddy hash table in the parent. This local
	 * depth value is passed into the constructor when the child is materialized
	 * to set the global depth of the child.
	 */
	protected int globalDepth;

	/**
	 * The size of the address space (in bits) for each buddy hash table on a
	 * directory page. The legal range is <code>[0:addressBits-1]</code>.
	 * <p>
	 * When the global depth is increased, the hash table requires twice as many
	 * slots on the page. This forces the split of the directory page onto two
	 * pages in order to accommodate the additional space requirements. The
	 * maximum global depth is <code>addressBits</code>, at which point the hash
	 * table fills the entire directory page. The minimum global depth is ZERO
	 * (0), at which point the buddy hash table has a single slot.
	 * <p>
	 * The global depth of a child page is just the local depth of the directory
	 * page in its parent. The global depth of the child page is often called
	 * its <em>local depth</em>.
	 * <p>
	 * The global depth of the root is always <i>addressBits</i>.
	 */
	public int getGlobalDepth() {
		return globalDepth;
	}

	/**
	 * The #of buddy tables (buckets) on a directory (bucket) page. This depends
	 * solely on <i>addressBits</i> (a constant) and <i>globalDepth</i> and is
	 * given by <code>(2^addressBits) / (2^globalBits)</code>.
	 */
	public int getNumBuddies() {
		final int nbuddies = (1 << htree.addressBits) / (1 << globalDepth);
		return nbuddies;
	}

	/**
	 * The #of directory entries in a buddy hash table for this directory page.
	 * This depends solely on the <i>globalDepth</i> of this directory page and
	 * is given by <code>2^globalDepth</code>.
	 */
	public int getSlotsPerBuddy() {
		final int slotsPerBuddy = (1 << globalDepth);
		return slotsPerBuddy;
	}

	/**
	 * Return the prefix length of the page (the #of bits of the key which have
	 * been consumed by the parent directory pages before reaching this page).
	 */
	final public int getPrefixLength() {

		int ret = 0;

		DirectoryPage dp = parent != null ? parent.get() : null;

		while (dp != null) {

			ret += dp.globalDepth;

			dp = dp.parent != null ? dp.parent.get() : null;

		}

		return ret;

	}

	/**
	 * Computed by recursing to the root and counting the levels. The root is at
	 * depth ZERO (0).
	 * 
	 * @return The level in the {@link HTree}.
	 */
	final public int getLevel() {

		int ret = 0;

		DirectoryPage dp = parent != null ? parent.get() : null;

		while (dp != null) {

			ret++;

			dp = dp.parent != null ? dp.parent.get() : null;

		}

		return ret;

	}

	/**
	 * Return the bits from the key which are relevant to the current directory
	 * page (varient for unsigned byte[] keys). This depends on the
	 * <i>prefixLength</i> to be ignored, the <i>globalDepth</i> of this
	 * directory page, and the key.
	 * 
	 * @param key
	 *            The key.
	 * @param prefixLength
	 *            The #of MSB bits in the key which are to be ignored at this
	 *            level of the hash tree. This is computed dynamically during
	 *            recursive traversal of the hash tree. This is ZERO (0) for the
	 *            root directory. It is incremented by <i>globalDepth</i> (the
	 *            #of address bits used by a given node) at each level of
	 *            recursion for insert, lookup, etc.
	 * 
	 * @return The int32 value containing the relevant bits from the key.
	 */
	public int getLocalHashCode(final byte[] key, final int prefixLength) {

		return BytesUtil.getBits(key, prefixLength, globalDepth);

	}

	/**
	 * Return the bits from the key which are relevant to the current directory
	 * page (variant for int32 keys). This depends on the <i>prefixLength</i> to
	 * be ignored, the <i>globalDepth</i> of this directory page, and the key.
	 * 
	 * @param key
	 *            The key.
	 * @param prefixLength
	 *            The #of MSB bits in the key which are to be ignored at this
	 *            level of the hash tree. This is computed dynamically during
	 *            recursive traversal of the hash tree. This is ZERO (0) for the
	 *            root directory. It is incremented by <i>globalDepth</i> (the
	 *            #of address bits used by a given node) at each level of
	 *            recursion for insert, lookup, etc.
	 * 
	 * @return The int32 value containing the relevant bits from the key.
	 */
	public int getLocalHashCode(final int key, final int prefixLength) {

		return BytesUtil.getBits(key, prefixLength, globalDepth);

	}

	public DirectoryPage getParentDirectory() {
		return parent != null ? parent.get() : null;
	}

	/**
	 * Disallowed.
	 */
	private AbstractPage() {

		throw new UnsupportedOperationException();

	}

	/**
	 * All constructors delegate to this constructor to set the htree reference
	 * and core metadata.
	 * 
	 * @param htree
	 *            The {@link HTree} to which the page belongs.
	 * @param dirty
	 *            Used to set the {@link PO#dirty} state. All nodes and leaves
	 *            created by non-deserialization constructors begin their life
	 *            cycle as <code>dirty := true</code> All nodes or leaves
	 *            de-serialized from the backing store begin their life cycle as
	 *            clean (dirty := false). This we read nodes and leaves into
	 *            immutable objects, those objects will remain clean. Eventually
	 *            a copy-on-write will create a mutable node or leaf from the
	 *            immutable one and that node or leaf will be dirty.
	 * @param globalDepth
	 *            The size of the address space (in bits) for each buddy hash
	 *            table (bucket) on a directory (bucket) page. The global depth
	 *            of a node is defined recursively as the local depth of that
	 *            node within its parent. The global/local depth are not stored
	 *            explicitly. Instead, the local depth is computed dynamically
	 *            when the child will be materialized by counting the #of
	 *            pointers to the the child in the appropriate buddy hash table
	 *            in the parent. This local depth value is passed into the
	 *            constructor when the child is materialized and set as the
	 *            global depth of the child.
	 */
	protected AbstractPage(final HTree htree, final boolean dirty,
			final int globalDepth) {

		if (htree == null)
			throw new IllegalArgumentException();

		if (globalDepth < 0)
			throw new IllegalArgumentException();

		if (globalDepth > htree.addressBits)
			throw new IllegalArgumentException();

		this.htree = htree;

		this.globalDepth = globalDepth;

		// reference to self: reused to link parents and children.
		this.self = htree.newRef(this);

		if (!dirty) {

			/*
			 * Nodes default to being dirty, so we explicitly mark this as
			 * clean. This is ONLY done for the de-serialization constructors.
			 */

			setDirty(false);

		}

		// Add to the hard reference queue.
		htree.touch(this);

	}

	public void delete() throws IllegalStateException {

		if (deleted) {

			throw new IllegalStateException();

		}

		/*
		 * Release the state associated with a node or a leaf when it is marked
		 * as deleted, which occurs only as a side effect of copy-on-write. This
		 * is important since the node/leaf remains on the hard reference queue
		 * until it is evicted but it is unreachable and its state may be
		 * reclaimed immediately.
		 */

		parent = null; // Note: probably already null.

		// release the key buffer.
		/* nkeys = 0; */
		// keys = null;

		// Note: do NOT clear the referenceCount.

		if (identity != NULL) {

			/*
			 * Deallocate the object on the store.
			 * 
			 * Note: This operation is not meaningful on an append only store.
			 * If a read-write store is defined then this is where you would
			 * delete the old version.
			 * 
			 * Note: Do NOT clear the [identity] field in delete().
			 * copyOnWrite() depends on the field remaining defined on the
			 * cloned node so that it may be passed on.
			 */

			// btree.store.delete(identity);

		}

		deleted = true;

	}

	/**
	 * Dump the data onto the {@link PrintStream}.
	 * 
	 * @param level
	 *            The logging level.
	 * @param out
	 *            Where to write the dump.
	 * @param height
	 *            The height of this node in the tree or -1 iff you need to
	 *            invoke this method on a node or leaf whose height in the tree
	 *            is not known.
	 * @param recursive
	 *            When true, the node will be dumped recursively using a
	 *            pre-order traversal.
	 * @param materialize
	 *            When <code>true</code>, children will be materialized as
	 *            necessary to dump the tree.
	 * 
	 * @return <code>true</code> unless an inconsistency was detected.
	 */
	abstract protected boolean dump(Level level, PrintStream out, int height,
			boolean recursive, boolean materialize);

	/** Pretty print the tree from this level on down. */
	abstract void PP(StringBuilder sb);

	/** Return a very short id used by {@link #PP()}. */
	protected String PPID() {

		final int hash = hashCode() % 100;

		// Note: fixes up the string if hash is only one digit.
		final String hashStr = "#" + (hash < 10 ? "0" : "") + hash;

		return (isLeaf() ? "B" : "D") + hashStr;

	}

	abstract void insertRawTuple(final byte[] key, final byte[] val,
			final int buddy);

	final public Iterator<AbstractPage> postOrderNodeIterator() {

		return postOrderNodeIterator(false/* dirtyNodesOnly */, false/* nodesOnly */);

	}

    /**
	 * Post-order traversal of nodes and leaves in the tree. For any given node,
	 * its children are always visited before the node itself (hence the node
	 * occurs in the post-order position in the traversal). The iterator is NOT
	 * safe for concurrent modification.
	 * 
	 * @param dirtyNodesOnly
	 *            When true, only dirty nodes and leaves will be visited
	 * 
	 * @return Iterator visiting {@link AbstractPage}s.
	 */
	final public Iterator<AbstractPage> postOrderNodeIterator(
			final boolean dirtyNodesOnly) {

		return postOrderNodeIterator(dirtyNodesOnly, false/* nodesOnly */);

	}

	/**
	 * Post-order traversal of nodes and leaves in the tree. For any given node,
	 * its children are always visited before the node itself (hence the node
	 * occurs in the post-order position in the traversal). The iterator is NOT
	 * safe for concurrent modification.
	 * 
	 * @param dirtyNodesOnly
	 *            When true, only dirty nodes and leaves will be visited
	 * @param nodesOnly
	 *            When <code>true</code>, the leaves will not be visited.
	 * 
	 * @return Iterator visiting {@link AbstractPage}s.
	 */
	abstract public Iterator<AbstractPage> postOrderNodeIterator(
			final boolean dirtyNodesOnly, final boolean nodesOnly);

}
