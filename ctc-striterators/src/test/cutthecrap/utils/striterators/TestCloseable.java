package cutthecrap.utils.striterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

public class TestCloseable extends TestCase {
	ArrayList<Character> m_data = new ArrayList<Character>();
	protected void setUp() {
		char[] chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
		for (int i = 0; i < chars.length; i++) {
			m_data.add(Character.valueOf(chars[i]));
		}
	}
	
	/**
	 * Test that Striterator is closed after hasNext return false
	 */
	public void test_simpleCloseOnEnd() {
		Striterator iter = new Striterator(m_data.iterator());
		assertTrue(iter.isOpen());
		while (iter.hasNext()) {
			iter.next();
			assertTrue(iter.isOpen());
		}
		
		assertTrue(!iter.isOpen());
	}
	
	/**
	 * Test that Striterator returns false for hasNext() after close()
	 */
	public void test_simplePrematureClose() {
		Striterator iter = new Striterator(m_data.iterator());
		int i = 0;
		while (iter.hasNext()) {
			iter.next();
			if (++i == 10) {
				iter.close();
				assertTrue(!iter.hasNext());
			}
		}		
		assertTrue(!iter.isOpen());
		assertTrue(i == 10);
	}
	
	/**
	 * Test that Striterator returns false for hasNext() after close() on parent iterator
	 */
	public void test_delegatedClose1() {
		final AtomicBoolean cls = new AtomicBoolean(false);
		final Striterator nested = new Striterator(m_data.iterator()) {
			public void close() {
				super.close();
				cls.set(true);
			}
		};
		Striterator iter = new Striterator((Iterator) nested);
		int i = 0;
		while (iter.hasNext()) {
			iter.next();
			if (++i == 10) {
				iter.close();
				assertTrue(!nested.hasNext());
			}
		}		
		assertTrue(cls.get());
		assertTrue(!iter.isOpen());
		assertTrue(i == 10);
	}
	
	/**
	 * Test that Striterator returns false for hasNext() after close() on nested iterator
	 */
	public void test_delegatedClose2() {
		final AtomicBoolean cls = new AtomicBoolean(false);
		final Striterator nested = new Striterator(m_data.iterator()) {
			public void close() {
				super.close();
				cls.set(true);
			}
		};
		Striterator iter = new Striterator((Iterator) nested);
		int i = 0;
		while (iter.hasNext()) {
			iter.next();
			if (++i == 10) {
				nested.close();
				assertTrue(!iter.hasNext());
			}
		}		
		assertTrue(cls.get());
		assertTrue(!iter.isOpen());
		assertTrue(i == 10);
	}
}
