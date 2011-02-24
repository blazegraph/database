package cutthecrap.utils.striterators;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class Prefetch implements Iterator {
	private Object m_next;
	private boolean m_ready = false;

	final private void checkInit() {
		if (!m_ready) {
			m_next = getNext();
			m_ready = true;
		}
	}

	abstract protected Object getNext();

	public boolean hasNext() {
		checkInit();

		return m_next != null;
	}

	public Object next() {
		checkInit(); // check prefetch is ready

		if (m_next == null) {
			throw new NoSuchElementException();
		}

		Object ret = m_next;

		// do not prefetch on next() since this may cause problems with
		// side-effecting
		// overides
		m_next = null;
		m_ready = false;

		return ret;
	}
	
	protected boolean ready() {
		return m_ready;
	}
}
