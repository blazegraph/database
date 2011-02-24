package cutthecrap.utils.striterators;

import java.util.Iterator;
import java.util.NoSuchElementException;

import cutthecrap.utils.striterators.IStriterator.ITailOp;

public class Expanderator extends Prefetch implements ITailOp {

	private final Iterator m_src;
	private Iterator m_child = null;
	protected final Object m_context;
	private final Expander m_expander;

	public Expanderator(Iterator src, Object context, Expander expander) {
		m_src = src;
		m_context = context;
		m_expander = expander;
	}

	// -------------------------------------------------------------

	protected Object getNext() {
		if (m_child != null && m_child.hasNext()) {
			final Object ret = m_child.next();
			
			// experimental tail optimisation
			if (m_child instanceof ITailOp) {
				m_child = ((ITailOp) m_child).availableTailOp();
			}

			return ret;
		} else if (m_src.hasNext()) {
			m_child = m_expander.expand(m_src.next());

			return getNext();
		} else {
			return null;
		}
	}

	// -------------------------------------------------------------

	public void remove() {
		m_child.remove();
	}

	public Iterator availableTailOp() {
		if ((!ready()) && !m_src.hasNext()) {
			return m_child;
		} else {
			return this;
		}
	}
}
