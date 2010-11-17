package com.bigdata.bop;

import java.util.LinkedHashSet;

/**
 * A factory which may be used when some identifiers need to be reserved.
 */
public class BOpIdFactory implements IdFactory {
	
	private final LinkedHashSet<Integer> ids = new LinkedHashSet<Integer>();
	
	private int nextId = 0;
	
	public void reserve(int id) {
		ids.add(id);
	}

	public int nextId() {

		while (ids.contains(nextId)) {

			nextId++;
			
		}

		return nextId++;
	}
	
}