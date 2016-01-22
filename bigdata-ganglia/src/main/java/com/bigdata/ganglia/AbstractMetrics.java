package com.bigdata.ganglia;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


abstract public class AbstractMetrics implements IGangliaAttributes {

	public static final Map<String, String[]> emptyMap = Collections
			.emptyMap();

	/**
	 * Return an extraValues map containing the optional title and/or
	 * description.
	 * 
	 * @param title
	 *            The title (optional).
	 * @param description
	 *            The description (optional).
	 * 
	 * @return The map.
	 */
	static public Map<String, String[]> getMap(final String group,
			final String title, final String description) {

		if (group == null && title == null && description == null) {

			return emptyMap;

		}
		
		final Map<String, String[]> map = new HashMap<String, String[]>();

		if (group != null) {

			map.put(ATTR_GROUP, new String[] { group });
			
		}
		
		if (title != null) {

			map.put(ATTR_TITLE, new String[] { title });

		}

		if (description != null) {

			map.put(ATTR_DESC, new String[] { description });

		}

		return map;

	}
	
	static class NV {
		final public String name;
		final public String[] value;

		public NV(final String name, final String[] value) {
			if (name == null || value == null || value.length == 0)
				throw new IllegalArgumentException();
			this.name = name;
			this.value = value;
		}
	}

	static Map<String, String[]> getMap(final NV[] a) {
		if (a == null || a.length == 0)
			return emptyMap;
		final Map<String, String[]> m = new HashMap<String, String[]>();
		for (NV t : a) {
			if (t == null)
				throw new IllegalArgumentException();
			m.put(t.name, t.value);
		}
		return m;
	}

	protected final String hostName;
	protected final GangliaSlopeEnum slope;
	protected final int tmax;
	protected final int dmax;

	/**
	 * @param hostName
	 *            The name of this host.
	 * @param slope
	 *            The default value to use in the declarations.
	 * @param tmax
	 *            The value of tmax to use in the declarations.
	 * @param dmax
	 *            The value of dmax to use in the declarations.
	 */
	public AbstractMetrics(final String hostName,
			final GangliaSlopeEnum slope, final int tmax, final int dmax) {

		if (hostName == null)
			throw new IllegalArgumentException();

		if (slope == null)
			throw new IllegalArgumentException();

		if (tmax < 0)
			throw new IllegalArgumentException();

		if (dmax < 0)
			throw new IllegalArgumentException();

		this.hostName = hostName;
		this.slope = slope;
		this.tmax = tmax;
		this.dmax = dmax;

	}

}
