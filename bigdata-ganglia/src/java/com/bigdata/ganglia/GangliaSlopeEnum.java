/*
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.ganglia;

/**
 * This slope is a metadata parameter which ganglia carries for the rrdtool
 * integration. Slope appears to be essentially a hint provided to rrdtool which
 * effects the visualization of the data.
 */
public enum GangliaSlopeEnum {

	/** Plot the points as step functions (no interpolation). */
	zero(0),
	/** Interpolate on positive value change. */
	positive(1),
	/** Interpolate on negative value change. */
	negative(2),
	/** Interpolate on any value change. */
	both(3),
	/**
	 * Used (by Ganglia) for things like <code>heartbeat</code> and
	 * <code>location</code>. (Location is reported with
	 * <code>units := (x,y,z)</code>.)
	 */
	unspecified(4);

	private GangliaSlopeEnum(final int v) {
		this.v = v;
	}

	private final int v;

	public int value() {
		return v;
	}

	public static final GangliaSlopeEnum valueOf(final int v) {
		switch (v) {
		case 0:
			return zero;
		case 1:
			return positive;
		case 2:
			return negative;
		case 3:
			return both;
		case 4:
			return unspecified;
		default:
			throw new IllegalArgumentException("value=" + v);
		}
	}
}
