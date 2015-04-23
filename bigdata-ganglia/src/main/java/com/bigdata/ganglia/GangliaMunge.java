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

public class GangliaMunge {

	/**
	 * Munge a metric name so that it is suitable for use in a filesystem
	 * (ganglia stores metric names and host names in the file system but does
	 * not munge them well itself). In particular, any non-word characters
	 * (other than '.', '-' and similar things which are legal in file names)
	 * are converted to an underscore character ("_"). This gets rid of all
	 * punctuation characters and whitespace in the metric name itself, but will
	 * not translate unicode characters.
	 * 
	 * @param s
	 *            The name of the scale-out index.
	 * 
	 * @return A string suitable for inclusion in a filename.
	 */
	static public String munge(String s) {
	
		// Hacks % Processor Time => Percent Processor Time.
		s = s.replaceAll("%", " Percent ");
	
		// Trim to remove whitespace from hack.
		s = s.trim();
		
		/*
		 * Note: The ganglia UI messes up when there is whitespace in a metric
		 * name (it is ok for the well known descriptions but does not work for
		 * the metric names.)
		 */
	    s = s.replaceAll("[^(\\w|\\.|\\-)]", "_");
	    
	    return s;
	
	}

	/**
	 * Combines the group name and the local name to obtain the fully qualified
	 * metric name. Both names are munged before they are combined.
	 * <p>
	 * Note: Munging is required because we receive counters from other ganglia
	 * services and those counters must obey the rules for file systems, so we
	 * have to munge everything.
	 * 
	 * @param groupName
	 *            The group name (optional).
	 * @param localName
	 *            The local name (required).
	 * 
	 * @return The fully qualified, munged name.
	 */
	static public String getFQName(final String groupName,
			final String localName) {

		final String metricName = groupName == null ? munge(localName)
				: munge(groupName) + "." + munge(localName);

		return metricName;
	
	}

}
