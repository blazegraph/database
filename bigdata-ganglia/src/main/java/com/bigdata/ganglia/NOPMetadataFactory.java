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
 * NOP factory.
 */
public class NOPMetadataFactory implements IGangliaMetadataFactory {

	/**
	 * Always returns <code>null</code>.
	 */
	@Override
	public IGangliaMetadataMessage newDecl(String hostName, String metricName,
			Object value) {
		// NOP
		return null;
	}

	/**
	 * Always returns the caller's argument.
	 */
	@Override
	public IGangliaMetadataMessage resolve(IGangliaMetadataMessage decl) {
		return decl;
	}

}
