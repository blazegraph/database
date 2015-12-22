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

import com.bigdata.ganglia.xdr.XDROutputBuffer;

/**
 * Interface for generating Ganglia message according to some version of the
 * ganglia wire protocol.
 */
public interface IGangliaMessageEncoder {

	/**
	 * Generate a request record.
	 * 
	 * @param xdr
	 *            Where to write the record.
	 * @param msg
	 *            The message.
	 */
	void writeRequest(XDROutputBuffer xdr, IGangliaRequestMessage msg);

	/**
	 * Ganglia metadata record. This is a NOP until version 3.1.
	 * 
	 * @param xdr
	 *            Where to write the record.
	 * @param decl
	 *            The metadata declaration.
	 */
	void writeMetadata(XDROutputBuffer xdr, IGangliaMetadataMessage decl);

	/**
	 * Generate a metric record.
	 * <p>
	 * Note: It is critical that the {@link IGangliaMetricMessage} is both
	 * internally consistent and consistent with the
	 * {@link IGangliaMetadataMessage} for that metric.
	 * 
	 * @param xdr
	 *            Where to write the record.
	 * @param decl
	 *            The metadata declaration.
	 * @param msg
	 *            The metric value message.
	 */
	void writeMetric(XDROutputBuffer xdr, IGangliaMetadataMessage decl,
			IGangliaMetricMessage msg);

}
