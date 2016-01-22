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
 * An earlier version of the Ganglia protocol.
 * 
 * TODO Is 3.0 or 2.5 the previous format and which one is this?
 */
public class GangliaMessageEncoder25 implements IGangliaMessageEncoder {

	/**
	 * TODO I am not sure whether or not there was a request message in this
	 * protocol version. While the metadata was sent with each metric record,
	 * the purpose of the request is not only to obtain the metadata but also to
	 * tell the ganglia services that a new service wants to get up to speed and
	 * would they please send it their current state.
	 * 
	 * @param xdr
	 * @param msg
	 */
	@Override
	public void writeRequest(XDROutputBuffer xdr, IGangliaRequestMessage msg) {

		// NOP
		
	}

	/**
	 * NOP. The metadata was sent with each metric message in this version of
	 * the protocol.
	 * 
	 * @param xdr
	 * @param decl
	 */
	@Override
	public void writeMetadata(XDROutputBuffer xdr, IGangliaMetadataMessage decl) {

		// NOP

	}

	public void writeMetric(final XDROutputBuffer xdr,
			final IGangliaMetadataMessage decl,
			final IGangliaMetricMessage msg) {

		xdr.reset();
		xdr.writeInt(0); // metric_user_defined
		xdr.writeString(decl.getMetricType().getGType());
		xdr.writeString(decl.getMetricName());
		xdr.writeString(msg.getStringValue());
		xdr.writeString(decl.getUnits());
		xdr.writeInt(decl.getSlope().value());
		xdr.writeInt(decl.getTMax());
		xdr.writeInt(decl.getDMax());

	}
}
