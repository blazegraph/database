/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/

package com.bigdata.attr;

import net.jini.entry.AbstractEntry;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ServiceInfo extends AbstractEntry {

    private static final long serialVersionUID = 1L;

    public ServiceInfo() { }

    /**
     * The unique id of the service associated with the given instance of
     * this class.
     *
     * @serial
     */
    public UUID source;

    /**
     * <code>String</code> value that should contain a human readable name,
     * description, or identifying string that can be used for display by
     * clients managing or monitoring the associated service. Note that if
     * this field is <code>null</code> or the empty string, then clients
     * are encouraged to use the associated service's class name in such
     * displays.
     *
     * @serial
     */
    public String serviceName;

    /**
     * <code>Map</code> whose key-value pairs consist of an
     * <code>InetAddress</code> as key, and a <code>String</code> as 
     * corresponding value. Each key in the map represents an IP address
     * associated with one of the network interface(s) of the node
     * on which the associated service is exectuing. Each key's value
     * is the name of the network interface to which that IP address
     * is assigned.
     *
     * @serial
     */
    public Map<InetAddress,String> inetAddresses;

    /**
     * <code>String</code> value that is unique across all <i>nodes</i>
     * in the system. With respect to the services executing on a given
     * node, each such service should be able to discover, (or
     * independently set through configuration for example), the information
     * represented by the value of this field. For example, the value in this
     * field might be set to the MAC address of an agreed upon network
     * interface card (NIC) installed on the node on which the associated
     * service is executing, or it might simply be that node's host name
     * or IP address.
     *
     * @serial
     */
    public String nodeToken;

    /**
     * The unique id of the <i>node service</i> having the same
     * <i>node token</i> as the token value referenced in this class;
     * where a single node service executes on each node in the system,
     * acting as a physical representation of the node on which this
     * class' associated service also executes.
     *
     * @serial
     */
    public UUID nodeId;

    /**
     * <code>String</code> value representing the <i>logical name</i> that
     * has been assigned to the <i>node</i> on which the associated service
     * is executing. 
     *
     * @serial
     */
    public String nodeName;

    /**
     * <code>Integer</code> value that uniquely identifies the
     * physical position, within a given <i>rack</i>, in which the 
     * associated node has been mounted, relative to all other
     * devices in the rack.
     * <p>
     * To understand the value that this field corresponds to, recall that
     * the possible positions in which devices can be mounted in a given
     * rack are generally identified by labeling the rack with a sequence
     * of monotonically increasing integers in which the space in the rack
     * from one number in the sequence to the next, represents <i>1U</i> in
     * height; that is, 1 <i>rack unit</i> in height. As such, some devices
     * will occupy only 1U of the rack, where the <i>U-number range</i> of
     * such a device would correspond to the labels <b>n</b> and <b>n+1</b>;
     * whereas other, larger devices may occupy 2U or 4U of the rack, and
     * such devices would have U-number ranges of <b>n</b> to <b>n+2</b>,
     * and <b>n</b> to <b>n+4</b> respectively. With respect to this
     * so-called U-number range, the value of this field then, is the
     * <i>first</i> number of the associated node's U-number range;
     * that is, the vlaue of the U-number label adjacent to the top of
     * the mounted node, relative to that device's position in the rack.
     * For example, consider a node that is 4U in height and which
     * is mounted in a given rack, occupying the positions ranging from
     * U-number 75 to U-number 79. For that particular node, the value
     * of this field would be 75; the first number of the range that is
     * occupied.
     *
     * @serial
     */
    public Integer uNumber;

    /**
     * <code>String</code> value that uniquely identifies the <i>rack</i>
     * (within a given <i>cage</i>) in the system that contains the
     * <i>node</i> on which the associated service is executing.
     *
     * @serial
     */
    public String rack;

    /**
     * <code>String</code> value that uniquely identifies the <i>cage</i>
     * (within a given <i>zone</i>) that contains the <i>rack</i> in which
     * the associated service is executing.
     *
     * @serial
     */
    public String cage;

    /**
     * <code>String</code> value that uniquely identifies the <i>zone</i>
     * (within a given <i>site</i>) that contains the <i>cage</i> in which
     * the associated service is executing.
     *
     * @serial
     */
    public String zone;

    /**
     * <code>String</code> value that uniquely identifies the <i>site</i>
     * (within a given <i>region</i>) that contains the <i>zone</i> in which
     * the associated service is executing.
     *
     * @serial
     */
    public String site;

    /**
     * <code>String</code> value that uniquely identifies the <i>region</i>
     * (within a given <i>geo</i>) that contains the <i>site</i> in which
     * the associated service is executing.
     *
     * @serial
     */
    public String region;

    /**
     * <code>String</code> value that uniquely identifies a given <i>geo</i>
     * designation, across <i>all</i> geo's, that contains the <i>region</i>
     * in which the associated service is executing.
     *
     * @serial
     */
    public String geo;

    /** 
     * @see <code>net.jini.entry.AbstractEntry#equals</code>.
     */
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    /** 
     * @see <code>net.jini.entry.AbstractEntry#hashCode</code>.
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /** 
     * @see <code>net.jini.entry.AbstractEntry#toString</code>.
     */
    @Override
    public String toString() {
        return super.toString();
    }
}
