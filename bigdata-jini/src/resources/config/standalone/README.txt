A sample configuration for a standalone bigdata instance (single host, one or
more data servers).

Note: If you want to modify this configuration for multiple hosts then
you MUST edit the *.config files to enable multicast discovery or explicitly
enumerate the URIs of the service registrar(s) (only localhost is listed by
default).

To enable multicase, change:

	/*
	 * Note: multicast discovery is always used if LookupDiscovery.ALL_GROUPS is
	 * specified.
	 */
//    groups = LookupDiscovery.ALL_GROUPS;
    groups = new String[]{"bigdata"};

	/*
	 * One or more unicast URIs of the form jini://host/ or jini://host:port/.
	 * This MAY be an empty array if you want to use multicast discovery _and_
	 * you have specified LookupDiscovery.ALL_GROUPS above.
	 */
    unicastLocators = new LookupLocator[] {
    	new LookupLocator("jini://localhost/")
	};    

to

	/*
	 * Note: multicast discovery is always used if LookupDiscovery.ALL_GROUPS is
	 * specified.
	 */
    groups = LookupDiscovery.ALL_GROUPS;

	/*
	 * One or more unicast URIs of the form jini://host/ or jini://host:port/.
	 * This MAY be an empty array if you want to use multicast discovery _and_
	 * you have specified LookupDiscovery.ALL_GROUPS above.
	 */
    unicastLocators = new LookupLocator[] {
	};    
