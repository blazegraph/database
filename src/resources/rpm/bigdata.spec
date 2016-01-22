Summary: bigdata highly available RDF/graph/SPARQL database
Name: bigdata
Version: @build.ver@
Release: @package.release@
License: GPLv2
Group: Applications/Databases
URL: http://www.bigdata.com/blog
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildArch: noarch

Requires: java >= @java.version@
Requires: zookeeper >= @zookeeper.version@

%description
Bigdata is a horizontally-scaled, open-source architecture for indexed data with an emphasis on RDF capable of loading 1B triples in under one hour on a 15 node cluster.  Bigdata operates in both a single machine mode (Journal), highly available replication cluster mode (HAJournalServer), and a horizontally sharded cluster mode (BigdataFederation).  The Journal provides fast scalable ACID indexed storage for very large data sets, up to 50 billion triples / quads.  The HAJournalServer adds replication, online backup, horizontal scaling of query, and high availability.  The federation provides fast scalable shard-wise parallel indexed storage using dynamic sharding and shard-wise ACID updates and incremental cluster size growth.  Both platforms support fully concurrent readers with snapshot isolation.

Distributed processing offers greater throughput but does not reduce query or update latency.  Choose the Journal when the anticipated scale and throughput requirements permit.  Choose the HAJournalServer for high availability and linear scaling in query throughput.  Choose the Federation when the administrative and machine overhead associated with operating a cluster is an acceptable tradeoff to have essentially unlimited data scaling and throughput.

%package javadoc
Summary: API documentation for %{name}-%{version}
BuildArch: noarch

%description javadoc
API documentation for %{name}-%{version}

%prep
%setup -q

%build
# NOP: The RPM is generated from "binaries".
mkdir -p %{_builddir}/%name-%version@package.prefix@

%install
rm -rf $RPM_BUILD_ROOT
# Rename file paths to reflect package prefix.
%{__mv} %{_builddir}/%name-%version/etc     %{_builddir}/%name-%version@package.prefix@/etc
%{__mv} %{_builddir}/%name-%version/bin     %{_builddir}/%name-%version@package.prefix@/bin
%{__mv} %{_builddir}/%name-%version/doc     %{_builddir}/%name-%version@package.prefix@/doc
%{__mv} %{_builddir}/%name-%version/var     %{_builddir}/%name-%version@package.prefix@/var
%{__mv} %{_builddir}/%name-%version/lib     %{_builddir}/%name-%version@package.prefix@/lib
%{__mv} %{_builddir}/%name-%version/lib-dl  %{_builddir}/%name-%version@package.prefix@/lib-dl
%{__mv} %{_builddir}/%name-%version/lib-ext %{_builddir}/%name-%version@package.prefix@/lib-ext
# Copy all files from BUILD to BUILDROOT
%{__cp} -Rip %{_builddir}/* $RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%files
# FIXME We need pre-/post-install and un-install scripts to symlink
# the init.d script and copy in /etc/bigdata/bigdataHA.config
%defattr(-,root,root,-)
%config @package.prefix@/etc/bigdata
@package.prefix@/etc/init.d/bigdataHA
%config @package.prefix@/var/config
%doc @package.prefix@/doc
@package.prefix@/bin
@package.prefix@/lib
@package.prefix@/lib-dl
@package.prefix@/lib-ext

%changelog
* Sun Nov 24 2013 EC2 Default User <ec2-user@ip-10-164-101-74.ec2.internal> - 
- Initial packaging as rpm.
