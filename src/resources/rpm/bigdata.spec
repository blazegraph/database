Summary: bigdata RDF/graph database
Name: bigdata
Version: 1.2.3
Release: 1
License: GPLv2
Group: Applications/Databases
URL: http://www.bigdata.com/blog
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildArch: noarch

Requires: java

%description

Bigdata is a horizontally-scaled, open-source architecture for indexed data with an emphasis on RDF capable of loading 1B triples in under one hour on a 15 node cluster.  Bigdata operates in both a single machine mode (Journal), highly available replication cluster mode (HAJournalServer), and a horizontally sharded cluster mode (BigdataFederation).  The Journal provides fast scalable ACID indexed storage for very large data sets, up to 50 billion triples / quads.  The HAJournalServer adds replication, online backup, horizontal scaling of query, and high availability.  The federation provides fast scalable shard-wise parallel indexed storage using dynamic sharding and shard-wise ACID updates and incremental cluster size growth.  Both platforms support fully concurrent readers with snapshot isolation.

Distributed processing offers greater throughput but does not reduce query or update latency.  Choose the Journal when the anticipated scale and throughput requirements permit.  Choose the HAJournalServer for high availability and linear scaling in query throughput.  Choose the Federation when the administrative and machine overhead associated with operating a cluster is an acceptable tradeoff to have essentially unlimited data scaling and throughput.

%package javadoc
Summary:        API documentation for %{name}-%{version}

%description javadoc
API documentation for %{name}-%{version}

%prep
%setup -q

%build

%install
rm -rf $RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
%doc


%changelog
* Sun Nov 24 2013 EC2 Default User <ec2-user@ip-10-164-101-74.ec2.internal> - 
- Initial build.

