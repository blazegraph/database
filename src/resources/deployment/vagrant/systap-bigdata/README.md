systap-bigdata Cookbook
======================
This cookbook provides [http://www.bigdata.com/bigdata/blog/](bigdata v1.3.0) under Tomcat 7 (latest), with Oracle JDK 7 (latest) within an Ubuntu 12.0.4 VM.

Typical synopsis:

	% vagrant up
	
	The bigdata service is then available at: http://33.33.33.10:8080/bigdata/


Requirements
------------

#### packages
In a stand alone context, this cookbook assumes the following resources have been installed:

* `VirtualBox` - Virtual machine provider [http://virtualbox.org/](http://virtualbox.org/) 
* `Vagrant` - Environment assembler [http://vagrantup.com/](http://vagrantup.com/)
* `Berkshelf` - The Berkshelf cookbook manager [http://berkshelf.com/](http://berkshelf.com/).


#### cookbook dependencies
Chef 10.14.2 or higher - has not been tested with previous versions.

The following Opscode cookbooks are dependencies (automatically retrieved by `Berkshelf`):

* apt
* java
* tomcat



Attributes
----------


#### systap-bigdata::default
<table>
  <tr>
    <th>Key</th>
    <th>Type</th>
    <th>Description</th>
    <th>Default</th>
  </tr>
  <tr>
    <td><tt>url</tt></td>
    <td>String</td>
    <td>where to download the bigdata.war file form</td>
    <td><tt>http://sourceforge.net/projects/bigdata/
    files/bigdata/1.3.0/bigdata.war/download</tt></td>
  </tr>
  <tr>
    <td><tt>home</tt></td>
    <td>String</td>
    <td>where the extracted bigdata.war contents are found</td>
    <td>Default: <tt>/var/lib/tomcat7/webapps/bigdata</tt></td>
  </tr>
  <tr>
    <td><tt>etc</tt></td>
    <td>String</td>
    <td>Where under the tomcat root the log files and the bigdata.jni should reside. Discussed in <a href="http://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer#Common_Startup_Problems">"Common Startup Problmems</a></td>
    <td>Default: <tt>/var/lib/tomcat7/webapps/bigdata/etc</tt></td>
  </tr>
</table>


Usage
-----
### Stand Alone Context
To bring the VM up the first time, or any future time after a `halt`, invoke from the cookbook directory:

	% vagrant up

The cookbbok will retrieve the Ubuntu 12.04 VM, Oracle's JDK 7, Apahce's Tomcat 7 and the Bigdata WAR file.  These downloads may take a significant amount of time to complete.  Should a download be interupted or some other error occur, continue with:
	
	% vagrant provision
	
Once complete, the bigdata server will be available under: 
 [http://33.33.33.10:8080/bigdata/](http://33.33.33.10:8080/bigdata/)

To halt the VM:

	% vagrant halt

To delete the VM and from VirtualBox:

	% vagrant destroy
	
To login into the VM:

	% vagrant ssh


### Cookbook Context

To use as a reciple in new cookbook, just include `systap-bigdata` in your node's `run_list` in the standard way:

```
  "run_list": [
    "recipe[systap-bigdata::default]",
     ...
  ]

```


License and Authors
-------------------
Author:: Daniel Mekonnen [daniel<no-spam-at>systap.com]

```
This pakcage may be resiributed under the same terms and conditions as the Bigdata project that it is a part of.
```
