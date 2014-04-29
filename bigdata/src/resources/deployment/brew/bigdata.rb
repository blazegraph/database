require 'formula'

# Documentation: https://github.com/mxcl/homebrew/wiki/Formula-Cookbook
#                /usr/local/Library/Contributions/example-formula.rb
# PLEASE REMOVE ALL GENERATED COMMENTS BEFORE SUBMITTING YOUR PULL REQUEST!

class Bigdata < Formula
  homepage 'http://bigdata.com/blog/'
  url 'http://bigdata.com/deploy/bigdata-1.3.0.tgz'
  sha1 'a395a243a2746ce47cf8893f2207fd2e0de4a9c1'

  def install
    prefix.install Dir['*']
  end

  def caveats; <<-EOS.undent
     After launching, visit the Bigdata Workbench at: 

       http://localhost:8080/bigdata

     "bigdata" command synopis:
     -------------------------

     Start the server:

          % bigdata start

     Stop the server:

          % bigdata stop

     Restart the server:

          % bigdata restart

     To tune the server configuration, edit the "#{var}/jetty/WEB-INF/RWStore.properties" file.

     Further documentation:
	
          #{doc}
    EOS
  end

  plist_options :startup => 'true', :manual => 'bigdata start'

  def plist; <<-EOS.undent
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE plist PUBLIC "-//Apple Computer//DTD PLIST 1.0//EN"
    "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
    <plist version="1.0">
      <dict>
        <key>Label</key>
        <string>#{plist_name}</string>
        <key>Program</key>
        <string>#{bin}/bigdata</string>
        <key>RunAtLoad</key>
        <true/>
        <key>WorkingDirectory</key>
        <string>#{prefix}</string>
      </dict>
    </plist>
    EOS
  end

end
