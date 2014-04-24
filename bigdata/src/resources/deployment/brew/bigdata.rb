require 'formula'

# Documentation: https://github.com/mxcl/homebrew/wiki/Formula-Cookbook
#                /usr/local/Library/Contributions/example-formula.rb
# PLEASE REMOVE ALL GENERATED COMMENTS BEFORE SUBMITTING YOUR PULL REQUEST!

class SystapBigdata < Formula
  homepage 'http://bigdata.com/bigdata/blog/'
  url 'http://iweb.dl.sourceforge.net/project/bigdata/bigdata/1.3.0/REL.bigdata-1.3.0.tgz'
  sha1 '605e800386300a6965125e0e9bfc06a268df0f08'

  # depends_on 'cmake' => :build
  # depends_on :java7 # if your formula requires any X11/XQuartz components

  def install
    # Install the base files
    prefix.install Dir['*']

    # Setup the lib files
    # (var+'lib/bigdata').mkpath


    # Extract bigdata and install to sbin

    # sbin.install 'bigdata'
    # (sbin/'bigdata').chmod 0755
  end

  def caveats; <<-EOS.undent
    After launching, visit the Bigdata Workbench at: http://localhost:8080/bigdata
    EOS
  end


  plist_options :manual => 'bigdata'

  def plist; <<-EOS.undent
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE plist PUBLIC "-//Apple Computer//DTD PLIST 1.0//EN"
    "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
    <plist version="1.0">
      <dict>
        <key>Label</key>
        <string>#{plist_name}</string>
        <key>Program</key>
        <string>#{opt_sbin}/bigdata</string>
        <key>RunAtLoad</key>
        <true/>
        <key>EnvironmentVariables</key>
        <dict>
          <!-- need erl in the path -->
          <key>PATH</key>
          <string>/usr/local/sbin:/usr/bin:/bin:/usr/local/bin</string>
        </dict>
      </dict>
    </plist>
    EOS
  end
end
