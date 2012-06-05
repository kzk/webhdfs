# webhdfs - A client library implementation for Hadoop WebHDFS, and HttpFs, for Ruby

The webhdfs gem is to access Hadoop WebHDFS (EXPERIMENTAL: and HttpFs). WebHDFS::Client is a client class, and WebHDFS::FileUtils is utility like 'fileutils'.

## Installation

    gem install webhdfs

## Usage

### WebHDFS::Client

For client object interface:

    require 'webhdfs'
    client = WebHDFS::Client.new(hostname, port)
    # or with pseudo username authentication
    client = WebHDFS::Client.new(hostname, port, username)

To create/append/read files:

    client.create('/path/to/file', data)
    client.create('/path/to/file', data, :overwrite => false, :blocksize => 268435456, :replication => 5, :permission => 0666)
    
    client.append('/path/to/existing/file', data)
    
    client.read('/path/to/target') #=> data
    client.read('/path/to/target' :offset => 2048, :length => 1024) #=> data

To mkdir/rename/delete directories or files:

    client.mkdir('/hdfs/dirname')
    client.mkdir('/hdfs/dirname', :permission => 0777)
    
    client.rename(original_path, dst_path)
    
    client.delete(path)
    client.delete(dir_path, :recursive => true)

To get status or list of files and directories:

    client.stat(file_path) #=> key-value pairs for file status
    client.list(dir_path)  #=> list of key-value pairs for files in dir_path

And, 'content_summary', 'checksum', 'homedir', 'chmod', 'chown', 'replication' and 'touch' methods available.

### WebHDFS::FileUtils

    require 'webhdfs/fileutils'
    WebHDFS::FileUtils.set_server(host, port)
    # or
    WebHDFS::FileUtils.set_server(host, port, username, doas)
    
    WebHDFS::FileUtils.copy_from_local(localpath, hdfspath)
    WebHDFS::FileUtils.copy_to_local(hdfspath, localpath)
    
    WebHDFS::FileUtils.append(path, data)

### For HttpFs

For HttpFs instead of WebHDFS:

    client = WebHDFS::Client('hostname', 14000)
    client.httpfs_mode = true
    
    client.read(path) #=> data
    
    # or with webhdfs/filetuils
    WebHDFS::FileUtils.set_server('hostname', 14000)
    WebHDFS::FileUtils.set_httpfs_mode
    WebHDFS::FileUtils.copy_to_local(remote_path, local_path)

## AUTHORS

* Kazuki Ohta <kazuki.ohta@gmail.com>
* TAGOMORI Satoshi <tagomoris@gmail.com>

## LICENSE

* Copyright: Copyright (c) 2012- Fluentd Project
* License: Apache License, Version 2.0
