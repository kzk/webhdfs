module WebHDFS
  module FileUtils
    require 'rest_client'

    # This hash table holds command options.
    OPT_TABLE = {} # internal use only

    @fu_host = 'localhost'
    @fu_port = 50070
    def set_host(host, port)
      @fu_host = host
      @fu_port = port
    end

    # Public: Create one or more directories.
    #
    # list - directory name, or list of them
    # options - :mode, :verbose
    #
    # Examples
    #
    #   FileUtils.mkdir 'test'
    #   FileUtils.mkdir %w( tmp data )
    #   FileUtils.mkdir 'tmp', :mode => 0700
    #
    def mkdir(list, options={})
      fu_check_options options, OPT_TABLE['mkdir']
      list = fu_list(list)
      fu_log "mkdir #{options[:mode] ? ('-m %03o ' % options[:mode]) : ''}#{list.join ' '}" if options[:verbose]
      if mode = options[:mode]
        mode = ('0%03o' % mode) if mode.is_a? Integer
      else
        mode = '0755'
      end
      list.each { |dir|
        fu_put(dir, 'MKDIRS', {:permission => mode})
      }
    end
    OPT_TABLE['mkdir'] = [:mode, :verbose]
    module_function :mkdir

    # Public: Create one or more directories recursively.
    #
    # list - directory name, or list of them
    # options - :mode, :verbose
    #
    # Examples
    #
    #   FileUtils.mkdir_p 'dir/subdir'
    #   FileUtils.mkdir_p %w( tmp data )
    #   FileUtils.mkdir_p 'dir/subdir', :mode => 0700
    #
    alias mkdir_p mkdir
    module_function :mkdir_p

    # Public: Remove one or more directories or files.
    #
    # list - directory name, or list of them
    # options - :recursive, :verbose
    #
    # Examples
    #
    #   FileUtils.rm 'dir'
    #   FileUtils.rm %w( tmp data )
    #   FileUtils.rm 'dir', :recursive => true
    #
    def rm(list, options={})
      fu_check_options options, OPT_TABLE['rm']
      list = fu_list(list)
      fu_log "rm #{list.join ' '}" if options[:verbose]
      list.each { |dir|
        fu_delete(dir, 'DELETE', {:recursive => options[:recursive] || false})
      }
    end
    OPT_TABLE['rm'] = [:verbose, :recursive]
    module_function :rm

    # Public: Remove one or more directories/files recursively.
    #
    # list - directory name, or list of them
    # options - :verbose
    #
    # Examples
    #
    #   FileUtils.rmr 'dir'
    #   FileUtils.rmr %w( tmp data )
    #   FileUtils.rmr 'dir'
    #
    def rmr(list, options={})
      fu_check_options options, OPT_TABLE['rmr']
      self.rm(list, options.merge({:recursive => true}))
    end
    OPT_TABLE['rmr'] = [:verbose]
    module_function :rmr

    # Public: Rename a file or directory.
    #
    # src - from
    # dst - to
    # options - :verbose
    #
    # Examples
    #
    #   FileUtils.rename 'from', 'to'
    #
    def rename(src, dst, options={})
      fu_check_options options, OPT_TABLE['rename']
      fu_log "rename #{src} #{dst}" if options[:verbose]
      fu_put(src, 'RENAME', {:destination => dst})
    end
    OPT_TABLE['rename'] = [:verbose]
    module_function :rename

    # Public: Change permission of one or more directories/files.
    #
    # mode - permission
    # list - file/directory name or list of them.
    # options - :verbose
    #
    # Examples
    #
    #   FileUtils.chmod 0755, 'dir'
    #   FileUtils.chmod 0644, 'file'
    #
    def chmod(mode, list, options={})
      fu_check_options options, OPT_TABLE['chmod']
      list = fu_list(list)
      fu_log sprintf('chmod %o %s', mode, list.join(' ')) if options[:verbose]
      mode = ('0%03o' % mode) if mode.is_a? Integer
      list.each { |dir|
        fu_put(dir, 'SETPERMISSION', {:permission => mode})
      }
    end
    OPT_TABLE['chmod'] = [:verbose]
    module_function :chmod

    # Public: Change an ownership of one or more directories/files.
    #
    # user - username
    # group - groupname
    # list - file/directory name or list of them
    # options - :verbose
    #
    # Examples
    #
    #   FileUtils.chmod 0755, 'dir'
    #   FileUtils.chmod 0644, 'file'
    #
    def chown(user, group, list, options={})
      fu_check_options options, OPT_TABLE['chown']
      list = fu_list(list)
      fu_log sprintf('chown %s%s',
                     [user,group].compact.join(':') + ' ',
                     list.join(' ')) if options[:verbose]
      list.each { |dir|
        fu_put(dir, 'SETOWNER', {:owner => user, :group => group})
      }
    end
    OPT_TABLE['chown'] = [:verbose]
    module_function :chown

    # Public: Set a replication factor of files
    #
    # list - file/directory name or list of them
    # num - replication factor
    # options - :verbose
    #
    # Examples
    #
    #   FileUtils.set_repl_factor 'file', 3
    #
    def set_repl_factor(list, num, options={})
      fu_check_options options, OPT_TABLE['set_repl_factor']
      list = fu_list(list)
      fu_log sprintf('set_repl_factor %s %d',
                     list.join(' '), num) if options[:verbose]
      list.each { |dir|
        fu_put(dir, 'SETREPLICATION', {:replication => num})
      }
    end
    OPT_TABLE['set_repl_factor'] = [:verbose]
    module_function :set_repl_factor

    # Public: Set an access time of files
    #
    # list - file/directory name or list of them
    # time - new access time
    # options - :verbose
    #
    # Examples
    #
    #   FileUtils.set_atime 'file', Time.now
    #
    def set_atime(list, time, options={})
      fu_check_options options, OPT_TABLE['set_atime']
      list = fu_list(list)
      time = time.to_i
      fu_log sprintf('set_atime %s %d', list.join(' '), time) if options[:verbose]
      list.each { |dir|
        fu_put(dir, 'SETTIMES', {:accesstime => time})
      }
    end
    OPT_TABLE['set_atime'] = [:verbose]
    module_function :set_atime

    # Public: Set a modification time of files
    #
    # list - file/directory name or list of them
    # time - new modification time
    # options - :verbose
    #
    # Examples
    #
    #   FileUtils.set_mtime 'file', Time.now
    #
    def set_mtime(list, time, options={})
      fu_check_options options, OPT_TABLE['set_mtime']
      list = fu_list(list)
      time = time.to_i
      fu_log sprintf('set_mtime %s %d', list.join(' '), time) if options[:verbose]
      list.each { |dir|
        fu_put(dir, 'SETTIMES', {:modificationtime => time})
      }
    end
    OPT_TABLE['set_mtime'] = [:verbose]
    module_function :set_mtime

    ##
    def self.private_module_function(name)
      module_function name
      private_class_method name
    end

    def fu_list(arg)
      [arg].flatten
    end
    private_module_function :fu_list

    def fu_put(path, op, params={}, payload='')
      url = "http://#{@fu_host}:#{@fu_port}/webhdfs/v1/#{path}"
      RestClient.put url, payload, :params => params.merge({:op => op})
    end
    private_module_function :fu_put

    def fu_delete(path, op, params={})
      url = "http://#{@fu_host}:#{@fu_port}/webhdfs/v1/#{path}"
      RestClient.delete url, :params => params.merge({:op => op})
    end
    private_module_function :fu_delete

    def fu_check_options(options, optdecl)
      h = options.dup
      optdecl.each do |opt|
        h.delete opt
      end
      raise ArgumentError, "no such option: #{h.keys.join(' ')}" unless h.empty?
    end
    private_module_function :fu_check_options

    @fileutils_output = $stderr
    @fileutils_label  = '[webhdfs]: '
    def fu_log(msg)
      @fileutils_output ||= $stderr
      @fileutils_label  ||= ''
      @fileutils_output.puts @fileutils_label + msg
    end
    private_module_function :fu_log
  end
end
