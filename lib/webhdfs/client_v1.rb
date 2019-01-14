require 'net/http'
require 'uri'
require 'json'
require 'addressable/uri'

require_relative 'exceptions'

module WebHDFS
  class ClientV1

    # This hash table holds command options.
    OPT_TABLE = {} # internal use only
    KNOWN_ERRORS = ['LeaseExpiredException'].freeze
    DEFAULT_PORT = 50070

    attr_accessor :host, :port, :username, :doas, :proxy_address, :proxy_port
    attr_accessor :proxy_user, :proxy_pass
    attr_accessor :open_timeout # default 30s (in ruby net/http)
    attr_accessor :read_timeout # default 60s (in ruby net/http)
    attr_accessor :httpfs_mode
    attr_accessor :retry_known_errors # default false (not to retry)
    attr_accessor :retry_times        # default 1 (ignored when retry_known_errors is false)
    attr_accessor :retry_interval     # default 1 ([sec], ignored when retry_known_errors is false)
    attr_accessor :ssl
    attr_accessor :ssl_ca_file
    attr_reader   :ssl_verify_mode
    attr_accessor :ssl_cert
    attr_accessor :ssl_key
    attr_accessor :ssl_version
    attr_accessor :kerberos, :kerberos_keytab
    attr_accessor :http_headers
    attr_accessor :jmx_host

    SSL_VERIFY_MODES = [:none, :peer]
    def ssl_verify_mode=(mode)
      unless SSL_VERIFY_MODES.include? mode
        raise ArgumentError, "Invalid SSL verify mode #{mode.inspect}"
      end
      @ssl_verify_mode = mode
    end

    def initialize(host='localhost', port=50070, username=nil, doas=nil, proxy_address=nil, proxy_port=nil, http_headers={})
      @host = host
      @port = port
      @username = username
      @doas = doas
      @proxy_address = proxy_address
      @proxy_port = proxy_port

      @jmx_host = nil

      @retry_known_errors = false
      @retry_times = 1
      @retry_interval = 1

      @httpfs_mode = false

      @ssl = false
      @ssl_ca_file = nil
      @ssl_verify_mode = nil
      @ssl_cert = nil
      @ssl_key = nil
      @ssl_version = nil

      @kerberos = false
      @kerberos_keytab = nil
      @http_headers = http_headers

      if block_given?
        Proc.new.call(self)
      end
    end

    def keytab_path_set?
      if @kerberos_keytab.nil? || @kerberos_keytab.empty?
        WebHDFS.logger.fatal("The kerberos keytab is not set")
        false
      else
        true
      end
    end

    def ensure_keytab_path_set
      raise WebHDFS::KerberosError, "The kerberos keytab must be set" unless keytab_path_set?
    end

    def operational?
      res = stat('/')
      !!res
    rescue StandardError => e
      WebHDFS.logger.error("Failed to access HDFS with error #{$!}")
      if e.is_a?(NameError) && e.message =~ /undefined local variable or method `min_stat'/
        WebHDFS.logger.info("Do you have a valid keytab file at #{@kerberos_keytab}?")
      end
      raise e
    end

    def ensure_operational
      raise WebHDFS::Error, "The client isn't working" unless operational?
    end

    def get_namenode_from_jmx
      if @jmx_host.nil? || @jmx_host.empty?
        raise WebHDFS::JMXError, "JMX host is not set"
      end

      conn = WebHDFS::APIConnection.new(@jmx_host)
      path = 'jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus'
      res = conn.get(path)

      res['beans'].first['HostAndPort'].split(':').first
    end

    def set_namenode_from_jmx
      @host = get_namenode_from_jmx
    rescue StandardError => e
      WebHDFS.logger.warn("Failed to detect namenode with error: #{e}")
      WebHDFS.logger.warn("Remaining on #{@host}")
    end

    # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
    #                 [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
    #                 [&permission=<OCTAL>][&buffersize=<INT>]"
    def create(path, body, options={})
      if @httpfs_mode
        options = options.merge({'data' => 'true'})
      end
      check_options(options, OPT_TABLE['CREATE'])
      res = operate_requests('PUT', path, 'CREATE', options, body)
      res.code == '201'
    end
    OPT_TABLE['CREATE'] = ['overwrite', 'blocksize', 'replication', 'permission', 'buffersize', 'data']

    # curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
    def append(path, body, options={})
      if @httpfs_mode
        options = options.merge({'data' => 'true'})
      end
      check_options(options, OPT_TABLE['APPEND'])
      res = operate_requests('POST', path, 'APPEND', options, body)
      res.code == '200'
    end
    OPT_TABLE['APPEND'] = ['buffersize', 'data']

    # curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
    #                [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
    def read(path, options={})
      check_options(options, OPT_TABLE['OPEN'])
      res = operate_requests('GET', path, 'OPEN', options)
      res.body
    end
    OPT_TABLE['OPEN'] = ['offset', 'length', 'buffersize']
    alias :open :read

    # curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
    def mkdir(path, options={})
      check_options(options, OPT_TABLE['MKDIRS'])
      res = operate_requests('PUT', path, 'MKDIRS', options)
      check_success_json(res, 'boolean')
    end
    OPT_TABLE['MKDIRS'] = ['permission']
    alias :mkdirs :mkdir

    # curl -i -X PUT "<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>"
    def rename(path, dest, options={})
      check_options(options, OPT_TABLE['RENAME'])
      unless dest.start_with?('/')
        dest = '/' + dest
      end
      res = operate_requests('PUT', path, 'RENAME', options.merge({'destination' => dest}))
      check_success_json(res, 'boolean')
    end

    # curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
    #                          [&recursive=<true|false>]"
    def delete(path, options={})
      check_options(options, OPT_TABLE['DELETE'])
      res = operate_requests('DELETE', path, 'DELETE', options)
      check_success_json(res, 'boolean')
    end
    OPT_TABLE['DELETE'] = ['recursive']

    # curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"
    def stat(path, options={})
      check_options(options, OPT_TABLE['GETFILESTATUS'])
      res = operate_requests('GET', path, 'GETFILESTATUS', options)
      check_success_json(res, 'FileStatus')
    end
    alias :getfilestatus :stat

    # curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"
    def list(path, options={})
      check_options(options, OPT_TABLE['LISTSTATUS'])
      res = operate_requests('GET', path, 'LISTSTATUS', options)
      check_success_json(res, 'FileStatuses')['FileStatus']
    end
    alias :liststatus :list

    # curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETCONTENTSUMMARY"
    def content_summary(path, options={})
      check_options(options, OPT_TABLE['GETCONTENTSUMMARY'])
      res = operate_requests('GET', path, 'GETCONTENTSUMMARY', options)
      check_success_json(res, 'ContentSummary')
    end
    alias :getcontentsummary :content_summary

    # curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM"
    def checksum(path, options={})
      check_options(options, OPT_TABLE['GETFILECHECKSUM'])
      res = operate_requests('GET', path, 'GETFILECHECKSUM', options)
      check_success_json(res, 'FileChecksum')
    end
    alias :getfilechecksum :checksum

    # curl -i "http://<HOST>:<PORT>/webhdfs/v1/?op=GETHOMEDIRECTORY"
    def homedir(options={})
      check_options(options, OPT_TABLE['GETHOMEDIRECTORY'])
      res = operate_requests('GET', '/', 'GETHOMEDIRECTORY', options)
      check_success_json(res, 'Path')
    end
    alias :gethomedirectory :homedir

    # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETPERMISSION
    #                 [&permission=<OCTAL>]"
    def chmod(path, mode, options={})
      check_options(options, OPT_TABLE['SETPERMISSION'])
      res = operate_requests('PUT', path, 'SETPERMISSION', options.merge({'permission' => mode}))
      res.code == '200'
    end
    alias :setpermission :chmod

    # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETOWNER
    #                          [&owner=<USER>][&group=<GROUP>]"
    def chown(path, options={})
      check_options(options, OPT_TABLE['SETOWNER'])
      unless options.has_key?('owner') or options.has_key?('group') or
          options.has_key?(:owner) or options.has_key?(:group)
        raise ArgumentError, "'chown' needs at least one of owner or group"
      end
      res = operate_requests('PUT', path, 'SETOWNER', options)
      res.code == '200'
    end
    OPT_TABLE['SETOWNER'] = ['owner', 'group']
    alias :setowner :chown

    # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETREPLICATION
    #                           [&replication=<SHORT>]"
    def replication(path, replnum, options={})
      check_options(options, OPT_TABLE['SETREPLICATION'])
      res = operate_requests('PUT', path, 'SETREPLICATION', options.merge({'replication' => replnum.to_s}))
      check_success_json(res, 'boolean')
    end
    alias :setreplication :replication

    # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETTIMES
    #                           [&modificationtime=<TIME>][&accesstime=<TIME>]"
    # motidicationtime: radix-10 logn integer
    # accesstime: radix-10 logn integer
    def touch(path, options={})
      check_options(options, OPT_TABLE['SETTIMES'])
      unless options.has_key?('modificationtime') or options.has_key?('accesstime') or
          options.has_key?(:modificationtime) or options.has_key?(:accesstime)
        raise ArgumentError, "'chown' needs at least one of modificationtime or accesstime"
      end
      res = operate_requests('PUT', path, 'SETTIMES', options)
      res.code == '200'
    end
    OPT_TABLE['SETTIMES'] = ['modificationtime', 'accesstime']
    alias :settimes :touch

    # def delegation_token(user, options={}) # GETDELEGATIONTOKEN
    #   raise NotImplementedError
    # end
    # def renew_delegation_token(token, options={}) # RENEWDELEGATIONTOKEN
    #   raise NotImplementedError
    # end
    # def cancel_delegation_token(token, options={}) # CANCELDELEGATIONTOKEN
    #   raise NotImplementedError
    # end

    def check_options(options, optdecl=[])
      ex = options.keys.map(&:to_s) - (optdecl || [])
      raise ArgumentError, "no such option: #{ex.join(' ')}" unless ex.empty?
    end

    def check_success_json(res, attr=nil)
      res.code == '200' and res.content_type == 'application/json' and (attr.nil? or JSON.parse(res.body)[attr])
    end

    def api_path(path)
      if path.start_with?('/')
        '/webhdfs/v1' + path
      else
        '/webhdfs/v1/' + path
      end
    end

    def build_path(path, op, params)
      opts = if @username and @doas
               {'op' => op, 'user.name' => @username, 'doas' => @doas}
             elsif @username
               {'op' => op, 'user.name' => @username}
             elsif @doas
               {'op' => op, 'doas' => @doas}
             else
               {'op' => op}
             end
      query = URI.encode_www_form(params.merge(opts))
      api_path(path) + '?' + query
    end

    REDIRECTED_OPERATIONS = ['APPEND', 'CREATE', 'OPEN', 'GETFILECHECKSUM']
    def operate_requests(method, path, op, params={}, payload=nil)
      if not @httpfs_mode and REDIRECTED_OPERATIONS.include?(op)
        res = request(@host, @port, method, path, op, params, nil)
        unless res.is_a?(Net::HTTPRedirection) and res['location']
          msg = "NameNode returns non-redirection (or without location header), code:#{res.code}, body:#{res.body}."
          raise WebHDFS::RequestFailedError, msg
        end
        uri = URI.parse(res['location'])
        rpath = if uri.query
                  uri.path + '?' + uri.query
                else
                  uri.path
                end
        request(uri.host, uri.port, method, rpath, nil, {}, payload, {'Content-Type' => 'application/octet-stream'})
      else
        if @httpfs_mode and not payload.nil?
          request(@host, @port, method, path, op, params, payload, {'Content-Type' => 'application/octet-stream'})
        else
          request(@host, @port, method, path, op, params, payload)
        end
      end
    end

    # IllegalArgumentException      400 Bad Request
    # UnsupportedOperationException 400 Bad Request
    # SecurityException             401 Unauthorized
    # IOException                   403 Forbidden
    # FileNotFoundException         404 Not Found
    # RumtimeException              500 Internal Server Error
    def request(host, port, method, path, op=nil, params={}, payload=nil, header=nil, retries=0)
      conn = Net::HTTP.new(host, port, @proxy_address, @proxy_port)
      conn.proxy_user = @proxy_user if @proxy_user
      conn.proxy_pass = @proxy_pass if @proxy_pass
      conn.open_timeout = @open_timeout if @open_timeout
      conn.read_timeout = @read_timeout if @read_timeout

      path = Addressable::URI.escape(path) # make path safe for transmission via HTTP
      request_path = if op
                       build_path(path, op, params)
                     else
                       path
                     end
      if @ssl
        conn.use_ssl = true
        conn.ca_file = @ssl_ca_file if @ssl_ca_file
        if @ssl_verify_mode
          require 'openssl'
          conn.verify_mode = case @ssl_verify_mode
                             when :none then OpenSSL::SSL::VERIFY_NONE
                             when :peer then OpenSSL::SSL::VERIFY_PEER
                             end
        end
        conn.cert = @ssl_cert if @ssl_cert
        conn.key = @ssl_key if @ssl_key
        conn.ssl_version = @ssl_version if @ssl_version
      end

      gsscli = nil
      if @kerberos
        require 'base64'
        require 'gssapi'
        gsscli = GSSAPI::Simple.new(@host, 'HTTP', @kerberos_keytab)
        token = nil
        begin
          token = gsscli.init_context
        rescue => e
          raise WebHDFS::KerberosError, e.message
        end
        if header
          header['Authorization'] = "Negotiate #{Base64.strict_encode64(token)}"
        else
          header = {'Authorization' => "Negotiate #{Base64.strict_encode64(token)}"}
        end
      else
        header = {} if header.nil?
        header = @http_headers.merge(header)
      end

      res = nil
      if !payload.nil? and payload.respond_to? :read and payload.respond_to? :size
        req = Net::HTTPGenericRequest.new(method,(payload ? true : false),true,request_path,header)
        raise WebHDFS::ClientError, 'Error accepting given IO resource as data payload, Not valid in methods other than PUT and POST' unless (method == 'PUT' or method == 'POST')

        req.body_stream = payload
        req.content_length = payload.size
        begin
          res = conn.request(req)
        rescue => e
          raise WebHDFS::ServerError, "Failed to connect to host #{host}:#{port}, #{e.message}"
        end
      else
        begin
          res = conn.send_request(method, request_path, payload, header)
        rescue => e
          raise WebHDFS::ServerError, "Failed to connect to host #{host}:#{port}, #{e.message}"
        end
      end

      if @kerberos and res.code == '307'
        itok = (res.header.get_fields('WWW-Authenticate') || ['']).pop.split(/\s+/).last
        unless itok
          raise WebHDFS::KerberosError, 'Server does not return WWW-Authenticate header'
        end

        begin
          gsscli.init_context(Base64.strict_decode64(itok))
        rescue => e
          raise WebHDFS::KerberosError, e.message
        end
      end

      case res
      when Net::HTTPSuccess
        res
      when Net::HTTPRedirection
        res
      else
        message = if res.body and not res.body.empty?
                    res.body.gsub(/\n/, '')
                  else
                    'Response body is empty...'
                  end

        if @retry_known_errors && retries < @retry_times
          detail = nil
          if message =~ /^\{"RemoteException":\{/
            begin
              detail = JSON.parse(message)
            rescue
              # ignore broken json response body
            end
          end
          if detail && detail['RemoteException'] && KNOWN_ERRORS.include?(detail['RemoteException']['exception'])
            sleep @retry_interval if @retry_interval > 0
            return request(host, port, method, path, op, params, payload, header, retries+1)
          end
        end

        case res.code
        when '400'
          raise WebHDFS::ClientError, message
        when '401'
          raise WebHDFS::SecurityError, message
        when '403'
          raise WebHDFS::IOError, message
        when '404'
          raise WebHDFS::FileNotFoundError, message
        when '500'
          raise WebHDFS::ServerError, message
        else
          raise WebHDFS::RequestFailedError, "response code:#{res.code}, message:#{message}"
        end
      end
    end

    def retry_request(*args)
      sleep @retry_interval if @retry_interval > 0
      return request(*args)
    end

    def failover_occurred?(error)
      specific_exception = error['RemoteException']['exception'] rescue nil
      specific_exception == 'StandbyException'
    end

    def handle_failover(*args)
      WebHDFS.logger.error("HDFS namenode in standby. Obtaining active from JMX and retrying.")
      sleep @retry_interval if @retry_interval > 0
      set_namenode_from_jmx
      ensure_operational
      request(*args)
    end

    def cannot_obtain_block_length?(error)
      message = error['RemoteException']['message'] rescue nil
      message =~ /^Cannot obtain block length/
    end

    def smart_retry(&block)
      block.call
    rescue WebHDFS::IOError => e
      specific_exception = JSON.parse(e.message)['RemoteException']['exception'] rescue nil
      message = JSON.parse(e.message)['RemoteException']['message'] rescue nil
      if specific_exception == 'StandbyException'
        WebHDFS.logger.error("HDFS namenode in standby. Sleeping for 10 seconds and then attempting to reconnect.")
        Kernel.sleep 10
        set_namenode_from_jmx
        ensure_operational
        block.call
      elsif message =~ /^Cannot obtain block length/
        WebHDFS.logger.error(e.message)
        Kernel.sleep 5
        block.call
      else
        raise
      end
    rescue WebHDFS::ServerError => e
      WebHDFS.logger.error(e.message)
      Kernel.sleep 15
      block.call
    rescue WebHDFS::KerberosError => e
      WebHDFS.logger.error("Kerberos credentials expired, refreshing them.")
      set_namenode_from_jmx
      ensure_operational
      block.call
    end

    def rescue_read_errors(path, &block)
      block.call
    rescue WebHDFS::FileNotFoundError => e
      begin
        message = JSON.parse(e.message)["RemoteException"]["message"]
      rescue StandardError
        message = e.message
      end
      if message =~ /not found/
        # raise NeutronicHelper::FileNotFoundError, message, e.backtrace
        raise WebHDFS::FileNotFoundError, message, e.backtrace
      else
        raise InvalidOpError, message, e.backtrace
      end
    end

    public

    # TODO: The following are all HDFS methods.
    # TODO: Use the CREATE op with overwrite=false (this is the default)
    # https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File
    # https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Overwrite
    # Follow it by append regardless of outcome
    def _append(path, data)
      smart_retry do
        begin
          stat(path)
          append(path, data + "\n")
        rescue WebHDFS::FileNotFoundError
          create(path, data + "\n")
        end
      end
    end

    def _rm_r(path)
      _rm_r!(path)
    # rescue NeutronicHelper::FileNotFoundError
    rescue WebHDFS::FileNotFoundError
      nil
    end

    def _rm_r!(path)
      smart_retry do
        begin
          stat(path)
        rescue WebHDFS::FileNotFoundError => e
          # raise NeutronicHelper::FileNotFoundError, "File #{path} not found"
          raise WebHDFS::FileNotFoundError, "File #{path} not found", e.backtrace
        end
        delete(path, recursive: true)
      end
    end

    # TODO: Rename this to list_filenames
    def _ls(path)
      smart_retry do
        list(path).map{ |f| f['pathSuffix'] }
      end
    end

    def _mkdir(path)
      smart_retry do
        mkdir(path, permission: '0775')
      end
    end

    def _mv(paths, target_dir)
      paths.each do |path|
        smart_retry do
          rename(path, File.join(target_dir, File.basename(path)))
        end
      end
    end

    def _read(path)
      smart_retry do
        rescue_read_errors(path) do
          read(path)
        end
      end
    end

    def _tip_of_tail(path)
      _read(path).split("\n").last || ''
    end

    def _content(path)
      smart_retry do
        content_summary(path)
      end
    end

    def _mtime(path)
      smart_retry do
        begin
          modification_time = stat(path)['modificationTime']
        rescue WebHDFS::FileNotFoundError => e
          # raise NeutronicHelper::FileNotFoundError, "File #{path} not found"
          raise WebHDFS::FileNotFoundError, "File #{path} not found", e.backtrace
        end
        Time.at(modification_time / 1000)
      end
    end
  end
end
