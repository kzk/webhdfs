require 'net/http'
require 'uri'
require 'json'

require_relative 'exceptions'

module WebHDFS
  class ClientV1

    # This hash table holds command options.
    OPT_TABLE = {} # internal use only
    KNOWN_ERRORS = ['LeaseExpiredException'].freeze

    attr_accessor :host, :port, :username, :doas
    attr_accessor :open_timeout # default 30s (in ruby net/http)
    attr_accessor :read_timeout # default 60s (in ruby net/http)
    attr_accessor :httpfs_mode
    attr_accessor :retry_known_errors # default false (not to retry)
    attr_accessor :retry_times        # default 1 (ignored when retry_known_errors is false)
    attr_accessor :retry_interval     # default 1 ([sec], ignored when retry_known_errors is false)

    def initialize(host='localhost', port=50070, username=nil, doas=nil)
      @host = host
      @port = port
      @username = username
      @doas = doas
      @retry_known_errors = false
      @retry_times = 1
      @retry_interval = 1

      @httpfs_mode = false
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
      conn = Net::HTTP.new(host, port)
      conn.open_timeout = @open_timeout if @open_timeout
      conn.read_timeout = @read_timeout if @read_timeout

      request_path = if op
                       build_path(path, op, params)
                     else
                       path
                     end

      res = conn.send_request(method, request_path, payload, header)

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
          if message =~ /^\{\\"RemoteException\\":\\\{/
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
  end
end
