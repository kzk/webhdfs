require 'net/http'
require 'uri'
require 'json'

require_relative 'exceptions'

module WebHDFS
  class ClientV1

    # This hash table holds command options.
    OPT_TABLE = {} # internal use only

    attr_accessor :host, :port, :username, :doas
    attr_accessor :open_timeout, :read_timeout

    def initialize(host='localhost', port=50070, username=nil, doas=nil)
      @host = host
      @port = port
      @username = username
      @doas = doas
    end

    # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
    #                 [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
    #                 [&permission=<OCTAL>][&buffersize=<INT>]"
    def create(path, body, options={})
      check_options(options, OPT_TABLE['CREATE'])
      res = operate_requests('PUT', path, 'CREATE', options, body)
      res.code == '201'
    end
    OPT_TABLE['CREATE'] = ['overwrite', 'blocksize', 'replication', 'permission', 'buffersize']

    # curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
    def append(path, body, options={})
      check_options(options, OPT_TABLE['APPEND'])
      res = operate_requests('POST', path, 'APPEND', options, body)
      res.code == '200'
    end
    OPT_TABLE['APPEND'] = ['buffersize']

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

      res.code == '200' and res.content_type == 'application/json' and JSON.parse(res.body)['boolean']
    end
    OPT_TABLE['MKDIRS'] = ['permission']
    alias :mkdirs :mkdir

    def rename(path, dest, options={})
    end

    def delete(path, options={}) # options['recursive']
    end

    def status(path, options={}) # GETFILESTATUS
    end

    def list(path, options={}) # LISTSTATUS
    end

    def content_summary(path, options={}) # GETCONTENTSUMMARY
      raise NotImplementedError
    end

    def checksum(path, options={}) # GETFILECHECKSUM
      raise NotImplementedError
    end

    def homedir(options={}) # GETHOMEDIRECTORY
      raise NotImplementedError
    end

    def chmod(path, options={}) # SETPERMISSION
      raise NotImplementedError
    end

    def chown(path, options={}) # SETOWNER
      raise NotImplementedError
    end

    def replication(path, option={}) # SETREPLICATION
      raise NotImplementedError
    end

    def touch(path, options={}) # SETTIMES
      raise NotImplementedError
    end

    def delegation_token(user, options={}) # GETDELEGATIONTOKEN
      raise NotImplementedError
    end
    def renew_delegation_token(token, options={}) # RENEWDELEGATIONTOKEN
      raise NotImplementedError
    end
    def cancel_delegation_token(token, options={}) # CANCELDELEGATIONTOKEN
      raise NotImplementedError
    end

    def check_options(options, optdecl)
      ex = options.keys - optdecl
      raise ArgumentError, "no such option: #{ex.keys.join(' ')}" unless ex.empty?
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

    REDIRECTED_OPERATIONS = ['APPEND', 'CREATE', 'OPEN']
    def operate_requests(method, path, op, params={}, payload=nil)
      if REDIRECTED_OPERATIONS.include?(op)
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
        request(uri.host, uri.port, method, rpath, nil, {}, payload)
      else
        request(@host, @port, method, path, op, params, nil)
      end
    end

    # IllegalArgumentException      400 Bad Request
    # UnsupportedOperationException 400 Bad Request
    # SecurityException             401 Unauthorized
    # IOException                   403 Forbidden
    # FileNotFoundException         404 Not Found
    # RumtimeException              500 Internal Server Error
    def request(host, port, method, path, op=nil, params={}, payload=nil)
      conn = Net::HTTP.start(host, port)
      conn.open_timeout = @open_timeout if @open_timeout
      conn.read_timeout = @read_timeout if @read_timeout

      request_path = if op
                       build_path(path, op, params)
                     else
                       path
                     end

      p({:host => host, :port => port, :method => method, :path => request_path})
      res = conn.send_request(method, request_path, payload)

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
