require 'faraday'

module WebHDFS
  class APIConnection
    def initialize(host, headers = {})
      @host = host
      @headers = headers
    end

    def connection
      @conn ||= Faraday.new(@host, headers: @headers) do |builder|
        builder.response :logger, WebHDFS.logger
        builder.use Faraday::Adapter::NetHttp
      end
    end

    def post(path, body)
      res = connection.post do |req|
        req.url path
        req.body = body
        yield req if block_given?
      end
      handle_response(res)
    end

    def put(path)
      res = connection.put do |req|
        req.url path
        yield req if block_given?
      end
      handle_response(res)
    end

    def get(path, params = {})
      res = connection.get do |req|
        req.url path
        req.params.merge!(params)
      end
      handle_response(res)
    end

    def handle_response(res)
      if res.success?
        parse_response(res)
      else
        raise RequestError.new(res)
      end
    end

    def parse_response(res)
      begin
        JSON.parse(res.body)
      rescue JSON::ParserError
        res.body
      end
    end
  end
end
