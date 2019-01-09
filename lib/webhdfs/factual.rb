require_relative '../webhdfs/client'

module WebHDFS
  module Factual
    class LOGGER
      def self.method_missing(m, *args, &block)
      end
    end

    class APIConnection
      def initialize(host, headers = {})
        @host = host
        @headers = headers
      end

      def connection
        @conn ||= Faraday.new(@host, headers: @headers) do |builder|
          builder.response :logger, LOGGER
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
          raise ExternalRequestError.new(res)
        end
      end

      def parse_response(res)
        begin
          JSON.parse(res.body)
        rescue JSON::ParserError => e
          res.body
        end
      end
    end

    class InnerClient
      def self.setup
        return false unless keytab_path_set?
        client = initialize_client
        return false unless client_working?(client)
        client
      end

      def self.keytab_path_set?
        if ENV['KEYTAB_PATH'].nil? || ENV['KEYTAB_PATH'].empty?
          LOGGER.info("KEYTAB_PATH not set.")
          false
        else
          true
        end
      end

      def self.initialize_client
        client = WebHDFS::Client.new(detect_namenode)
        client.kerberos = true
        client.kerberos_keytab = ENV['KEYTAB_PATH']
        client
      end

      def self.detect_namenode
        api = APIConnection.new(CONFIG.hdfs_dev_api)
        path = 'jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus'
        res = api.get(path)
        p res
        namenode = res['beans'].first['HostAndPort'].split(':').first
        p namenode
        namenode
      rescue StandardError => e
        LOGGER.warn("Failed to detect namenode with error: #{e}")
        LOGGER.warn("Defaulting to #{CONFIG.default_hdfs_namenode}")
        CONFIG.default_hdfs_namenode
      end

      def self.client_working?(client)
        res = client.stat('/')
        !!res
      rescue StandardError => e
        LOGGER.error("Failed to access HDFS with error #{$!}")
        if e.is_a?(NameError) && e.message =~ /undefined local variable or method `min_stat'/
          LOGGER.info("Do you have a valid keytab file at #{ENV['KEYTAB_PATH']}?")
        end
        raise e
      end
    end

    class Client
      def initialize
        @client = InnerClient.setup
      end

      def append(path, data)
        smart_retry do
          begin
            @client.stat(path)
            @client.append(path, data + "\n")
          rescue WebHDFS::FileNotFoundError => e
            @client.create(path, data + "\n")
          end
        end
      end

      def rm_r(path)
        rm_r!(path)
      rescue NeutronicHelper::FileNotFoundError => e
        nil
      end

      def rm_r!(path)
        smart_retry do
          begin
            @client.stat(path)
          rescue WebHDFS::FileNotFoundError => e
            raise NeutronicHelper::FileNotFoundError.new("File #{path} not found")
          end
          @client.delete(path, recursive: true)
        end
      end

      def ls(path)
        smart_retry do
          @client.list(path).map{ |f| f['pathSuffix'] }
        end
      end

      def mkdir(path)
        smart_retry do
          @client.mkdir(path, permission: '0775')
        end
      end

      def mv(paths, target_dir)
        paths.each do |path|
          smart_retry do
            @client.rename(path, File.join(target_dir, File.basename(path)))
          end
        end
      end

      def read(path)
        smart_retry do
          rescue_read_errors(path) do
            @client.read(path)
          end
        end
      end

      def tip_of_tail(path)
        read(path).split("\n").last || ''
      end

      def content(path)
        smart_retry do
          @client.content_summary(path)
        end
      end

      def mtime(path)
        smart_retry do
          begin
            modification_time = @client.stat(path)['modificationTime']
          rescue WebHDFS::FileNotFoundError => e
            raise NeutronicHelper::FileNotFoundError.new("File #{path} not found")
          end
          Time.at(modification_time / 1000)
        end
      end

      private

      def smart_retry(&block)
        block.call
      rescue WebHDFS::IOError => e
        specific_exception = JSON.parse(e.message)['RemoteException']['exception'] rescue nil
        message = JSON.parse(e.message)['RemoteException']['message'] rescue nil
        if specific_exception == 'StandbyException'
          LOGGER.error("HDFS namenode in standby. Sleeping for 10 seconds and then attempting to reconnect.")
          Kernel.sleep 10
          @client = InnerClient.setup
          block.call
        elsif message =~ /^Cannot obtain block length/
          LOGGER.error(e.message)
          Kernel.sleep 5
          block.call
        else
          raise
        end
      rescue WebHDFS::ServerError => e
        LOGGER.error(e.message)
        Kernel.sleep 15
        block.call
      rescue WebHDFS::KerberosError => e
        LOGGER.error("Kerberos credentials expired, refreshing them.")
        @client = InnerClient.setup
        block.call
      end

      def rescue_read_errors(path, &block)
        block.call
      rescue WebHDFS::FileNotFoundError => e
        begin
          message = JSON.parse(e.message)["RemoteException"]["message"]
        rescue StandardError => e2
          message = e2.message
        end
        if message =~ /not found/
          raise NeutronicHelper::FileNotFoundError, message, e.backtrace
        else
          raise InvalidOpError, message, e.backtrace
        end
      end
    end
  end
end
