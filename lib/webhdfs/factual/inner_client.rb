module WebHDFS
  module Factual
    class InnerClient
      def self.setup(api = 'localhost', default_namenode = 'localhost')
        return false unless keytab_path_set?
        client = initialize_client(api, default_namenode)
        return false unless client_working?(client)
        client
      end

      def self.keytab_path_set?
        if ENV['KEYTAB_PATH'].nil? || ENV['KEYTAB_PATH'].empty?
          WebHDFS::Factual.logger.info("KEYTAB_PATH not set.")
          false
        else
          true
        end
      end

      def self.initialize_client(api, default_namenode)
        client = WebHDFS::Client.new(detect_namenode(api, default_namenode))
        client.kerberos = true
        client.kerberos_keytab = ENV['KEYTAB_PATH']
        client
      end

      def self.detect_namenode(api, default_namenode)
        api = WebHDFS::Factual::APIConnection.new(api)
        path = 'jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus'
        res = api.get(path)
        p res
        namenode = res['beans'].first['HostAndPort'].split(':').first
        p namenode
        namenode
      rescue StandardError => e
        WebHDFS::Factual.logger.warn("Failed to detect namenode with error: #{e}")
        WebHDFS::Factual.logger.warn("Defaulting to #{default_namenode}")
        default_namenode
      end

      def self.client_working?(client)
        res = client.stat('/')
        !!res
      rescue StandardError => e
        WebHDFS::Factual.logger.error("Failed to access HDFS with error #{$!}")
        if e.is_a?(NameError) && e.message =~ /undefined local variable or method `min_stat'/
          WebHDFS::Factual.logger.info("Do you have a valid keytab file at #{ENV['KEYTAB_PATH']}?")
        end
        raise e
      end
    end
  end
end
