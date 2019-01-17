require_relative 'client_v1'

module WebHDFS
  class Simple
    attr_accessor :jmx_host
    attr_reader :raw

    def initialize(opts = {})
      @jmx_host = opts[:jmx_host]
      @raw = ClientV1.new

      if block_given?
        Proc.new.call(@raw)
      end
    end

    def self.keytab_path_set?
      if ENV['KEYTAB_PATH'].nil? || ENV['KEYTAB_PATH'].empty?
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
      res = @raw.stat('/')
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

    def get_host_from_jmx
      if @jmx_host.nil? || @jmx_host.empty?
        raise WebHDFS::JMXError, "JMX host is not set"
      end

      conn = WebHDFS::APIConnection.new(@jmx_host)
      path = 'jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus'
      res = conn.get(path)

      res['beans'].first['HostAndPort'].split(':').first
    end

    def set_host_from_jmx
      @raw.host = get_host_from_jmx
    rescue StandardError => e
      WebHDFS.logger.warn("Failed to detect namenode with error: #{e}")
      WebHDFS.logger.warn("Remaining on #{@raw.host}")
    end

    def append(path, data)
      smart_retry do
        begin
          @raw.stat(path)
          @raw.append(path, data + "\n")
        rescue WebHDFS::FileNotFoundError
          @raw.create(path, data + "\n")
        end
      end
    end

    def rm_r(path)
      rm_r!(path)
    rescue WebHDFS::FileNotFoundError
      nil
    end

    def rm_r!(path)
      smart_retry do
        begin
          @raw.stat(path)
        rescue WebHDFS::FileNotFoundError => e
          raise WebHDFS::FileNotFoundError, "File #{path} not found", e.backtrace
        end
        @raw.delete(path, recursive: true)
      end
    end

    def ls(path)
      smart_retry do
        @raw.list(path).map{ |f| f['pathSuffix'] }
      end
    end

    def mkdir(path)
      smart_retry do
        @raw.mkdir(path)
      end
    end

    def mv(paths, target_dir)
      paths.each do |path|
        smart_retry do
          @raw.rename(path, File.join(target_dir, File.basename(path)))
        end
      end
    end

    def read(path)
      smart_retry do
        rescue_read_errors(path) do
          @raw.read(path)
        end
      end
    end

    def tip_of_tail(path)
      read(path).split("\n").last || ''
    end

    def content(path)
      smart_retry do
        @raw.content_summary(path)
      end
    end

    def mtime(path)
      smart_retry do
        begin
          modification_time = @raw.stat(path)['modificationTime']
        rescue WebHDFS::FileNotFoundError => e
          raise WebHDFS::FileNotFoundError, "File #{path} not found", e.backtrace
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
        WebHDFS.logger.error("HDFS namenode in standby. Sleeping for 10 seconds and then attempting to reconnect.")
        Kernel.sleep 10
        set_host_from_jmx
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
      puts "RETRYING"
      WebHDFS.logger.error(e.message)
      Kernel.sleep 15
      set_host_from_jmx
      ensure_operational
      block.call
    rescue WebHDFS::KerberosError => e
      WebHDFS.logger.error("Kerberos credentials expired, refreshing them.")
      set_host_from_jmx
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
        raise WebHDFS::FileNotFoundError, message, e.backtrace
      else
        raise InvalidOpError, message, e.backtrace
      end
    end
  end
end
