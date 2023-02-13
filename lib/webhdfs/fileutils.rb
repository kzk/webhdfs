require_relative 'client'

module WebHDFS
  module FileUtils
    # Those values hold NameNode location
    @fu_host = 'localhost'
    @fu_port = 50070
    @fu_user = nil
    @fu_doas = nil
    @fu_httpfs_mode = false
    @fu_ssl = false
    @fu_ssl_ca_file = nil
    @fu_ssl_verify_mode = nil
    @fu_kerberos = false

    class << self
      attr_accessor :fu_host, :fu_port, :fu_user, :fu_doas, :fu_paddr, :fu_pport, :fu_httpfs_mode, :fu_ssl, :fu_ssl_ca_file, :fu_ssl_verify_mode, :fu_kerberos

      # Public: Set hostname and port number of WebHDFS
      #
      # host - hostname
      # port - port
      # user - username
      # doas - proxy user name
      # proxy_address - address of the net http proxy to use
      # proxy_port - port of the net http proxy to use
      #
      # Examples
      #
      #   FileUtils.set_server 'localhost', 50070
      #
      def set_server(host, port, user=nil, doas=nil, proxy_address=nil, proxy_port=nil)
        self.fu_host = host
        self.fu_port = port
        self.fu_user = user
        self.fu_doas = doas
        self.fu_paddr = proxy_address
        self.fu_pport = proxy_port
      end

      # Public: Set httpfs mode enable/disable
      #
      # mode - boolean (default true)
      #
      # Examples
      #
      #   FileUtils.set_httpfs_mode
      #
      def set_httpfs_mode(mode=true)
        self.fu_httpfs_mode = mode
      end

      # Public: Set ssl enable/disable
      #
      # mode - boolean (default true)
      #
      # Examples
      #
      #   FileUtils.set_ssl
      #
      def set_ssl(mode=true)
        self.fu_ssl = mode
      end

      # Public: Set ssl ca_file
      #
      # ca_file - string
      #
      # Examples
      #
      #   FileUtils.set_ca_file("/path/to/ca_file.pem")
      #
      def set_ssl_ca_file(ca_file)
        self.fu_ssl_ca_file = ca_file
      end

      # Public: Set ssl verify mode
      #
      # mode - :none or :peer
      #
      # Examples
      #
      #   FileUtils.set_ssl_verify_mode(:peer)
      #
      def set_ssl_verify_mode(mode)
        self.fu_ssl_verify_mode = mode
      end

      # Public: Set kerberos authentication enable/disable
      def set_kerberos_auth(mode=true)
      # mode - boolean (default true)
      #
      # Examples
      #
      #   FileUtils.set_kerberos
      #
      def set_kerberos(mode=true)
        self.fu_kerberos = mode
      end
    end
  end
end
