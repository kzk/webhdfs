module WebHDFS
  Error = Class.new(StandardError)

  class FileNotFoundError < Error; end
  class IOError < Error; end
  class SecurityError < Error; end

  class ClientError < Error; end
  class ServerError < Error; end

  class RequestFailedError < Error; end

  class KerberosError < Error; end
end
