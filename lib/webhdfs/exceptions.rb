module WebHDFS
  class FileNotFoundError < StandardError; end

  class IOError < StandardError; end
  class SecurityError < StandardError; end

  class ClientError < StandardError; end
  class ServerError < StandardError; end

  class RequestFailedError < StandardError; end
end
