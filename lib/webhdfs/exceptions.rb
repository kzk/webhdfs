module WebHDFS; end
class WebHDFS::Error < StandardError; end

class WebHDFS::FileNotFoundError < WebHDFS::Error; end

class WebHDFS::IOError < WebHDFS::Error; end
class WebHDFS::SecurityError < WebHDFS::Error; end

class WebHDFS::ClientError < WebHDFS::Error; end
class WebHDFS::ServerError < WebHDFS::Error; end

class WebHDFS::RequestFailedError < WebHDFS::Error; end

class WebHDFS::JMXError < StandardError; end
class WebHDFS::KerberosError < WebHDFS::Error; end

class WebHDFS::RequestError < WebHDFS::Error
  attr_accessor :response, :metadata

  def initialize(response)
    @response = response
    @metadata = {
      headers: response.headers,
      body: response.body
    }
    message = default_message(response)
    super(message)
  end

  private

  def default_message(response)
    "Request failed: #{response.env.method.upcase} #{response.status} #{response.env.url}"
  end
end
