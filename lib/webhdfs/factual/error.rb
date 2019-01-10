module WebHDFS
  module Factual
    class RequestError < Error
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
  end
end
