require_relative './factual/null_logger'

module WebHDFS
  module Factual
    class << self
      attr_accessor :logger
    end

    self.logger = WebHDFS::Factual::NullLogger.new
  end
end

require_relative '../webhdfs/client'
require_relative './factual/api_connection'
require_relative './factual/client'
