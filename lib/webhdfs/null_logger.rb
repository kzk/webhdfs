require 'logger'

module WebHDFS
  class << self
    attr_accessor :logger
  end

  class NullLogger
    def debug(*args)
      false
    end

    def error(*args)
      false
    end

    def fatal(*args)
      false
    end

    def info(*args)
      false
    end

    def warn(*args)
      false
    end
  end

  self.logger = NullLogger.new
end
