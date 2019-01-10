require 'logger'

module WebHDFS
  module Factual
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
  end
end
