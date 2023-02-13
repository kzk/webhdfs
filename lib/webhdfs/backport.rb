unless RUBY_VERSION >= '2.0.0'
  require 'cgi'

  module URI
    def self.encode_www_form(enum)
      enum.map do |k,v|
        if v.nil?
          CGI.escape(k)
        elsif v.respond_to?(:to_ary)
          v.to_ary.map do |w|
            str = CGI.escape(k)
            unless w.nil?
              str << '='
              str << CGI.escape(w)
            end
          end.join('&')
        else
          str = CGI.escape(k.to_s)
          str << '='
          str << CGI.escape(v.to_s)
        end
      end.join('&')
    end
  end
end
