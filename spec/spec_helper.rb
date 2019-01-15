require 'webmock/rspec'
WebMock.disable_net_connect!(allow: [/\/localhost/, /\/hdfs-dev\./, /\/webhdfs\//])

require_relative '../test/environment'

def fixture_path
  File.expand_path("./spec/fixtures")
end

def fixture(file)
  File.new(fixture_path + '/' + file)
end
