require 'webmock/rspec'
WebMock.disable_net_connect!(allow: [/\/localhost/, /\/hdfs-dev\./, /\/webhdfs\//])

API_HOST = ENV['API_HOST'] || 'http://localhost'
DEFAULT_NAMENODE = ENV['DEFAULT_NAMENODE'] || 'localhost'

def fixture_path
  File.expand_path("./spec/fixtures")
end

def fixture(file)
  File.new(fixture_path + '/' + file)
end

Dir["./spec/shared_examples/**/*.rb"].sort.each { |f| require f }
