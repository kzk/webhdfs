require 'webmock/rspec'
WebMock.disable_net_connect!(allow: [/\/localhost/, /\/hdfs-dev\./, /\/webhdfs\//])

JMX = ENV['JMX'] || 'http://localhost'
DEFAULT_NAMENODE = ENV['DEFAULT_NAMENODE'] || 'localhost'
TEST_PATH = ENV['TEST_PATH']

if TEST_PATH.nil? || TEST_PATH.empty?
  raise "Must explicitly set a TEST_PATH to run tests within"
end

def fixture_path
  File.expand_path("./spec/fixtures")
end

def fixture(file)
  File.new(fixture_path + '/' + file)
end
