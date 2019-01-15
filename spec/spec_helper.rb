require 'webmock/rspec'
WebMock.disable_net_connect!(allow: [/\/localhost/, /\/hdfs-dev\./, /\/webhdfs\//])

# FIXME: DRY
JMX_HOST = ENV['JMX_HOST'] || 'http://localhost'
DEFAULT_NAMENODE = ENV['DEFAULT_NAMENODE'] || 'localhost'
TEST_DIR = ENV['TEST_DIR']

if TEST_DIR.nil? || TEST_DIR.empty?
  raise "Must explicitly set a TEST_DIR to run tests within"
end

def fixture_path
  File.expand_path("./spec/fixtures")
end

def fixture(file)
  File.new(fixture_path + '/' + file)
end
