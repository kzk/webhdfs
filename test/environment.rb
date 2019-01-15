JMX_HOST = ENV['JMX_HOST'] || 'http://localhost'
DEFAULT_NAMENODE = ENV['DEFAULT_NAMENODE'] || 'localhost'
TEST_DIR = ENV['TEST_DIR']

if TEST_DIR.nil? || TEST_DIR.empty?
  raise "Must explicitly set a TEST_DIR to run tests within"
end
