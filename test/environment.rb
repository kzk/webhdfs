JMX = ENV['JMX'] || 'http://localhost'
DEFAULT_NAMENODE = ENV['DEFAULT_NAMENODE'] || 'localhost'
TEST_PATH = ENV['TEST_PATH']

if TEST_PATH.nil? || TEST_PATH.empty?
  raise "Must explicitly set a TEST_PATH to run tests within"
end
