$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'rubygems'
require 'test/unit'

# FIXME: DRY
JMX_HOST = ENV['JMX_HOST'] || 'http://localhost'
DEFAULT_NAMENODE = ENV['DEFAULT_NAMENODE'] || 'localhost'
TEST_DIR = ENV['TEST_DIR']

if TEST_DIR.nil? || TEST_DIR.empty?
  raise "Must explicitly set a TEST_DIR to run tests within"
end

if ENV['SIMPLE_COV']
  require 'simplecov'
  SimpleCov.start do 
    add_filter 'test/'
    add_filter 'pkg/'
    add_filter 'vendor/'
  end
end

require 'test/unit'
