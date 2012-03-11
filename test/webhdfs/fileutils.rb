require 'test_helper'

class FileUtilsTest < Test::Unit::TestCase
  def setup
    require 'webhdfs'
  end

  def test_rm
    WebHDFS::FileUtils.mkdir('foo', :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.rm('foo', :verbose => true)
  end

  def test_rmr
    WebHDFS::FileUtils.mkdir_p('foo/bar/buzz', :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.rmr('foo', :verbose => true)
  end

  def test_rename
    #WebHDFS::FileUtils.mkdir_p('foo', :mode => 0777, :verbose => true)
    #WebHDFS::FileUtils.rename('foo', 'foo2', :verbose => true)
    #WebHDFS::FileUtils.rmr('foo2', :verbose => true)
  end

  def 
end
