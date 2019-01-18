require 'test_helper'

def scoped_path(path)
  File.join(TEST_PATH, path)
end

class FileUtilsTest < Test::Unit::TestCase
  def setup
    require 'lib/webhdfs'
    require 'lib/webhdfs/fileutils'

    WebHDFS::FileUtils.set_kerberos(true) if ENV['KERBEROS']
    WebHDFS::FileUtils.set_server(DEFAULT_NAMENODE, '50070')

    @version = scoped_path('VERSION')
    @foo = scoped_path('food')
  end

  def test_copy_from_local
    WebHDFS::FileUtils.copy_from_local('VERSION', @version, :verbose => true)
    WebHDFS::FileUtils.copy_to_local(@version, 'VERSION2', :verbose => true)
    WebHDFS::FileUtils.append(@version, 'foo-bar-buzz', :verbose => true)
    WebHDFS::FileUtils.rm(@version, :verbose => true)
    File.delete('VERSION2')
  end

  def test_copy_from_local_via_stream
    WebHDFS::FileUtils.copy_from_local_via_stream('VERSION', @version, :verbose => true)
    WebHDFS::FileUtils.rm(@version, :verbose => true)
  end

  def test_rm
    WebHDFS::FileUtils.mkdir(@foo, :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.rm(@foo, :verbose => true)
  end

  def test_rmr
    WebHDFS::FileUtils.mkdir_p(scoped_path('foo/bar/buzz'), :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.rmr(@foo, :verbose => true)
  end

  def test_rename
    foo2 = scoped_path('foo2')

    WebHDFS::FileUtils.mkdir_p(@foo, :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.rename(@foo, foo2, :verbose => true)
    WebHDFS::FileUtils.rmr(@foo, :verbose => true)
  end

  def test_chmod
    WebHDFS::FileUtils.mkdir(@foo, :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.chmod(0755, @foo, :verbose => true)
    WebHDFS::FileUtils.chmod(0777, @foo, :verbose => true)
    WebHDFS::FileUtils.rm(@foo, :verbose => true)
  end

  # def test_chown
  #   WebHDFS::FileUtils.mkdir(@foo, :mode => 0777, :verbose => true)
  #   WebHDFS::FileUtils.chown('test', 'test', @foo, :verbose => true)
  #   WebHDFS::FileUtils.rm(@foo, :verbose => true)
  # end

  def test_set_repl_factor
    WebHDFS::FileUtils.mkdir(@foo, :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.set_repl_factor(@foo, 2)
    WebHDFS::FileUtils.rm(@foo, :verbose => true)
  end

  def test_set_atime
    WebHDFS::FileUtils.mkdir(@foo, :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.set_atime(@foo, Time.now)
    WebHDFS::FileUtils.rm(@foo, :verbose => true)
  end

  def test_set_mtime
    WebHDFS::FileUtils.mkdir(@foo, :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.set_mtime(@foo, Time.now)
    WebHDFS::FileUtils.rm(@foo, :verbose => true)
  end
end
