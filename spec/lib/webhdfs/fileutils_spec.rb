require_relative '../../../lib/webhdfs'

def scoped_path(path)
  File.join(TEST_PATH, path)
end

describe WebHDFS::FileUtils, order: :defined do
  let(:version) { scoped_path('VERSION') }
  let(:foo) { scoped_path('foo') }

  before do
    WebHDFS::FileUtils.set_kerberos(true) if ENV['KERBEROS']
    WebHDFS::FileUtils.set_server(DEFAULT_NAMENODE, '50070')
  end

  it "can copy from local" do
    WebHDFS::FileUtils.copy_from_local('VERSION', version, :verbose => true)
    WebHDFS::FileUtils.copy_to_local(version, 'VERSION2', :verbose => true)
    WebHDFS::FileUtils.append(version, 'foo-bar-buzz', :verbose => true)
    WebHDFS::FileUtils.rm(version, :verbose => true)
    File.delete('VERSION2')
  end

  it "can copy from local via stream" do
    WebHDFS::FileUtils.copy_from_local_via_stream('VERSION', version, :verbose => true)
    WebHDFS::FileUtils.rm(version, :verbose => true)
  end

  it "can rm" do
    WebHDFS::FileUtils.mkdir(foo, :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.rm(foo, :verbose => true)
  end

  it "can rm -r" do
    WebHDFS::FileUtils.mkdir_p(scoped_path('foo/bar/buzz'), :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.rmr(foo, :verbose => true)
  end

  it "can rename" do
    foo2 = scoped_path('foo2')

    WebHDFS::FileUtils.mkdir_p(foo, :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.rename(foo, foo2, :verbose => true)
    WebHDFS::FileUtils.rmr(foo, :verbose => true)
  end

  it "can chmod" do
    WebHDFS::FileUtils.mkdir(foo, :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.chmod(0755, foo, :verbose => true)
    WebHDFS::FileUtils.chmod(0777, foo, :verbose => true)
    WebHDFS::FileUtils.rm(foo, :verbose => true)
  end

  # it "can chown" do
  #   WebHDFS::FileUtils.mkdir(foo, :mode => 0777, :verbose => true)
  #   WebHDFS::FileUtils.chown('test', 'test', foo, :verbose => true)
  #   WebHDFS::FileUtils.rm(foo, :verbose => true)
  # end

  it "can set repl factor" do
    WebHDFS::FileUtils.mkdir(foo, :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.set_repl_factor(foo, 2)
    WebHDFS::FileUtils.rm(foo, :verbose => true)
  end

  it "can set atime" do
    WebHDFS::FileUtils.mkdir(foo, :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.set_atime(foo, Time.now)
    WebHDFS::FileUtils.rm(foo, :verbose => true)
  end

  it "can set mtime" do
    WebHDFS::FileUtils.mkdir(foo, :mode => 0777, :verbose => true)
    WebHDFS::FileUtils.set_mtime(foo, Time.now)
    WebHDFS::FileUtils.rm(foo, :verbose => true)
  end
end
