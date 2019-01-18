shared_examples 'dir setup' do

  let(:path) do
    File.join(TEST_PATH, 'test_dir')
  end

  before :each do
    WebHDFS::Client.simple.mkdir(path)
  end

  after :each do
    WebHDFS::Client.simple.delete_recursive(path)
  end
end
