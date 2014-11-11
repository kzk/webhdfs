require 'minitest/autorun'
require 'webhdfs/client_v1'

describe WebHDFS::ClientV1 do
  describe 'knox paths' do
    it 'takes a optional path so that you can connect to knox gateways' do
      client = WebHDFS::ClientV1.new('host', 8443, nil, nil, nil, nil, '/gateway/cluster')
      client.api_path('').must_equal '/gateway/cluster/webhdfs/v1/'
    end

    it 'does not affect the api path if not specified' do
      client = WebHDFS::ClientV1.new('host', 8443)
      client.api_path('').must_equal '/webhdfs/v1/'
    end
  end
end
