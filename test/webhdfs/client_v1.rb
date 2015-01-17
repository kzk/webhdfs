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

    it 'does not add the path if already present' do
      client = WebHDFS::ClientV1.new('host', 8443, nil, nil, nil, nil, '/gateway/cluster')
      client.api_path('/gateway/cluster/webhdfs/v1/path').must_equal '/gateway/cluster/webhdfs/v1/path'
    end
  end

  describe 'basic auth' do
    it 'takes an optional basic auth username and password' do
      client = WebHDFS::ClientV1.new('host', 8443,
                                     nil, nil, nil, nil,
                                     '/gateway/cluster', 'user', 'password')

      mock_request = MiniTest::Mock.new
      mock_request.expect :basic_auth, nil, ['user', 'password']

      response = Net::HTTPSuccess.new 1, 200, 'hi'

      mock_connection = MiniTest::Mock.new
      mock_connection.expect :request, response, [mock_request]

      client.stub :build_connection, mock_connection do
        Net::HTTPGenericRequest.stub :new, mock_request do
          client.request('host', 'port','method', 'path')
        end
      end

      mock_request.verify
    end
  end
end
