require_relative '../../../lib/webhdfs'

require_relative '../../shared_examples/target_fs_examples'

describe WebHDFS::Client do
  it_behaves_like 'a target filesystem interface'
  it_behaves_like 'a target filesystem implementation'

  describe '#get_namenode_from_jmx' do
    it 'returns the correct namenode' do
      client = WebHDFS::Client.simple
      stub_request(:get, /localhost/).to_return(body: fixture('namenode_status.json'))
      expect(client.get_host_from_jmx).to eq(DEFAULT_NAMENODE)
    end

    it 'returns the default namenode when request fails' do
      client = WebHDFS::Client.simple
      client.host = 'remain'

      stub_request(:get, /localhost/).to_return(status: 500)
      allow_any_instance_of(WebHDFS::Client).to receive(:get_host_from_jmx).and_raise(WebHDFS::Error.new("Whatever"))
      client.set_host_from_jmx

      expect(client.host).to eq('remain')
    end
  end

  context 'when hdfs namenode has changed' do
    before :each do
      allow_any_instance_of(WebHDFS::Client).to receive(:get_host_from_jmx).and_return('bad_hdfs')
      allow_any_instance_of(WebHDFS::Client).to receive(:ensure_operational).and_return(true)

      @bad_hdfs = WebHDFS::Client.simple
      @bad_hdfs.set_host_from_jmx

      expect(@bad_hdfs.host).to eq('bad_hdfs')
    end

    it 'switches to the correct namenode' do
      allow_any_instance_of(WebHDFS::Client).to receive(:get_host_from_jmx).and_return(DEFAULT_NAMENODE)
      allow_any_instance_of(WebHDFS::Client).to receive(:ensure_operational).and_return(true)
      allow(Kernel).to receive(:sleep).and_return(true)
      expect {
        begin
          @bad_hdfs.list('/')
        rescue WebHDFS::IOError => e
        end
      }.to change {
        @bad_hdfs.host
      }.to eq DEFAULT_NAMENODE
    end
  end

  it 'retries when a "Cannot obtain block length" error occurs' do
    client = WebHDFS::Client.simple
    exception = WebHDFS::IOError.new(
      '{"RemoteException":{"exception":"IOException","javaClassName":"java.io.IOException","message":"Cannot obtain block length for L"}}')

    allow(Kernel).to receive(:sleep).and_return(true)
    expect(client).to receive(:request).and_raise(exception).twice

    begin
      client.list_filenames('/')
    rescue WebHDFS::IOError
    end
  end

  it 'retries when a ServerError occurs' do
    client = WebHDFS::Client.simple
    exception = WebHDFS::ServerError.new('Failed to connect to host d495.la.prod.factual.com:1006, execution expired')

    allow(Kernel).to receive(:sleep).and_return(true)
    expect(client).to receive(:request).and_raise(exception).twice

    begin
      client.list_filenames('/')
    rescue WebHDFS::ServerError
    end
  end
end
