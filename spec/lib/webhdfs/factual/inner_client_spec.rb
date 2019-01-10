require_relative '../../../../lib/webhdfs/factual'

include WebHDFS::Factual

describe InnerClient do
  describe '#detect_namenode' do
    it 'returns the correct namenode' do
      stub_request(:get, /localhost/).to_return(body: fixture('namenode_status.json'))
      expect(described_class.detect_namenode(API_HOST, DEFAULT_NAMENODE)).to eq('localhost')
    end

    it 'returns the default namenode when request fails' do
      stub_request(:get, /localhost/).to_return(status: 500)
      expect(described_class.detect_namenode(API_HOST, DEFAULT_NAMENODE)).to eq(DEFAULT_NAMENODE)
    end
  end
end
