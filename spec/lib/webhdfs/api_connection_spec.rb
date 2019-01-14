require_relative '../../../lib/webhdfs'

include WebHDFS

describe APIConnection do

  subject(:api) { described_class.new('http://httpbin.org') }

  describe '#handle_response' do

    context 'when request failed' do
      before :each do
        stub_request(:get, /status\/404/).to_return(status: 404, body: "not found", headers: {cats: "dogs"})
      end

      it 'raises an exception' do
        expect {
          api.instance_eval{ get('status/404') }
        }.to raise_error(WebHDFS::RequestError)
      end

      it 'the exception message includes response status, url, and method' do
        begin
          api.instance_eval{ get('status/404') }
        rescue WebHDFS::RequestError => e
          expect(e.message).to match /GET/i
          expect(e.message).to match /404/
          expect(e.message).to match /http:/
        end
      end

      it 'the exception metadata includes the response body and headers' do
        begin
          api.instance_eval{ get('status/404') }
        rescue WebHDFS::RequestError => e
          expect(e.metadata[:body]).to match /not found/
          expect(e.metadata[:headers].keys).to include 'cats'
        end
      end
    end

  end
end
