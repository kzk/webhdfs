require_relative 'dir_examples'

shared_examples 'a target filesystem interface' do

  subject{described_class.new(API_HOST, DEFAULT_NAMENODE)}

  %w(
    _append
    _rm_r
    _ls
    _mkdir
    _mv
    _read
    _tip_of_tail
    _mtime
  ).each do |method|
    it { is_expected.to respond_to(method) }
  end

end

shared_examples 'a target filesystem implementation' do
  include_context 'dir setup'

  subject{get_client}

  let(:filepath){ File.join(path, 'test.txt') }

  describe '#ls' do
    it 'lists files' do
      files = %w(a b)
      files.each do |letter|
        subject._append(File.join(path, letter), letter)
      end
      expect(subject._ls(path)).to eq files
    end
  end

  describe '#append' do

    it 'creates the file if it does not exist' do
      expect {
        subject._append(filepath, 'IMPORTING')
      }.not_to raise_error
    end
  end

  describe '#tip_of_tail' do

    it 'returns the last line appended to the file' do
      subject._append(filepath, 'IMPORTING')
      subject._append(filepath, 'QAING')
      expect(subject._tip_of_tail(filepath)).to eq 'QAING'
    end

    it 'returns an empty string when file is empty' do
      subject._append(filepath, '')
      expect(subject._tip_of_tail(filepath)).to eq ''
    end

    it 'raises an exception when file not found' do
      expect {
        subject._tip_of_tail(filepath)
      }.to raise_error(WebHDFS::FileNotFoundError, /not found/)
    end
  end

  describe '#rm_r' do
    it 'deletes the file or directory' do
      subject._append(filepath, 'IMPORTING')
      expect {
        subject._rm_r(filepath)
      }.to change {
        subject._ls(File.dirname(filepath)).size
      }.by(-1)
    end
  end

  describe '#rm_r!' do
    it 'raises an exception when file not found' do
      expect {
        subject._rm_r!(filepath)
      }.to raise_error(WebHDFS::FileNotFoundError, /not found/)
    end
  end

  describe '#read' do
    it 'returns the entire file' do
      subject._append(filepath, 'IMPORTING')
      subject._append(filepath, 'QAING')
      expect(subject._read(filepath)).to eq "IMPORTING\nQAING\n"
    end

    it 'raises an exception when file not found' do
      expect {
        subject._read(filepath)
      }.to raise_error(WebHDFS::FileNotFoundError, /not found/)
    end
  end

  describe '#mtime' do
    it 'returns the modification time for the file' do
      before = Time.now - 1
      subject._append(filepath, 'IMPORTING')
      expect(subject._mtime(filepath)).to be > before
      expect(subject._mtime(filepath)).to be <= Time.now
    end

    it 'raises an exception when file is not found' do
      expect {
        subject._mtime(filepath)
      }.to raise_error(WebHDFS::FileNotFoundError, /not found/)
    end
  end

end
