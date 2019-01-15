require_relative 'dir_examples'

shared_examples 'a target filesystem interface' do

  subject{
    described_class.new(jmx_host: JMX_HOST) do |c|
      c.host = DEFAULT_NAMENODE
    end
  }

  %w(
    append
    rm_r
    ls
    mkdir
    mv
    read
    tip_of_tail
    mtime
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
        subject.append(File.join(path, letter), letter)
      end
      expect(subject.ls(path)).to eq files
    end
  end

  describe '#append' do

    it 'creates the file if it does not exist' do
      expect {
        subject.append(filepath, 'IMPORTING')
      }.not_to raise_error
    end
  end

  describe '#tip_of_tail' do

    it 'returns the last line appended to the file' do
      subject.append(filepath, 'IMPORTING')
      subject.append(filepath, 'QAING')
      expect(subject.tip_of_tail(filepath)).to eq 'QAING'
    end

    it 'returns an empty string when file is empty' do
      subject.append(filepath, '')
      expect(subject.tip_of_tail(filepath)).to eq ''
    end

    it 'raises an exception when file not found' do
      expect {
        subject.tip_of_tail(filepath)
      }.to raise_error(WebHDFS::FileNotFoundError, /not found/)
    end
  end

  describe '#rm_r' do
    it 'deletes the file or directory' do
      subject.append(filepath, 'IMPORTING')
      expect {
        subject.rm_r(filepath)
      }.to change {
        subject.ls(File.dirname(filepath)).size
      }.by(-1)
    end
  end

  describe '#rm_r!' do
    it 'raises an exception when file not found' do
      expect {
        subject.rm_r!(filepath)
      }.to raise_error(WebHDFS::FileNotFoundError, /not found/)
    end
  end

  describe '#read' do
    it 'returns the entire file' do
      subject.append(filepath, 'IMPORTING')
      subject.append(filepath, 'QAING')
      expect(subject.read(filepath)).to eq "IMPORTING\nQAING\n"
    end

    it 'raises an exception when file not found' do
      expect {
        subject.read(filepath)
      }.to raise_error(WebHDFS::FileNotFoundError, /not found/)
    end
  end

  describe '#mtime' do
    it 'returns the modification time for the file' do
      before = Time.now - 1
      subject.append(filepath, 'IMPORTING')
      expect(subject.mtime(filepath)).to be > before
      expect(subject.mtime(filepath)).to be <= Time.now
    end

    it 'raises an exception when file is not found' do
      expect {
        subject.mtime(filepath)
      }.to raise_error(WebHDFS::FileNotFoundError, /not found/)
    end
  end

end
