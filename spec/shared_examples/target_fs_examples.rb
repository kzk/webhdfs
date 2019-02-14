require_relative 'dir_examples'

shared_examples 'a target filesystem interface' do

  subject { described_class.simple }

  %w(
    append_or_create
    delete_recursive
    list_filenames
    mkdir
    move_paths
    safe_read
    tip_of_tail
    mtime
  ).each do |method|
    it { is_expected.to respond_to(method) }
  end

end

shared_examples 'a target filesystem implementation' do
  include_context 'dir setup'

  subject { WebHDFS::Client.simple }

  let(:filepath){ File.join(path, 'test.txt') }

  describe '#list_filenames' do
    it 'lists filenames' do
      files = %w(a b)
      files.each do |letter|
        subject.append_or_create(File.join(path, letter), letter)
      end
      expect(subject.list_filenames(path)).to eq files
    end
  end

  describe '#append_or_create' do

    it 'creates the file if it does not exist' do
      expect {
        subject.append_or_create(filepath, 'IMPORTING')
      }.not_to raise_error
    end
  end

  describe '#tip_of_tail' do

    it 'returns the last line appended to the file' do
      subject.append_or_create(filepath, 'IMPORTING')
      subject.append_or_create(filepath, 'QAING')
      expect(subject.tip_of_tail(filepath)).to eq 'QAING'
    end

    it 'returns an empty string when file is empty' do
      subject.append_or_create(filepath, '')
      expect(subject.tip_of_tail(filepath)).to eq ''
    end

    it 'raises an exception when file not found' do
      expect {
        subject.tip_of_tail(filepath)
      }.to raise_error(WebHDFS::FileNotFoundError)
    end
  end

  describe '#delete_recursive' do
    it 'deletes the file or directory' do
      subject.append_or_create(filepath, 'IMPORTING')
      expect {
        subject.delete_recursive(filepath)
      }.to change {
        subject.list_filenames(File.dirname(filepath)).size
      }.by(-1)
    end

    it 'raises an exception when file not found' do
      expect {
        subject.delete_recursive!(filepath)
      }.to raise_error(WebHDFS::FileNotFoundError)
    end
  end

  describe '#safe_read' do
    it 'returns the entire file' do
      subject.append_or_create(filepath, 'IMPORTING')
      subject.append_or_create(filepath, 'QAING')
      expect(subject.safe_read(filepath)).to eq "IMPORTING\nQAING\n"
    end

    it 'raises an exception when file not found' do
      expect {
        subject.safe_read(filepath)
      }.to raise_error(WebHDFS::FileNotFoundError)
    end
  end

  describe '#mtime' do
    it 'returns the modification time for the file' do
      before = Time.now - 1
      subject.append_or_create(filepath, 'IMPORTING')
      expect(subject.mtime(filepath)).to be > before
      expect(subject.mtime(filepath)).to be <= Time.now
    end

    it 'raises an exception when file is not found' do
      expect {
        subject.mtime(filepath)
      }.to raise_error(WebHDFS::FileNotFoundError)
    end
  end

end
