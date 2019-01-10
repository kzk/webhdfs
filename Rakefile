require 'bundler'
Bundler::GemHelper.install_tasks

require 'rake/testtask'

Rake::TestTask.new(:test_unit) do |t|
  t.libs << 'test' << '.'
  t.test_files = FileList['test/webhdfs/*.rb']
  t.verbose = true
end

begin
  require 'rspec/core/rake_task'
  RSpec::Core::RakeTask.new(:spec) do |t, task_args|
    t.rspec_opts = "-r ./spec/spec_helper.rb"
  end
rescue LoadError
end

task :test do |t|
  Rake::Task["spec"].invoke
  Rake::Task["test_unit"].invoke
end

task :doc do |t|
  `bundle exec rdoc --markup=tomdoc --visibility=public --include=lib --exclude=test`
end

task :coverage do |t|
  ENV['SIMPLE_COV'] = '1'
  Rake::Task["test"].invoke
end

task :default => [:build]
