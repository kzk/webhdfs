require 'bundler'
Bundler::GemHelper.install_tasks

require 'rake/testtask'

Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.test_files = FileList['test/webhdfs/*.rb']
  test.verbose = true
end

task :doc do |t|
  `bundle exec rdoc --markup=tomdoc --visibility=public --include=lib --exclude=test`
end

task :coverage do |t|
  ENV['SIMPLE_COV'] = '1'
  Rake::Task["test"].invoke
end

task :default => [:build]
