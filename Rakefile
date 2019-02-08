require 'bundler'
Bundler::GemHelper.install_tasks

require 'rake/testtask'

begin
  require 'rspec/core/rake_task'
  RSpec::Core::RakeTask.new(:test)
rescue LoadError
end

task :doc do |t|
  `bundle exec rdoc --markup=tomdoc --visibility=public --include=lib --exclude=test`
end

task :default => [:build]
