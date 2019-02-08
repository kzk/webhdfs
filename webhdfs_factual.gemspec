# encoding: utf-8

Gem::Specification.new do |gem|
  gem.name        = "webhdfs-factual"
  gem.description = "Ruby WebHDFS/HttpFs client"
  gem.homepage    = "https://github.com/Factual/webhdfs/"
  gem.summary     = gem.description
  gem.version     = File.read("VERSION").strip
  gem.authors     = ["Kazuki Ohta", "Satoshi Tagomori", "Jorge Israel Peña"]
  gem.email       = ["kazuki.ohta@gmail.com", "tagomoris@gmail.com", "jorge@factual.com"]
  gem.has_rdoc    = false
  gem.files       = Dir['lib/**/*','spec/**/*','*.gemspec','*.md','AUTHORS','COPYING','Gemfile','VERSION']
  gem.test_files  = gem.files.grep(%r{^(spec|features)/})
  gem.require_paths = ['lib']

  gem.add_development_dependency "rake"
  gem.add_development_dependency "rdoc"
  gem.add_development_dependency "simplecov"
  gem.add_development_dependency "rspec"
  gem.add_development_dependency "webmock"

  gem.add_runtime_dependency "addressable"
  gem.add_runtime_dependency "faraday"
  gem.add_runtime_dependency "gssapi-factual"
end
