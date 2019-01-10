require 'webmock/rspec'
# WebMock.disable_net_connect!(allow: [/\/hdfs-dev\./, /\/webhdfs\//, /\/localhost\//])
WebMock.disable_net_connect!(allow: [/\/localhost/])

def fixture_path
  File.expand_path("./spec/fixtures")
end

def fixture(file)
  File.new(fixture_path + '/' + file)
end
