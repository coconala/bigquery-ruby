# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'bigquery/version'

Gem::Specification.new do |spec|
  spec.name          = "bigquery-ruby"
  spec.version       = BigQuery::VERSION
  spec.authors       = ["Satoshi Ebisawa"]
  spec.email         = ["satoshi.ebisawa@coconala.com"]

  spec.summary       = %q{A BigQuery client library for Ruby}
  spec.description   = %q{A BigQuery client library for Ruby}
  spec.homepage      = "https://github.com/coconala/bigquery-ruby"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.9"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "minitest"

  spec.add_runtime_dependency 'google-api-client', "~> 0.8.6"
end
