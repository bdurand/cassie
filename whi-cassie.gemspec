# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "whi-cassie"
  spec.version       = File.read(File.expand_path("../VERSION", __FILE__)).chomp
  spec.authors       = ["We Heart It", "Brian Durand"]
  spec.email         = ["dev@weheartit.com"]
  spec.description   = %q{Simple object mapper for Cassandra data tables.}
  spec.summary       = %q{Simple object mapper for Cassandra data tables specifically designed to work with the limitations and strengths of Cassandra.}
  spec.homepage      = "https://github.com/weheartit/cassie"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.required_ruby_version = '>= 2.0'
  spec.add_dependency('cassandra-driver', '~>3.0')
  spec.add_dependency('activemodel', '>=4.0')

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "appraisal"
end
