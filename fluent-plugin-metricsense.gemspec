$:.push File.expand_path('../lib', __FILE__)

Gem::Specification.new do |s|
  s.name        = "fluent-plugin-metricsense"
  s.description = "MetricSense - application metrics aggregation plugin for Fluentd"
  s.summary     = s.description
  s.homepage    = "https://github.com/treasure-data/fluent-plugin-metricsense"
  s.version     = File.read("VERSION").strip
  s.authors     = ["Sadayuki Furuhashi"]
  s.email       = "sf@treasure-data.com"
  s.has_rdoc    = false
  s.require_paths = ['lib']
  #s.platform    = Gem::Platform::RUBY
  s.files       = `git ls-files`.split("\n")
  s.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }

  s.add_dependency "fluentd", "~> 0.10.6"
  s.add_dependency "dogapi"
  s.add_development_dependency "rake", ">= 0.8.7"
  s.add_development_dependency 'bundler', ['>= 1.0.0']
  s.add_development_dependency "simplecov", ">= 0.5.4"
end
