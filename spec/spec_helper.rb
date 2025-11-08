require "bundler/setup"
Bundler.require(:test)
require 'logger'
require 'ostruct'

# Подгружать ДО BestChange
require_relative 'support/stub'

require 'virtus'
require 'grape'
require 'grape-entity'
require 'auto_logger'
require 'fast_jsonapi'
require 'sidekiq'
require 'factory_bot'

require 'money'
require 'gera'
require "gera/engine"
require "gera/railtie"
require 'active_support'
require 'active_support/core_ext'
require 'active_support/core_ext/module/delegation'
require 'active_support/all'
require 'active_support/core_ext/module'

require "best_change"

Gera::MoneySupport.init

gem_spec = Gem::Specification.find_by_name('gera')
gera_factories_path = File.join(gem_spec.gem_dir, 'factories')
FactoryBot.definition_file_paths << gera_factories_path
FactoryBot.find_definitions

BestChange.configure do |config|
  config.redis = "redis://#{ENV['REDIS_HOST'] || 'localhost' }:6379/1"
  config.exchanger_id = 522
  config.valuta_access_log = './log/valuta_access.log'
end

require 'vcr'

VCR.configure do |c|
  c.cassette_library_dir = 'spec/vcr_cassettes'
  # c.allow_http_connections_when_no_cassette = true
  c.ignore_localhost = true
  c.hook_into :webmock
  c.configure_rspec_metadata!
end

RSpec.configure do |config|
  config.include FactoryBot::Syntax::Methods
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.before(:each) do
    Gera::Universe.clear!
  end

  config.before(:suite) do
  end

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
