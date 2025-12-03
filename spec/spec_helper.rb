require "bundler/setup"
Bundler.require(:test)
require 'logger'
require 'ostruct'

# Load support files first
require_relative 'support/stub'

# Core dependencies
require 'virtus'
require 'grape'
require 'grape-entity'
require 'auto_logger'
require 'fast_jsonapi'
# ActiveJob for background job testing
require 'active_job'
require 'active_job/test_helper'
require 'factory_bot'
require 'redis'
require 'redis/namespace'

# Money and Gera (external dependencies)
require 'money'
require 'active_support'
require 'active_support/core_ext'
require 'active_support/core_ext/module/delegation'
require 'active_support/all'
require 'active_support/core_ext/module'

# Define currency constants for factories
USD = Money::Currency.find('USD')
RUB = Money::Currency.find('RUB')
BTC = Money::Currency.find('BTC')

# Gera is not loaded to avoid conflicts with Money gem
# Using stubs from support/stub.rb instead

# Load BestChange after dependencies
require "best_change"

# Configure ActiveJob for testing
ActiveJob::Base.queue_adapter = :test
ActiveJob::Base.logger = Logger.new(nil)

# Stub SolidQueue-specific methods that aren't available in plain ActiveJob
module ActiveJob
  class Base
    def self.limits_concurrency(**_options)
      # No-op in tests - this is a SolidQueue-specific feature
    end
  end
end

# Load jobs manually (since Rails engine initializers don't run in tests)
Dir[File.join(File.dirname(__FILE__), '../app/jobs/best_change/*.rb')].sort.each do |file|
  require file
end

# Load our factories only (avoid duplication with gem factories)
FactoryBot.definition_file_paths = [File.join(__dir__, 'factories')]
FactoryBot.find_definitions

# Stub job for testing
class StubRatesExportJob
  def perform; end
end

# Configure BestChange for testing
BestChange.configure do |config|
  config.redis = Redis.new(db: 1, host: ENV['REDIS_HOST'] || 'localhost')
  config.exchanger_id = 522
  config.valuta_access_log = Rails.root.join('tmp', 'valuta_access.log').to_s if defined?(Rails)
  config.rates_export_job_class = StubRatesExportJob
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

  # Clean up Gera universe before each test
  config.before(:each) do
    Gera::Universe.clear!
  end

  # Clean up Redis before each test (unless Rails handles it)
  config.before(:each) do
    unless defined?(Rails) && Rails.env.test?
      if defined?(Redis)
        redis = Redis.current
        redis.flushdb if redis
      end
    end
  end

  # Setup for ActiveJob testing
  config.include ActiveJob::TestHelper, type: :job

  # Mock Time.zone for tests
  config.before(:each) do
    Time.zone = ActiveSupport::TimeZone['UTC']
  end

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  # Filter VCR cassettes from backtraces
  config.filter_rails_from_backtrace! if defined?(Rails)
end
