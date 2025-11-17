# This file is copied to spec/ when you run 'rails generate rspec:install'
ENV['RAILS_ENV'] ||= 'test'

require File.expand_path('dummy/config/environment', __dir__)

# Prevent database truncation if the environment is production
abort("The Rails environment is running in production mode!") if Rails.env.production?

require 'spec_helper'

# Load Rails specific gema dependencies
require_relative 'support/rails_dependencies'
require_relative 'support/factory_loading'

require 'rspec/rails'
# Add additional requires below this line. Rails is not loaded until this point!

# VCR is already configured in spec_helper, but we ensure Rails-specific paths are set correctly
VCR.configure do |c|
  c.cassette_library_dir = Rails.root.join('spec', 'vcr_cassettes').to_s
end

# Requires supporting ruby files with custom matchers and macros, etc, in
# spec/support/ and its subdirectories. Files matching `spec/**/*_spec.rb` are
# run as spec files by default. This means that files in spec/support that end
# in _spec.rb will both be required and run as specs, causing the specs to be
# run twice. It is recommended that you do not name files matching this glob to
# end with _spec.rb. You can configure this pattern with the --pattern
# option on the command line or in ~/.rspec, .rspec or `.rspec-local`.
#
# The following line is provided for convenience purposes. It has the downside
# of increasing the boot-up time by auto-requiring all files in the support
# directory. Alternatively, in the individual `*_spec.rb` files, manually
# require only the support files necessary.
#
Dir[Rails.root.join('spec', 'support', '**', '*.rb')].sort.each { |f| require f }

# Checks for pending migrations and applies them before tests are run.
# If you are not using ActiveRecord, you can remove this line.
begin
  ActiveRecord::Migration.maintain_test_schema!
rescue ActiveRecord::PendingMigrationError => e
  puts e.to_s.strip
  exit 1
end

RSpec.configure do |config|
  # Remove this line if you're not using ActiveRecord or ActiveRecord fixtures
  config.fixture_path = Rails.root.join('spec', 'fixtures')

  # If you're not using ActiveRecord, or you'd prefer not to run each of your
  # examples within a transaction, remove the following line or assign false
  # instead of true.
  config.use_transactional_fixtures = true

  # You can uncomment this line to turn off ActiveRecord support entirely.
  # config.use_active_record = false

  # RSpec Rails can automatically mix in different types of examples with faster,
  # less explicit types. For example, instead of writing `type: :model` in your
  # describe block, you can simply write `require "rails_helper"` and RSpec will
  # automatically infer the type.
  #
  # In addition to using `require "rails_helper"`, you can also tag examples with
  # a specific type using the `:type` metadata:
  #
  #     describe "My feature", type: :system do
  #       # ...
  #     end
  #
  # The different available types are:
  #
  # * `component` - Uses a Rails component to test a component.
  # * `controller` - Uses ActionController::TestCase for controller tests.
  # * `feature` - Uses Capybara for feature/integration tests.
  # * `helper` - Uses ActionView::TestCase for helper tests.
  # * `job` - Uses ActiveJob::TestCase for job tests.
  # * `mailer` - Uses ActionMailer::TestCase for mailer tests.
  # * `model` - Uses ActiveRecord::TestCase for model tests.
  # * `request` - Uses ActionDispatch::IntegrationTest for request (API) tests.
  # * `routing` - Uses ActionDispatch::RoutingTestSuite for routing tests.
  # * `system` - Uses Capybara for system/integration tests.
  # * `view` - Uses ActionView::TestCase for view tests.
  #
  # You can set this meta-data to true to automatically tag all examples
  # in a spec file with the corresponding type metadata:
  #
  #     # spec/models/user_spec.rb
  #     require "rails_helper"
  #
  #     RSpec.describe User, type: :model do
  #       # ...
  #     end
  #
  # Read more about automatic type inference in
  # https://relishapp.com/rspec/rspec-rails/docs/autotagging-with-examples-metadata

  # Automatically infer spec type from file location, e.g. type: :model
  config.infer_spec_type_from_file_location!

  # Filter lines from Rails gems in backtraces.
  config.filter_rails_from_backtrace!
  # arbitrary gems may also be filtered via:
  # config.filter_gems_from_backtrace("gem name")

  # Include FactoryBot syntax methods
  config.include FactoryBot::Syntax::Methods

  # Include engine specific helpers
  config.include BestChange::Engine.routes.url_helpers if defined?(BestChange::Engine)

  # Setup for Sidekiq testing
  config.before(:each) do
    Sidekiq::Worker.clear_all
  end

  # Clean up Redis before each test
  config.before(:each) do
    if defined?(Redis)
      redis = Redis.current
      redis.flushdb if redis
    end
  end

  # Clean up Gera universe before each test
  config.before(:each) do
    Gera::Universe.clear! if defined?(Gera::Universe)
  end

  # Configuration for engine testing
  config.before(:suite) do
    # Load any additional engine-specific configuration
  end

  # For engine testing - ensure routes are loaded
  config.before(:each, type: :controller) do
    @routes = BestChange::Engine.routes if defined?(BestChange::Engine)
  end

  # For request/integration testing
  config.before(:each, type: :request) do
    @routes = BestChange::Engine.routes if defined?(BestChange::Engine)
  end

  # Include VCR for tests that use external HTTP requests
  config.around(:each, :vcr) do |example|
    VCR.use_cassette(example.metadata[:vcr]) do
      example.run
    end
  end
end