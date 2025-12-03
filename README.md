# BestChange

A Ruby gem that provides an adapter and utilities to work with data from [bestchange.ru](https://bestchange.ru) - a Russian currency exchange aggregator.

## Features

* **Exchange Rate Loading** - Loads exchange rates from BestChange API
* **Commission Calculation** - Determines commission rates of other exchanges relative to base rates
* **Competitiveness Analysis** - Determines if current rates are competitive in the market
* **Rails Engine Integration** - Full Rails Engine with API endpoints and background job support
* **Background Processing** - ActiveJob-based background jobs for rate processing
* **Redis-based Storage** - Fast data storage and caching with Redis
* **RESTful API** - Built-in API endpoints for accessing exchange rate data

## Installation

### Rails Application Installation

Add this line to your application's Gemfile:

```ruby
gem 'best_change'
```

And then execute:
```bash
$ bundle
```

### Installation Generator

Run the installation generator to set up the gem:

```bash
$ rails generate best_change:install
```

This will:
* Create `config/initializers/best_change.rb`
* Add engine routes to `config/routes.rb`
* Create `config/best_change.yml` configuration file
* Configure ActiveJob queue adapter

### Manual Installation

If you prefer manual setup, add the gem and create the initializer manually:

```ruby
# config/initializers/best_change.rb
BestChange.configure do |config|
  config.redis = Redis.new(
    host: ENV.fetch('REDIS_HOST', 'localhost'),
    port: ENV.fetch('REDIS_PORT', 6379),
    db: ENV.fetch('REDIS_DB', 0)
  )
  config.exchanger_id = ENV.fetch('BESTCHANGE_EXCHANGER_ID', 123)
  config.logger = Rails.logger
end
```

Add the engine routes:

```ruby
# config/routes.rb
mount BestChange::Engine => '/best_change'
```

## Configuration

### Environment Variables

Required:
* `BESTCHANGE_EXCHANGER_ID` - Your BestChange exchanger ID

Optional:
* `REDIS_HOST` - Redis host (default: localhost)
* `REDIS_PORT` - Redis port (default: 6379)
* `REDIS_DB` - Redis database number (default: 0)
* `REDIS_PASSWORD` - Redis password if required
* `BESTCHANGE_FETCHER_PATH` - Path to BestChange fetcher executable
* `BESTCHANGE_ACCESS_LOG` - Path to access log file

### Configuration Options

```ruby
BestChange.configure do |config|
  config.redis = Redis.new(...)           # Redis connection (required)
  config.exchanger_id = 123               # Your exchanger ID (required)
  config.fetcher_path = '~/fetcher/main'  # Path to fetcher (optional)
  config.valuta_access_log = 'log.txt'    # Access log path (optional)
  config.auto_start_jobs = true            # Enable background jobs (default: true)
  config.api_enabled = true               # Enable API endpoints (default: true)
  config.logger = Rails.logger            # Logger instance (optional)
end
```

## Usage

### API Endpoints

Once mounted, the engine provides the following API endpoints:

```bash
# Health check
GET /best_change/health

# Get all exchange rates
GET /best_change/api/v1/exchange_rates

# Get status summary
GET /best_change/api/v1/exchange_rates/status

# Get competitive rates only
GET /best_change/api/v1/exchange_rates/competitive

# Get available currency pairs
GET /best_change/api/v1/currency_pairs
GET /best_change/api/v1/currency_pairs/available
```

### Background Jobs

#### Manual Rate Loading

```bash
# Load rates asynchronously (via ActiveJob)
$ rails best_change:load_rates

# Load rates synchronously (blocking)
$ rails best_change:load_rates_sync
```

#### Configuration Check

```bash
# Check your BestChange configuration
$ rails best_change:check_config

# Clear all BestChange data from Redis
$ rails best_change:clear_data
```

### Direct Usage

```ruby
# Initialize service
service = BestChange::Service.new

# Get all exchange rates
rates = service.get_all_rates

# Get competitive rates only
competitive_rates = service.get_competitive_rates

# Get status summary
status = service.get_status_summary

# Get available currency pairs
pairs = service.get_available_currency_pairs
```

### Custom Jobs

Generate custom jobs:

```bash
$ rails generate best_change:job MyCustomJob
```

This creates:
* `app/jobs/best_change/my_custom_job.rb`
* `spec/jobs/best_change/my_custom_job_spec.rb`

## Development

### Setup

After checking out the repo, run:
```bash
$ bin/setup
```

### Running Tests

```bash
# Run all tests
$ bundle exec rake spec

# Run specific test file
$ rspec spec/best_change/service_spec.rb

# Run tests with coverage
$ COVERAGE=true bundle exec rake spec
```

### Dummy Application

The gem includes a dummy Rails application in `spec/dummy/` for testing:

```bash
# Run console with dummy app
$ cd spec/dummy && bin/rails console

# Run tests in dummy context
$ cd spec/dummy && bundle exec rake spec
```

### Console

```bash
$ bin/console
```

## Rails Compatibility

* ✅ Rails 6.0+
* ✅ Rails 6.1+
* ✅ Rails 7.0+
* ✅ Rails 7.1+

## Ruby Compatibility

* ✅ Ruby 2.7+
* ✅ Ruby 3.0+
* ✅ Ruby 3.1+
* ✅ Ruby 3.2+

## Dependencies

### Runtime Dependencies
* `rails` (>= 6.1)
* `redis` (~> 4.0)
* `activejob` (included in Rails)
* `virtus`
* `gera`
* `grape`
* `oj`
* `money-rails` (~> 1.11)

### Development Dependencies
* `rspec-rails`
* `factory_bot_rails`
* `rubocop`
* `byebug`

## Performance

* **Redis Storage**: Fast O(1) lookup for exchange rates
* **Background Processing**: Non-blocking rate processing with ActiveJob
* **Batch Processing**: Efficient batch processing of rate updates
* **Caching**: Built-in caching layer for frequently accessed data

## Troubleshooting

### Common Issues

1. **Redis Connection Failed**
   ```bash
   $ rails best_change:check_config
   ```
   Ensure Redis is running and accessible.

2. **Exchanger ID Not Set**
   ```bash
   export BESTCHANGE_EXCHANGER_ID=123
   ```

3. **Fetcher Not Found**
   ```bash
   export BESTCHANGE_FETCHER_PATH=/path/to/fetcher/main
   ```

### Debug Mode

Enable detailed logging:
```ruby
BestChange.configure do |config|
  config.logger = Logger.new(STDOUT, level: :debug)
end
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/dapi/best_change.

### Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b my-new-feature`)
3. Run the test suite (`bundle exec rake spec`)
4. Add tests for your changes
5. Commit your changes (`git commit -am 'Add some feature'`)
6. Push to the branch (`git push origin my-new-feature`)
7. Create a Pull Request

### Code Style

This project uses:
* [RuboCop](https://github.com/rubocop-hq/rubocop) for Ruby code style
* [RSpec](https://rspec.info/) for testing
* Conventional commit messages

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the BestChange project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/dapi/best_change/blob/master/CODE_OF_CONDUCT.md).
