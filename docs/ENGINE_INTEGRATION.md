# BestChange Rails Engine Integration Guide

## Overview

This document describes the Rails Engine integration for the BestChange gem, which provides a complete Rails Engine solution for working with BestChange exchange rate data.

## Architecture

### Core Components

1. **Rails Engine (`lib/best_change/engine.rb`)**
   - Isolated namespace for BestChange functionality
   - Automatic middleware and initializers loading
   - Rake tasks and generators integration

2. **API Controllers (`app/controllers/best_change/api/v1/*`)**
   - RESTful API endpoints for exchange rate data
   - JSON responses with error handling
   - Health check endpoint

3. **Background Workers (`lib/best_change/*_worker.rb`)**
   - Rails-compatible Sidekiq workers
   - Enhanced error handling and logging
   - Automatic configuration validation

4. **Configuration System**
   - Environment-based configuration
   - Rails logger integration
   - Validation middleware for development

## Installation Process

### 1. Standard Gem Installation

```bash
# Add to Gemfile
gem 'best_change'

# Install
bundle install

# Run installation generator
rails generate best_change:install
```

### 2. Manual Installation

```ruby
# config/initializers/best_change.rb
BestChange.configure do |config|
  config.redis = Redis.new(host: 'localhost', port: 6379)
  config.exchanger_id = ENV['BESTCHANGE_EXCHANGER_ID']
  config.logger = Rails.logger
end

# config/routes.rb
mount BestChange::Engine => '/best_change'
```

## Available Features

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/best_change/health` | Engine health check |
| GET | `/best_change/api/v1/exchange_rates` | All exchange rates |
| GET | `/best_change/api/v1/exchange_rates/status` | Status summary |
| GET | `/best_change/api/v1/exchange_rates/competitive` | Competitive rates |
| GET | `/best_change/api/v1/currency_pairs` | Currency pairs |

### Rake Tasks

```bash
# Configuration and testing
rails best_change:check_config        # Verify configuration
rails best_change:clear_data          # Clear Redis data

# Rate loading
rails best_change:load_rates          # Async loading
rails best_change:load_rates_sync     # Sync loading

# Monitoring
rails best_change:start_monitoring    # Auto monitoring
rails best_change:stop_monitoring     # Stop monitoring
```

### Generators

```bash
# Installation generator
rails generate best_change:install

# Custom worker generator
rails generate best_change:worker MyWorker
```

## Configuration Options

### Required Configuration
- `redis` - Redis connection instance
- `exchanger_id` - BestChange exchanger ID

### Optional Configuration
- `logger` - Logger instance (defaults to Rails.logger)
- `auto_start_workers` - Enable background workers (default: true)
- `api_enabled` - Enable API endpoints (default: true)
- `fetcher_path` - Path to fetcher executable
- `valuta_access_log` - Access log file path

### Environment Variables

```bash
# Required
BESTCHANGE_EXCHANGER_ID=123

# Optional
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=secret
BESTCHANGE_FETCHER_PATH=~/fetcher/main
BESTCHANGE_ACCESS_LOG=/var/log/best_change.log
```

## Development Workflow

### Running Tests

```bash
# Run all tests
bundle exec rake spec

# Run with coverage
COVERAGE=true bundle exec rake spec

# Run Rails Engine tests
bundle exec rake test_engine

# Run all CI checks
bundle exec rake ci
```

### Working with Dummy App

```bash
# Console with dummy app
cd spec/dummy && bin/rails console

# Test API endpoints
cd spec/dummy && bin/rails s -p 3000
curl http://localhost:3000/best_change/health
```

## Multiple Rails Version Support

The gem supports:
- Rails 6.1+
- Ruby 2.7+

### Testing Multiple Versions

The CI matrix tests against:
- Ruby 2.7, 3.0, 3.1, 3.2, 3.3
- Rails 6.1, 7.0, 7.1

### Local Testing with Appraisals

```bash
# Install appraisal gem
gem install appraisal

# Generate gemfiles
bundle exec appraisal install

# Run tests against all versions
bundle exec appraisal bundle exec rake spec

# Run specific version
bundle exec appraisal rails-7.0 bundle exec rake spec
```

## Security Considerations

### Input Validation
- Configuration validation in development mode
- Error handling in API controllers
- Safe parameter parsing

### Environment Variables
- Sensitive data stored in environment variables
- No hardcoded credentials
- Support for Rails credentials

### Security Scanning
- Brakeman integration for Rails security
- CI security checks
- Dependency vulnerability scanning

## Performance Optimization

### Redis Optimization
- Connection pooling
- Namespace isolation
- Batch operations

### Background Processing
- Sidekiq queue management
- Error retry mechanisms
- Performance monitoring

### API Performance
- Efficient JSON serialization
- Response caching where appropriate
- Minimal database queries

## Troubleshooting

### Common Issues

1. **Redis Connection Failed**
   ```bash
   rails best_change:check_config
   # Verify Redis is running and accessible
   ```

2. **Engine Not Loading**
   ```ruby
   # Check if Rails is defined
   Rails.logger.info "BestChange loaded: #{defined?(BestChange::Engine)}"
   ```

3. **API Routes Not Available**
   ```bash
   bin/rails routes -g best_change
   # Should show engine routes
   ```

### Debug Mode

```ruby
# Enable detailed logging
BestChange.configure do |config|
  config.logger = Logger.new(STDOUT, level: :debug)
end
```

## Migration from Standalone Gem

### Breaking Changes
- None - maintains backward compatibility
- Rails components are optional
- Existing configuration continues to work

### Upgrade Steps

1. Update gem version
2. Run installation generator
3. Update configuration (optional)
4. Test API endpoints
5. Deploy with background workers

## Contributing to Engine Development

### Adding New API Endpoints

1. Create controller in `app/controllers/best_change/api/v1/`
2. Add route in `config/routes.rb`
3. Add corresponding tests
4. Update documentation

### Adding New Workers

1. Create worker in `lib/best_change/`
2. Follow Rails worker patterns
3. Add error handling
4. Create Rake task if needed
5. Add tests

### Configuration Changes

1. Update `Configuration` class
2. Add validation
3. Update initializer template
4. Update documentation
5. Test against all Rails versions