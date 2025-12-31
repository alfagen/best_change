# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BestChange is a Ruby gem that provides an adapter and utilities to work with data from bestchange.ru (a Russian currency exchange aggregator). The gem:

* Loads exchange rates from BestChange
* Determines commission rates of other exchanges relative to base rates
* Determines the status of exchange rates in BestChange (whether they're competitive)

## Architecture

### Core Components

**Main Service (`lib/best_change/service.rb`)**
- Central service that processes exchange rate data
- Calculates commission percentages using `Gera::Mathematic`
- Builds ranked lists of exchanges with their rates and positions
- Determines if current rates are competitive (`Status::STATE_ACTUAL` or `Status::STATE_AWAIT`)

**Repository Pattern (`lib/best_change/repository.rb`)**
- Redis-based data repository using Oj for JSON serialization
- `getRows(key)` - retrieves exchange rate data by currency pair key
- `setRows(key, data)` - stores exchange rate data in Redis
- Keys are formatted as `"source-ID1-ID2"` where IDs are BestChange payment system IDs

**Data Models**
- `Record` - Represents an exchange rate record with commission data
- `Row` - Raw BestChange data row
- `Status` - Status of an exchange rate (actual/await)
- `Configuration` - Redis connection and exchanger ID settings

### Background Jobs (ActiveJob)

**TrusteeLoadingJob/TrusteeSaverJob** - Handle trustee-related data processing

## Development Commands

### Running Tests
```bash
# Note: Dependencies are currently outdated and may need updating
bundle install  # May fail due to outdated gems
bundle exec rake spec  # Run RSpec tests
rspec  # Alternative test runner
```

### Development Setup
```bash
bin/setup  # Install dependencies
bin/console  # Interactive console for experimentation
```

### Building and Installation
```bash
bundle exec rake install  # Install gem locally
bundle exec rake release  # Create git tag and push to RubyGems
```

### Code Quality
```bash
rubocop  # Run Ruby linter
yard  # Generate documentation
```

## Dependencies

The project uses several key dependencies:
- **Redis** - Data storage and caching
- **ActiveJob** - Background job processing (Rails built-in)
- **Gera** - External gem for currency exchange logic (GitHub dependency)
- **Virtus** - Attributes and models
- **Oj** - Fast JSON parsing
- **Grape** - API framework
- **Money/MoneyRails** - Currency handling

## Configuration

Configure via `BestChange.configure` block:

```ruby
BestChange.configure do |config|
  config.redis = Redis.new(url: 'redis://localhost:6379')
  config.exchanger_id = 123  # Your BestChange exchanger ID
  config.valuta_access_log = 'path/to/log/file'
end
```

## Key Patterns

### Data Flow
1. Data is processed and stored in Redis via `Repository`
2. `Service` retrieves data, calculates commissions, and determines competitiveness
3. Results are serialized and made available for API consumption

### Key Calculations
- Commission calculation: `calculate_comission(row.rate, base_rate_multiplicator)`
- Rate comparison against base rates from `Gera::Universe.currency_rates_repository`
- Position ranking and status determination

## Testing

Tests are located in `spec/` directory with RSpec framework. Key test files:
- `spec/best_change/service_spec.rb` - Core service logic tests
- `spec/support/` - Test helpers and stubs

Note: The project has outdated dependencies and may need bundle updates before tests can run successfully.

## Russian Comments

This codebase contains Russian comments and variable names, reflecting the Russian exchange market context of BestChange.ru.