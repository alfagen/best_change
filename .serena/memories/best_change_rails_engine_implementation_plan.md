# BestChange Rails Engine Implementation Plan Session

## Project Context
- **Project**: BestChange gem (Ruby gem for currency exchange data from bestchange.ru)
- **Current State**: Standard Ruby gem with RSpec tests
- **Goal**: Transform into Rails Engine with comprehensive Rails testing capabilities
- **Session Date**: 2025-11-08

## Implementation Plan Overview

### Current Architecture Analysis
- **Main Service**: `lib/best_change/service.rb` - Central service for processing exchange rates
- **Repository**: Redis-based data repository using Oj for JSON serialization
- **Workers**: Sidekiq background workers (LoadingWorker, BatchLoadingWorker, etc.)
- **Dependencies**: Redis, Sidekiq, Gera (external gem), Virtus, Oj, Grape

### Target Architecture
- Rails Engine with `isolate_namespace BestChange`
- Dummy Rails application in `spec/dummy/` for testing
- Full Rails testing stack (controllers, integration, requests)
- Background jobs integration within Rails context

## 5-Phase Implementation Strategy

### Phase 1: Engine Foundation (2-3 days)
**Tasks:**
- Create `lib/best_change/engine.rb` inheriting from Rails::Engine
- Configure `isolate_namespace BestChange` and autoload paths
- Update `lib/best_change.rb` to load engine
- Update gemspec with Rails dependencies

### Phase 2: Dummy Application (2-3 days)
**Tasks:**
- Create `spec/dummy/` directory structure
- Create key dummy app files (boot.rb, application.rb, routes.rb)
- Configure dummy application for engine testing
- Set up test database schema and migrations

### Phase 3: Test Migration (3-5 days)
**Tasks:**
- Migrate existing RSpec tests to Rails environment
- Create `spec/rails_helper.rb` for Rails-specific tests
- Create new test types: controllers, integration, requests
- Adapt factories for dummy app

### Phase 4: Rails-Specific Features (2-4 days)
**Tasks:**
- Create routes and controllers (if needed)
- Add middleware and initializers
- Adapt Sidekiq workers for Rails context
- Create background job tests

### Phase 5: Integration & Documentation (1-2 days)
**Tasks:**
- Create installation and migration generators
- Update README.md with Rails integration instructions
- Update CI/CD for multiple Rails versions testing

## Session Recommendations

### Context Management Strategy
**Recommended per session:**
- **Maximum**: 2 phases (6-8 tasks)
- **Ideal**: 1 phase (3-4 tasks)
- **Quality-focused**: 3-5 key tasks

### Suggested Session Breakdown

**Session 1: Foundation** (Phase 1 + Phase 2 start)
```
1. Engine class creation
2. Main module update
3. Gemspec dependencies
4. Basic dummy app structure
```

**Session 2: Test Environment** (Phase 2 completion + Phase 3 start)
```
5. Complete dummy app configuration
6. rails_helper.rb and basic test migration
```

**Session 3: Testing** (Phase 3 completion)
```
7. Full test adaptation
8. Controller tests creation
```

## Technical Specifications

### Engine Class Structure
```ruby
module BestChange
  class Engine < ::Rails::Engine
    isolate_namespace BestChange
    
    config.generators do |g|
      g.test_framework :rspec
      g.fixture_replacement :factory_bot
      g.factory_bot dir: 'spec/factories'
    end
    
    initializer 'best_change.configuration' do |app|
      # Engine configuration
    end
  end
end
```

### Dummy Application Structure
- `spec/dummy/config/application.rb` - Main application class
- `spec/dummy/config/routes.rb` - Routes with engine mount
- `spec/dummy/config/database.yml` - Test database configuration
- `spec/dummy/db/schema.rb` - Database schema

### Rails Helper Configuration
```ruby
require 'spec_helper'
ENV['RAILS_ENV'] ||= 'test'
require File.expand_path('../dummy/config/environment', __FILE__)

require 'rspec/rails'
ActiveRecord::Migration.maintain_test_schema!

RSpec.configure do |config|
  config.use_transactional_fixtures = true
  config.infer_spec_type_from_file_location!
  config.filter_rails_from_backtrace!
  
  config.include BestChange::Engine.routes.url_helpers, type: :controller
  config.include BestChange::Engine.routes.url_helpers, type: :view
end
```

## Success Criteria
1. ✅ All existing tests continue working
2. ✅ New engine tests cover Rails functionality
3. ✅ Dummy application enables integration testing
4. ✅ CI/CD tests against multiple Rails versions
5. ✅ Documentation is clear and complete
6. ✅ Users can easily integrate engine

## Risk Mitigation
1. **Backward Compatibility**: Maintain old API, gradual migration
2. **Complexity**: Clear documentation, automated tests
3. **Performance**: Lazy loading, dependency optimization

## Timeline
- **Total**: 10-17 working days
- **Per Phase**: 1-5 days
- **Sessions**: 3-5 recommended sessions

## Next Steps
1. Create development branch
2. Start with Phase 1 - Engine foundation
3. Regular merges to main branch
4. Code reviews for each phase
5. Test backward compatibility at each step

## Memory Storage Notes
- Session context saved for project continuity
- TODO list maintained with 17 tasks across 5 phases
- Implementation plan detailed with code examples
- Session strategy defined for optimal context management