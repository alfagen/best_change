# Тестирование Ruby Gems с Rails Engine

Руководство по тестированию Ruby gems, которые добавляют функциональность Rails через engine, используя dummy приложения.

## Основные подходы к тестированию Rails Engine

### 1. Создание Engine с Dummy App

**Генерация engine с поддержкой тестирования:**
```bash
# Базовая команда для создания mountable engine
rails plugin new my_engine --mountable

# С RSpec и кастомным dummy path
rails plugin new my_engine --mountable -T --dummy-path=spec/dummy

# С PostgreSQL вместо SQLite
rails plugin new my_engine --mountable --dummy-path=spec/dummy -d postgresql

# Полный пример с флагами
rails plugin new my_engine \
  --mountable \
  --dummy-path=spec/dummy \
  -T \
  --skip-keeps \
  -d postgresql
```

### 2. Структура проекта

```
my_engine/
├── app/
├── config/
├── lib/
├── spec/
│   ├── dummy/           # Тестовое Rails приложение
│   │   ├── app/
│   │   ├── config/
│   │   │   ├── routes.rb
│   │   │   └── environments/
│   │   └── db/
│   ├── spec_helper.rb
│   ├── rails_helper.rb
│   └── support/
├── my_engine.gemspec
└── Gemfile
```

### 3. Конфигурация gemspec

```ruby
# my_engine.gemspec
Gem::Specification.new do |spec|
  spec.name          = "my_engine"
  spec.version       = MyEngine::VERSION
  spec.authors       = ["Your Name"]

  spec.files         = Dir["{app,config,db,lib}/**/*", "MIT-LICENSE", "Rakefile", "README.md"]

  spec.add_dependency "rails", "~> 7.0"

  # Тестовые зависимости
  spec.add_development_dependency "rspec-rails"
  spec.add_development_dependency "factory_bot_rails"
end
```

### 4. Настройка RSpec для Engine

**spec/rails_helper.rb:**
```ruby
require 'spec_helper'

# Загрузка dummy приложения
ENV['RAILS_ENV'] ||= 'test'
require File.expand_path('../dummy/config/environment', __FILE__)

# Проверка миграций
abort("The Rails environment is running in production mode!") if Rails.env.production?

require 'rspec/rails'

# Дополнительные requires
# Capybara не требуется, если нет feature/System тестов

# Настройки
Dir[Rails.root.join('spec/support/**/*.rb')].each { |f| require f }

RSpec.configure do |config|
  config.use_transactional_fixtures = true
  config.infer_spec_type_from_file_location!
  config.filter_rails_from_backtrace!
end
```

**spec/spec_helper.rb:**
```ruby
require 'rubygems'
require 'bundler/setup'

# Загрузка самого engine
require 'my_engine'

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
end
```

### 5. Конфигурация Dummy App

**spec/dummy/config/routes.rb:**
```ruby
Rails.application.routes.draw do
  # Монтирование engine для тестов
  mount MyEngine::Engine => "/my_engine"
end
```

**spec/dummy/config/application.rb:**
```ruby
require_relative 'boot'

require "rails/all"

Bundler.require(*Rails.groups)

module Dummy
  class Application < Rails::Application
    config.load_defaults Rails::VERSION::STRING.to_f

    # Минимальная конфигурация для тестов
    config.eager_load = false
    config.session_store :cookie_store, key: '_dummy_session'
  end
end
```

### 6. Написание тестов

**Тестирование контроллеров:**
```ruby
# spec/controllers/my_engine/articles_controller_spec.rb
require 'rails_helper'

RSpec.describe MyEngine::ArticlesController, type: :controller do
  routes { MyEngine::Engine.routes }

  describe "GET #index" do
    it "returns a successful response" do
      get :index
      expect(response).to be_successful
    end
  end

  describe "POST #create" do
    it "creates a new article" do
      expect {
        post :create, params: { article: { title: "Test Article" } }
      }.to change(Article, :count).by(1)
    end
  end
end
```

**Тестирование моделей:**
```ruby
# spec/models/my_engine/article_spec.rb
require 'rails_helper'

RSpec.describe MyEngine::Article, type: :model do
  it "is valid with valid attributes" do
    article = MyEngine::Article.new(title: "Test", content: "Content")
    expect(article).to be_valid
  end

  it "is invalid without title" do
    article = MyEngine::Article.new(content: "Content")
    expect(article).not_to be_valid
  end
end
```

**Request тесты (альтернатива feature тестам):**
```ruby
# spec/requests/articles_management_spec.rb
require 'rails_helper'

RSpec.describe "Articles Management", type: :request do
  describe "POST /my_engine/articles" do
    it "creates a new article" do
      headers = { "CONTENT_TYPE" => "application/json" }
      params = {
        article: {
          title: "Test Article",
          content: "This is test content"
        }
      }.to_json

      post "/my_engine/articles", params: params, headers: headers

      expect(response).to have_http_status(:created)
      expect(JSON.parse(response.body)["title"]).to eq("Test Article")
    end
  end
end
```

### 7. Очистка базы данных в тестах

**Транзакционные фикстуры (рекомендуется):**
```ruby
# spec/rails_helper.rb
RSpec.configure do |config|
  config.use_transactional_fixtures = true
end
```

**Database Cleaner (если нужна сложная логика):**
Если транзакционные фикстуры не подходят (например, для тестов с треггерами базы данных или внешними сервисами):
```ruby
# Gemfile
group :test do
  gem 'database_rewinder' # или database_cleaner-active_record
end

# spec/rails_helper.rb
RSpec.configure do |config|
  config.use_transactional_fixtures = false

  config.before :suite do
    DatabaseRewinder.clean_all
  end

  config.after :each do
    DatabaseRewinder.clean
  end
end
```

### 8. Альтернатива: Gem Combustion

**Gemfile добавление:**
```ruby
group :test do
  gem 'combustion', '~> 1.3'
end
```

**spec/spec_helper.rb с Combustion:**
```ruby
require 'combustion'
Combustion.initialize! :all

require 'rspec/rails'

RSpec.configure do |config|
  config.use_transactional_fixtures = true
end
```

### 8. Подготовка базы данных для тестов

**spec/dummy/config/database.yml:**
```yaml
test:
  adapter: sqlite3
  database: db/test.sqlite3
  pool: 5
  timeout: 5000
```

**Создание миграций:**
```bash
# В корне engine
cd spec/dummy
rails db:create db:migrate
```

### 9. Тестирование генераторов

Если ваш engine содержит генераторы:
```ruby
# spec/generators/my_engine/install_generator_spec.rb
require 'rails_helper'
require 'generators/my_engine/install_generator'

RSpec.describe MyEngine::InstallGenerator, type: :generator do
  destination File.expand_path('../../tmp', __FILE__)

  before do
    prepare_destination
    run_generator
  end

  it 'creates the initializer file' do
    assert_file 'config/initializers/my_engine.rb'
  end
end
```

### 10. Запуск тестов

```bash
# Запуск всех тестов
bundle exec rspec

# Запуск с покрытием кода
bundle exec rspec --format documentation

# Запуск тестов для конкретной части
bundle exec rspec spec/models
```

## Лучшие практики

1. **Изоляция тестов:** Используйте транзакции и database_cleaner для изоляции тестов
2. **Factory Bot:** Используйте фабрики вместо fixtures для создания тестовых данных
3. **Правильные routes:** В тестах контроллеров всегда указывайте `routes { MyEngine::Engine.routes }`
4. **Минимальная конфигурация:** Dummy app должна иметь только необходимую конфигурацию
5. **Тестирование интеграции:** Проверяйте работу engine в контексте Rails приложения

Эта структура позволяет тестировать engine в полноценном Rails окружении, обеспечивая при этом изоляцию и правильную конфигурацию зависимостей.