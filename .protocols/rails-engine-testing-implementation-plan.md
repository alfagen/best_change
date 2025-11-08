# План имплементации тестирования BestChange gem как Rails Engine

## Обзор проекта

BestChange gem - это Ruby gem для работы с данными bestchange.ru (агрегатор курсов валют). В данный момент проект имеет структуру обычного gem с RSpec тестами, но для полноценного тестирования Rails функциональности необходимо реорганизовать его в Rails Engine с dummy приложением.

## Текущая архитектура

### Существующая структура
```
best_change/
├── lib/
│   ├── best_change/
│   │   ├── configuration.rb
│   │   ├── loading_worker.rb
│   │   ├── service.rb
│   │   ├── repository.rb
│   │   ├── record.rb
│   │   ├── row.rb
│   │   ├── status.rb
│   │   └── ...
├── spec/
│   ├── best_change/
│   ├── factories/
│   ├── support/
│   └── vcr_cassettes/
└── best_change.gemspec
```

### Проблемы текущей структуры
1. Отсутствует полноценное Rails окружение для тестирования
2. Нельзя тестировать integration с Rails приложением
3. Нет возможности тестировать routing, middleware, controllers
4. Ограниченная возможность тестирования background jobs в Rails контексте

## Целевая архитектура

### Новая структура проекта
```
best_change/
├── lib/
│   ├── best_change.rb
│   ├── best_change/
│   │   ├── engine.rb          # Новый файл - Rails Engine
│   │   ├── configuration.rb
│   │   ├── loading_worker.rb
│   │   ├── service.rb
│   │   └── ...
│   └── generators/
│       └── install/          # Генератор установки
├── spec/
│   ├── dummy/                 # Dummy Rails приложение
│   │   ├── app/
│   │   │   ├── controllers/
│   │   │   ├── models/
│   │   │   └── ...
│   │   ├── config/
│   │   │   ├── application.rb
│   │   │   ├── routes.rb
│   │   │   └── ...
│   │   ├── db/
│   │   │   └── migrate/
│   │   └── ...
│   ├── best_change/          # Существующие тесты gem
│   ├── controllers/          # Controller тесты (новые)
│   ├── models/               # Model тесты (новые)
│   ├── integration/          # Integration тесты
│   ├── requests/             # Request тесты
│   ├── factories/            # Factories
│   ├── support/              # Support файлы
│   └── rails_helper.rb       # Rails хелперы
├── app/                      # Если нужен UI
│   ├── controllers/          # Контроллеры engine
│   ├── models/               # Модели engine
│   └── views/                # Views engine
├── config/
│   └── routes.rb             # Routes engine
└── db/
    └── migrate/              # Migrations для engine
```

## План имплементации

### Этап 1: Подготовка основы Engine

**1.1 Создание Engine класса**
- [ ] Создать `lib/best_change/engine.rb`
- [ ] Наследовать от `Rails::Engine`
- [ ] Настроить `isolate_namespace BestChange`
- [ ] Добавить autoload путей

**1.2 Модификация основного модуля**
- [ ] Обновить `lib/best_change.rb` для загрузки engine
- [ ] Добавить engine.require в основной файл
- [ ] Обеспечить backward compatibility

**1.3 Обновление gemspec**
- [ ] Добавить Rails зависимости в runtime
- [ ] Обновить development зависимости для Rails тестирования:
  ```ruby
  spec.add_development_dependency "rspec-rails"
  spec.add_development_dependency "factory_bot_rails"
  ```
- [ ] Добавить rake задачи для engine

### Этап 2: Создание Dummy приложения

**2.1 Создание структуры вручную**

**Создание иерархии директорий:**
```bash
mkdir -p spec/dummy/{app/{controllers,models,views,helpers,jobs/mailers},config/{environments,initializers,locales},db/{migrate,seeds},lib/{assets,tasks},log,test,tmp,bin}
```

**Ключевые файлы dummy приложения:**
- [ ] `spec/dummy/config/boot.rb` - загружает Bundler
- [ ] `spec/dummy/config/application.rb` - основной класс приложения
- [ ] `spec/dummy/config/routes.rb` - маршруты с mount BestChange::Engine
- [ ] `spec/dummy/config/database.yml` - конфигурация БД для тестов
- [ ] `spec/dummy/config/environments/test.rb` - окружение для тестов
- [ ] `spec/dummy/config/secrets.yml` - секреты (можно пустой)
- [ ] `spec/dummy/Rakefile` - задачи Rails
- [ ] `spec/dummy/config.ru` - для Rack сервера
- [ ] `spec/dummy/Gemfile` - зависимости dummy app (может быть пустым, т.к. использует gemspec)
- [ ] `spec/dummy/db/schema.rb` - схема БД

**Добавить engine файлы в существующую структуру**
- [ ] Создать `lib/best_change/engine.rb`
- [ ] Добавить engine файлы в существующую структуру
- [ ] Настроить dummy app для тестирования engine

**2.2 Конфигурация Dummy приложения**
- [ ] Настроить `spec/dummy/config/application.rb`
- [ ] Настроить `spec/dummy/config/routes.rb` с mount engine
- [ ] Настроить database.yml для тестов
- [ ] Добавить необходимые initializers

**2.3 Тестовая база данных**
- [ ] Создать `spec/dummy/db/schema.rb`
- [ ] Настроить миграции для тестовых данных
- [ ] Создать rake задачи для управления БД

### Этап 3: Миграция тестовой структуры

**3.1 Адаптация существующих тестов**
- [ ] Перенести RSpec тесты в `spec/`
- [ ] Обновить `spec/spec_helper.rb` для Rails environment
- [ ] Создать `spec/rails_helper.rb` для Rails специфичных тестов
- [ ] Адаптировать factories для dummy app

**3.2 Создание новых типов тестов**
- [ ] Controller тесты в `spec/controllers/`
- [ ] Integration тесты в `spec/integration/`
- [ ] Request тесты в `spec/requests/`
- [ ] Model тесты для Rails моделей (если необходимо)

**3.3 Поддержка Minitest (опционально)**
- [ ] Создать `test/test_helper.rb`
- [ ] Настроить Minitest для Rails environment
- [ ] Добавить Minitest тесты при необходимости

### Этап 4: Rails специфичная функциональность

**4.1 Routes и controllers (если необходимо)**
- [ ] Создать `config/routes.rb` для engine
- [ ] Определить необходимые controllers
- [ ] Создать controller тесты

**4.2 Middleware и configuration**
- [ ] Добавить middleware для engine
- [ ] Создать initializers
- [ ] Настроить engine configuration

**4.3 Background jobs integration**
- [ ] Адаптировать Sidekiq workers для Rails контекста
- [ ] Создать тесты для background jobs
- [ ] Настроить тестовую очередь

### Этап 5: Интеграция и документация

**5.1 Generators**
- [ ] Создать генератор установки
- [ ] Добавить генератор миграций
- [ ] Создать install tasks

**5.2 Documentation**
- [ ] Обновить README.md с инструкциями по Rails integration
- [ ] Создать документацию по engine usage
- [ ] Добавить примеры использования

**5.3 CI/CD обновление**
- [ ] Обновить `.github/workflows/ci.yml`
- [ ] Добавить тестирование против multiple Rails версий
- [ ] Настроить testing against multiple Ruby версий

## Детальная имплементация

### Engine класс (`lib/best_change/engine.rb`)

```ruby
module BestChange
  class Engine < ::Rails::Engine
    isolate_namespace BestChange

    # Генераторы
    config.generators do |g|
      g.test_framework :rspec
      g.fixture_replacement :factory_bot
      g.factory_bot dir: 'spec/factories'
    end

    # Initializers
    initializer 'best_change.configuration' do |app|
      # Конфигурация engine
    end

    # Tasks
    rake_tasks do
      load 'tasks/best_change_tasks.rake'
    end
  end
end
```

### Dummy application setup

**spec/dummy/config/application.rb**
```ruby
require_relative 'boot'

require 'rails/all'

Bundler.require(*Rails.groups)

module Dummy
  class Application < Rails::Application
    # Initialize configuration defaults for originally generated Rails version.
    config.load_defaults 7.0  # конкретная версия Rails

    # Configuration for the application, engines, and railties goes here.
    config.eager_load_paths << Rails.root.join('lib')

    # Mount BestChange engine
    config.hosts.clear
  end
end
```

**spec/dummy/config/routes.rb**
```ruby
Rails.application.routes.draw do
  mount BestChange::Engine => "/best_change"
end
```

### Специализированные тестовые хелперы

**spec/rails_helper.rb**
```ruby
require 'spec_helper'
ENV['RAILS_ENV'] ||= 'test'
require File.expand_path('../dummy/config/environment', __FILE__)
abort("The Rails environment is running in production mode!") if Rails.env.production?

require 'rspec/rails'

# Requires supporting ruby files with custom matchers and macros, etc,
# in spec/support/ and its subdirectories.
Dir[Rails.root.join('spec/support/**/*.rb')].each { |f| require f }

# Checks for pending migrations and applies them before tests are run.
ActiveRecord::Migration.maintain_test_schema!

RSpec.configure do |config|
  # Transactional fixtures для очистки базы данных
  config.use_transactional_fixtures = true
  config.infer_spec_type_from_file_location!
  config.filter_rails_from_backtrace!

  
  # Engine специфичные хелперы
  config.include BestChange::Engine.routes.url_helpers, type: :controller
  config.include BestChange::Engine.routes.url_helpers, type: :view
end
```

### Controller тесты пример

**spec/controllers/best_change/exchange_rates_controller_spec.rb**
```ruby
require 'rails_helper'

RSpec.describe BestChange::ExchangeRatesController, type: :controller do
  routes { BestChange::Engine.routes }

  describe 'GET #index' do
    it 'returns a successful response' do
      get :index
      expect(response).to be_successful
    end

    it 'loads exchange rates' do
      # Mock BestChange service
      allow(BestChange::Service).to receive(:load_rates)

      get :index
      expect(assigns(:rates)).to be_present
    end
  end
end
```

### Integration тесты пример

**spec/integration/best_change_integration_spec.rb**
```ruby
require 'rails_helper'

RSpec.describe 'BestChange integration', type: :request do
  before do
    # Настройка тестовых данных
  end

  it 'loads and processes exchange rates' do
    get '/best_change/exchange_rates'

    expect(response).to have_http_status(:success)
    expect(JSON.parse(response.body)).to be_an(Array)
  end

  it 'handles service integration correctly' do
    # Mock сервис
    allow(BestChange::Service).to receive(:load_rates).and_return([])

    get '/best_change/exchange_rates'
    expect(response).to have_http_status(:success)
  end
end
```

## Преимущества нового подхода

1. **Полноценное Rails тестирование**: Возможность тестировать integration с Rails
2. **Изоляция неймспейсов**: `isolate_namespace` предотвращает конфликты
3. **Правильные роуты**: Тестирование routing в контексте engine
4. **Background jobs**: Тестирование Sidekiq workers в Rails окружении
5. **Middleware testing**: Возможность тестирования Rails middleware
6. **Development workflow**: Лучшая разработка с Rails консолью
7. **Documentation**: Более понятная структура для пользователей

## Риски и митигация

1. **Backward compatibility**:
   - Риск: Сломать существующий код
   - Митигация: Поддерживать старый API, постепенная миграция

2. **Сложность поддержки**:
   - Риск: Увеличение сложности проекта
   - Митигация: Четкая документация, автоматизированные тесты

3. **Производительность**:
   - Риск: Замедление загрузки из-за Rails
   - Митигация: Ленивая загрузка, оптимизация зависимостей

## Временные затраты

- **Этап 1**: 2-3 дня
- **Этап 2**: 2-3 дня
- **Этап 3**: 3-5 дней
- **Этап 4**: 2-4 дня
- **Этап 5**: 1-2 дня

**Итого**: 10-17 рабочих дней

## Критерии успеха

1. ✅ Все существующие тесты продолжают работать
2. ✅ Новые engine тесты покрывают Rails функциональность
3. ✅ Dummy приложение позволяет тестировать integration
4. ✅ CI/CD успешно тестирует против multiple Rails версий
5. ✅ Документация понятна и полна
6. ✅ Пользователи могут легко интегрировать engine в свои приложения

## Следующие шаги

1. Создать分支 для development
2. Начать с Этапа 1 - Engine основа
3. Регулярно мержать изменения в основную ветвь
4. Проводить code review для каждого этапа
5. Тестировать backward compatibility на каждом шаге