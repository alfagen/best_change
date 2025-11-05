source "https://rubygems.org"

git_source(:github) {|repo_name| "https://github.com/#{repo_name}" }

gem 'gera', github: 'alfagen/gera', branch: 'chore/upgrade-rails-to-8'

# Можно перейти в master если туда влили этот PR https://github.com/Netflix/fast_jsonapi/pull/230
# Данные изменения отлавливаются в spec/serializers/order_serializer_spec.rb
gem 'fast_jsonapi', github: 'HoJSim/fast_jsonapi', branch: 'dev'
# Specify your gem's dependencies in best_change.gemspec
gem "redis", "~> 4.0", :require => ["redis", "redis/connection/hiredis"]
gem "concurrent-ruby", "~> 1.3"

# Для gera
gem 'noty_flash', github: 'BrandyMint/noty_flash'


gemspec
