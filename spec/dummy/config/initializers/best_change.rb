# BestChange Rails Engine initializer
Rails.application.configure do
  # Configure BestChange gem with Rails defaults
  BestChange.configure do |config|
    # Use Rails logger if available
    config.logger ||= Rails.logger

    # Default Redis connection from Rails config
    config.redis ||= Redis.new(
      host: ENV.fetch('REDIS_HOST', 'localhost'),
      port: ENV.fetch('REDIS_PORT', 6379),
      db: ENV.fetch('REDIS_DB', 0),
      password: ENV['REDIS_PASSWORD']
    )

    # Configuration from environment variables
    config.exchanger_id = ENV['BESTCHANGE_EXCHANGER_ID']&.to_i if ENV['BESTCHANGE_EXCHANGER_ID']
    config.valuta_access_log = ENV['BESTCHANGE_ACCESS_LOG'] if ENV['BESTCHANGE_ACCESS_LOG']
  end

  # Setup Sidekiq if available
  if defined?(Sidekiq)
    redis_url = "redis://#{ENV.fetch('REDIS_HOST', 'localhost')}:#{ENV.fetch('REDIS_PORT', 6379)}/#{ENV.fetch('REDIS_DB', 0)}"

    Sidekiq.configure_server do |sidekiq_config|
      sidekiq_config.redis = { url: redis_url }
    end

    Sidekiq.configure_client do |sidekiq_config|
      sidekiq_config.redis = { url: redis_url }
    end
  end
end