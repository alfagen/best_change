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

  # Configure ActiveJob queue adapter
  config.active_job.queue_adapter = :async
end