namespace :best_change do
  desc "Load BestChange exchange rates"
  task load_rates: :environment do
    if defined?(Sidekiq)
      BestChange::RailsLoadingWorker.perform_async
      puts "BestChange loading job enqueued"
    else
      puts "Sidekiq is not available. Please add 'sidekiq' to your Gemfile."
    end
  end

  desc "Load BestChange exchange rates synchronously"
  task load_rates_sync: :environment do
    puts "Loading BestChange rates synchronously..."
    BestChange::RailsLoadingWorker.new.perform
    puts "BestChange rates loaded successfully"
  end

  desc "Check BestChange configuration"
  task check_config: :environment do
    puts "BestChange Configuration Check:"
    puts "=" * 40

    config = BestChange.configuration
    if config.nil?
      puts "❌ Configuration not set. Please configure BestChange first."
      exit 1
    end

    puts "✅ Configuration loaded"

    # Check Redis connection
    if config.redis
      begin
        config.redis.ping
        puts "✅ Redis connection: OK"
      rescue => e
        puts "❌ Redis connection failed: #{e.message}"
      end
    else
      puts "❌ Redis connection not configured"
    end

    # Check exchanger ID
    if config.exchanger_id
      puts "✅ Exchanger ID: #{config.exchanger_id}"
    else
      puts "❌ Exchanger ID not configured"
    end

    # Check fetcher path
    if config.fetcher_path
      full_path = config.fetcher_full_path
      if File.exist?(full_path)
        puts "✅ Fetcher path: #{full_path}"
      else
        puts "⚠️  Fetcher path exists but file not found: #{full_path}"
      end
    end

    puts "=" * 40
  end

  desc "Start BestChange monitoring"
  task start_monitoring: :environment do
    puts "Starting BestChange monitoring..."

    if defined?(Sidekiq::Cron)
      # Set up cron job for periodic loading
      Sidekiq::Cron::Job.create(name: 'best_change_monitoring',
                                cron: '*/5 * * * *',
                                class: 'BestChange::RailsLoadingWorker')
      puts "BestChange monitoring job scheduled to run every 5 minutes"
    else
      puts "Sidekiq::Cron not available. Please add 'sidekiq-cron' to your Gemfile for automatic monitoring."
    end
  end

  desc "Stop BestChange monitoring"
  task stop_monitoring: :environment do
    if defined?(Sidekiq::Cron)
      job = Sidekiq::Cron::Job.find('best_change_monitoring')
      if job
        job.destroy
        puts "BestChange monitoring job stopped"
      else
        puts "No monitoring job found"
      end
    else
      puts "Sidekiq::Cron not available"
    end
  end

  desc "Clear BestChange data from Redis"
  task clear_data: :environment do
    config = BestChange.configuration
    if config&.redis
      # Clear BestChange keys from Redis
      keys = config.redis.keys("best_change:*")
      if keys.any?
        config.redis.del(*keys)
        puts "Cleared #{keys.size} BestChange keys from Redis"
      else
        puts "No BestChange keys found in Redis"
      end
    else
      puts "Redis not configured"
    end
  end
end