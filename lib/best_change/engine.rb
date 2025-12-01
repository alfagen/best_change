require 'rails/engine'

module BestChange
  class Engine < ::Rails::Engine
    isolate_namespace BestChange

    # Configuration defaults
    config.after_initialize do |app|
      BestChange.configure do |config|
        config.redis ||= app.config.redis if app.config.respond_to?(:redis)
        config.logger ||= Rails.logger
      end
    end

    # Load background jobs
    initializer 'best_change.background_jobs' do
      # Auto-load ActiveJob jobs
      Dir[File.join(File.dirname(__FILE__), '../../app/jobs/best_change/*.rb')].sort.each do |file|
        require file
      end
    end

    # Load generators
    generators do
      require File.expand_path('../generators/best_change/install/install_generator', __dir__)
    end
  end
end
