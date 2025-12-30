require 'sidekiq'

module BestChange
  class BatchLoadingWorker
    include ::Sidekiq::Worker
    include ::AutoLogger

    sidekiq_options queue: :best_change, retry: false

    def perform(exchange_rates, timestamp)
      api_key = Settings.bestchange_api_key
      bm = Benchmark.measure do
        `#{BestChange.configuration.fetcher_path} #{exchange_rates} #{timestamp} #{api_key}`
      end
      logger.info bm.real
    end
  end
end
