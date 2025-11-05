require 'sidekiq'

module BestChange
  class BatchLoadingWorker
    include ::Sidekiq::Worker
    include ::AutoLogger

    sidekiq_options queue: :critical, retry: false

    def perform(exchange_rates, timestamp)
      bm = Benchmark.measure do
        `#{BestChange.configuration.fetcher_full_path} #{exchange_rates} #{timestamp}`
      end

      logger.info bm.real
    end
  end
end
