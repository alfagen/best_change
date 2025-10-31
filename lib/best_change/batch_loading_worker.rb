require 'sidekiq'

module BestChange
  class BatchLoadingWorker
    include ::Sidekiq::Worker
    include ::AutoLogger

    sidekiq_options queue: :critical, retry: false

    def perform(exchange_rates, timestamp)
      go_program_path = File.expand_path("~/bestchange_fetcher/main")
      api_key = Settings.bestchange_api_key
      bm = Benchmark.measure do
        output = `#{go_program_path} #{exchange_rates} #{timestamp} #{api_key}`
      end

      logger.info bm.real
    end
  end
end
