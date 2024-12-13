require 'sidekiq'

module BestChange
  class BatchLoadingWorker
    include ::Sidekiq::Worker
    include ::AutoLogger

    sidekiq_options queue: :critical, retry: false

    def perform(exchange_rates, timestamp, exchangers_json)
      go_program_path = File.expand_path("~/bestchange_fetcher/main")
      bm = Benchmark.measure do
        output = `#{go_program_path} #{exchange_rates} #{timestamp} #{exchangers_json}`
        logger.info output
      end

      logger.info bm.real
    end
  end
end
