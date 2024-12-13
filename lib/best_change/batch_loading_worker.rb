require 'sidekiq'

module BestChange
  class BatchLoadingWorker
    include ::Sidekiq::Worker
    include ::AutoLogger

    sidekiq_options queue: :critical, retry: false

    def perform(exchange_rates, timestamp)
      go_program_path = File.expand_path("~/bestchange_fetcher/main")
      bm = Benchmark.measure { output = `#{go_program_path} #{exchange_rates} #{timestamp}` }

      logger.info "#{bm.real}: #{output}"
    end
  end
end
