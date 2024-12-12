require 'sidekiq'

module BestChange
  class LoadingWorker
    include ::Sidekiq::Worker
    include ::AutoLogger

    sidekiq_options queue: :critical, retry: false, lock: :until_executed

    def perform
      go_program_path = File.expand_path("~/bestchange_fetcher/main")
      output = `#{go_program_path}`

      logger.info output
      if $?.success?
        logger.info 'DONE'
        exchange_rate_ids1 = Gera::TargetAutorateSetting.where('updated_at >= ?', 2.minutes.ago).pluck(:exchange_rate_id)
        exchange_rate_ids2 = Gera::ExchangeRate.where('updated_at >= ?', 2.minutes.ago).pluck(:id)
        exchange_rate_ids = (exchange_rate_ids1 + exchange_rate_ids2)
        Gera::DirectionRateSnapshot.last.direction_rates.where(exchange_rate_id: exchange_rate_ids).each do |dr|
          dr.calculate_rate
          dr.save!
        end
      end
    end
  end
end
