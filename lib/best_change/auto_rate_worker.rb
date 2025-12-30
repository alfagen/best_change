module BestChange
  class AutoRateWorker
    include ::Sidekiq::Worker
    include ::AutoLogger

    sidekiq_options queue: :best_change, retry: false, lock: :until_executed

    def perform
      exchange_rate_ids1 = Gera::TargetAutorateSetting.where('updated_at >= ?', 30.seconds.ago).pluck(:exchange_rate_id)
      exchange_rate_ids2 = Gera::ExchangeRate.where('updated_at >= ?', 30.seconds.ago).pluck(:id)
      exchange_rate_ids = (exchange_rate_ids1 + exchange_rate_ids2)
      Gera::DirectionRateSnapshot.last.direction_rates.where(exchange_rate_id: exchange_rate_ids).each do |dr|
        dr.calculate_rate
        dr.save!
      end
    end
  end
end
