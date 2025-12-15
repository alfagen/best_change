require 'sidekiq'

module BestChange
  class LoadingWorker
    include ::Sidekiq::Worker
    include ::AutoLogger

    sidekiq_options queue: :critical, retry: false, lock: :until_executed

    def perform
      bm = Benchmark.measure do
        all_rates = []
        Gera::ExchangeRate.includes(:payment_system_from, :payment_system_to).find_each(batch_size: 499) do |e|
          id_from = e.payment_system_from.bestchange_id
          id_to = e.payment_system_to.bestchange_id
          next if id_from.blank? || id_from == 0
          next if id_to.blank? || id_to == 0

          all_rates << "#{id_from}-#{id_to}"
        end

        time = Time.zone.now.to_i
        api_limit_counter = 0
        all_rates.each_slice(499) do |rates|
            logger.info(
              worker: 'BestChange::BatchLoadingWorker',
              args: args
            )

          BatchLoadingWorker.perform_async(rates.join('+'), time)

          api_limit_counter += 1
          sleep 2 if api_limit_counter % 10 == 0
        end

        sleep 10

        exchange_rate_ids1 = Gera::TargetAutorateSetting.where('updated_at >= ?', 30.seconds.ago).pluck(:exchange_rate_id)
        exchange_rate_ids2 = Gera::ExchangeRate.where('updated_at >= ?', 30.seconds.ago).pluck(:id)
        exchange_rate_ids = (exchange_rate_ids1 + exchange_rate_ids2)
        Gera::DirectionRateSnapshot.last.direction_rates.where(exchange_rate_id: exchange_rate_ids).each do |dr|
          dr.calculate_rate
          dr.save!
        end
      end

      logger.info bm.real
    end
  end
end
