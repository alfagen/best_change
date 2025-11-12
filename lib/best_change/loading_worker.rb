require 'sidekiq'

module BestChange
  class LoadingWorker
    include ::Sidekiq::Worker
    include ::AutoLogger

    sidekiq_options queue: :critical, retry: false, lock: :while_executing

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
          BatchLoadingWorker.perform_async(rates.join('+'), time)

          api_limit_counter += 1
        end

        sleep 2

        BestChangeRatesExportWorker.perform_async
        GenerateCompositeStatusWorker.perform_async
      end

      logger.info bm.real
    end
  end
end
