require 'sidekiq'

module BestChange
  class LoadingWorker
    include ::Sidekiq::Worker
    include ::AutoLogger

    sidekiq_options queue: :critical, retry: false, lock: :until_executed

    EXCHANGERS_BASE_URL = 'https://www.bestchange.app/v2/fe3fc3f28942d637d70cd09bc1c2b978/changers/ru'
    def perform
      uri = URI(EXCHANGERS_BASE_URL)
      response = Net::HTTP.get(uri)
      exchangers = {}
      JSON.parse(response)['changers'].each do |e|
        exchangers[e['id']] = e['name']
      end
      exchangers_json = exchangers.to_json
      all_rates = []
      Gera::ExchangeRate.includes(:payment_system_from, :payment_system_to).find_each(batch_size: 499) do |e|
        id_from = e.payment_system_from.bestchange_id
        id_to = e.payment_system_to.bestchange_id
        next if id_from.blank? || id_from == 0
        next if id_to.blank? || id_to == 0

        all_rates << "#{id_from}-#{id_to}"
      end

      time = Time.zone.now.to_i
      all_rates.each_slice(499).each do |rates|
        BatchLoadingWorker.perform_async(rates.join('+'), time, exchangers_json)
      end

      sleep 3

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
