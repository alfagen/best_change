# frozen_string_literal: true

module BestChange
  class TrusteeLoadingWorker
    include ::Sidekiq::Worker

    sidekiq_options retry: false, lock: :until_executed

    def perform
      Gera::ExchangeRate.enabled.find_each do |exchange_rate|
        TrusteeSaverWorker.perform_async(exchange_rate.id, time)
      end
    end

    private

    def time
      @time ||= Time.zone.now.to_i
    end
  end
end
