# frozen_string_literal: true

module BestChange
  class TrusteeLoadingJob < ApplicationJob
    limits_concurrency to: 1, key: ->(_job) { 'best_change_trustee_loading' }, duration: 5.minutes

    def perform
      Gera::ExchangeRate.enabled.find_each do |exchange_rate|
        TrusteeSaverJob.perform_later(exchange_rate.id, time)
      end
    end

    private

    def time
      @time ||= Time.zone.now.to_i
    end
  end
end
