# frozen_string_literal: true

module BestChange
  class LoadingJob < ApplicationJob
    include ::AutoLogger

    queue_as :best_change
    limits_concurrency to: 1, key: ->(*) { 'best_change_loading' }, duration: 5.minutes

    def perform
      bm = Benchmark.measure do
        exchange_rate_ids1 = Gera::TargetAutorateSetting.where('updated_at >= ?', 30.seconds.ago).pluck(:exchange_rate_id)
        exchange_rate_ids2 = Gera::ExchangeRate.where('updated_at >= ?', 30.seconds.ago).pluck(:id)
        exchange_rate_ids = (exchange_rate_ids1 + exchange_rate_ids2)
        Gera::DirectionRateSnapshot.last.direction_rates.where(exchange_rate_id: exchange_rate_ids).each do |dr|
          dr.calculate_rate
          dr.save!
        end

        BestChange.configuration.rates_export_job.perform
      end

      logger.info bm.real
    end
  end
end
