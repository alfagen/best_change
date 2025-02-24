# frozen_string_literal: true

module BestChange
  class ExnodeLoadingWorker
    include ::Sidekiq::Worker

    sidekiq_options retry: false, lock: :until_executed

    def perform
      Gera::ExchangeRate.available.includes(:payment_system_from, :payment_system_to).to_a.each_slice(250) do |chunk|
        params = []
        chunk.each do |exchange_rate|
          ps_from = exchange_rate.payment_system_from.letter_cod
          ps_to = exchange_rate.payment_system_to.letter_cod

          next if !ps_from.present? || !ps_to.present?

          params.push("#{ps_from}-#{ps_to}-#{exchange_rate.payment_system_from.bestchange_id}-#{exchange_rate.payment_system_to.bestchange_id}")
        end
        
        ExnodeSaverWorker.perform_async(params.join('#'), time)
      end
    end

    private

    def time
      @time ||= Time.zone.now.to_i
    end
  end
end
