module BestChange
  module Api
    module V1
      class ExchangeRatesController < ApplicationController
        def index
          service = BestChange::Service.new
          rates = service.get_all_rates

          render_success(
            data: rates.map(&:to_hash),
            count: rates.size
          )
        rescue StandardError => e
          render_error(e.message, :internal_server_error)
        end

        def status
          service = BestChange::Service.new
          status_data = service.get_status_summary

          render_success(data: status_data)
        rescue StandardError => e
          render_error(e.message, :internal_server_error)
        end

        def competitive
          service = BestChange::Service.new
          competitive_rates = service.get_competitive_rates

          render_success(
            data: competitive_rates.map(&:to_hash),
            count: competitive_rates.size
          )
        rescue StandardError => e
          render_error(e.message, :internal_server_error)
        end
      end
    end
  end
end