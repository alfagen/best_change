module BestChange
  module Api
    module V1
      class CurrencyPairsController < ApplicationController
        def index
          pairs = BestChange::Service.new.get_available_currency_pairs

          render_success(
            data: pairs.map(&:to_hash),
            count: pairs.size
          )
        rescue StandardError => e
          render_error(e.message, :internal_server_error)
        end

        def available
          service = BestChange::Service.new
          available_pairs = service.get_available_currency_pairs

          render_success(
            data: available_pairs.map { |pair|
              {
                id_from: pair.id_from,
                id_to: pair.id_to,
                name_from: pair.name_from,
                name_to: pair.name_to
              }
            },
            count: available_pairs.size
          )
        rescue StandardError => e
          render_error(e.message, :internal_server_error)
        end
      end
    end
  end
end