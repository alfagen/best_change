# frozen_string_literal: true

module BestChange
  class BatchLoadingJob < ApplicationJob
    include ::AutoLogger

    queue_as :best_change

    def perform(exchange_rates, timestamp)
      `#{BestChange.configuration.fetcher_path} #{exchange_rates} #{timestamp}`
    end
  end
end
