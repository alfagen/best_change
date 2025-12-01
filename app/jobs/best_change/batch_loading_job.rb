# frozen_string_literal: true

module BestChange
  class BatchLoadingJob < ApplicationJob
    include ::AutoLogger

    queue_as :critical

    def perform(exchange_rates, timestamp)
      `#{BestChange.configuration.fetcher_path} #{exchange_rates} #{timestamp}`
    end
  end
end
