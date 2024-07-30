# frozen_string_literal: true

module BestChange
  class TrusteeSaverWorker
    include ::Sidekiq::Worker

    sidekiq_options queue: :critical, retry: 3

    BASE_URL = 'https://api.v3.trustee.deals/data/all'
    TOKEN_NETWORK_TO_CURRENCY = {
      'trc20' => 'TRX_USDT',
      'erc20' => 'ETH_USDT'
    }.freeze

    def perform(exchange_rate_id, timestamp)
      exchange_rate = Gera::ExchangeRate.find(exchange_rate_id)
      ps1 = exchange_rate.income_payment_system.bestchange_id
      ps2 = exchange_rate.outcome_payment_system.bestchange_id
      key = BestChange::Repository.generate_key_from_bestchange_ids ps1, ps2, 'trustee'
      in_currency = format_currency(exchange_rate.income_payment_system)
      out_currency = format_currency(exchange_rate.outcome_payment_system)

      url = "#{BASE_URL}?inCurrencyCode=#{in_currency}&outCurrencyCode=#{out_currency}"
      uri = URI(url)
      response = Net::HTTP.get(uri)
      data = JSON.parse(response)
      rates = data['sell'].map do |rate|
        BestChange::Row.new(
          exchanger_id:   rate['exchangeWayId'],
          exchanger_name: rate['provider'],
          buy_price:      1,
          sell_price:     rate['exchangeRate']['exchangeRate'],
          reserve:        rate['limits']['max'],
          time:           timestamp
        )
      end

      BestChange::Repository.setRows key, rates.sort
    end

    private

    def format_currency(payment_system)
      currency = payment_system.currency.to_s.downcase
      return currency.upcase unless currency.inquiry.usdt?

      TOKEN_NETWORK_TO_CURRENCY[payment_system.token_network]
    end
  end
end
