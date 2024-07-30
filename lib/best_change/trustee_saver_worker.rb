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
      ps1 = exchange_rate.income_payment_system
      ps2 = exchange_rate.outcome_payment_system
      key = BestChange::Repository.generate_key_from_bestchange_ids ps1.bestchange_id, ps2.bestchange_id, 'trustee'
      in_currency = format_currency(ps1)
      out_currency = format_currency(ps2)

      url = "#{BASE_URL}?inCurrencyCode=#{in_currency}&outCurrencyCode=#{out_currency}"
      uri = URI(url)
      response = Net::HTTP.get(uri)
      data = JSON.parse(response)
      rates = collect(ps1, ps2, timestamp, data['sell'])
      rates = collect(ps1, ps2, timestamp, data['exchange']) if rates.empty?

      BestChange::Repository.setRows key, rates.sort
    end

    private

    def format_currency(payment_system)
      currency = payment_system.currency.to_s.downcase
      return currency.upcase unless currency.inquiry.usdt?

      TOKEN_NETWORK_TO_CURRENCY[payment_system.token_network]
    end

    def collect(ps1, ps2, timestamp, data)
      data.map do |rate|
        next if ps1.trustee_payway_code.present? && ps1.trustee_payway_code != rate['inPaywayCode']
        next if ps2.trustee_payway_code.present? && ps2.trustee_payway_code != rate['outPaywayCode']

        BestChange::Row.new(
          exchanger_id:   rate['exchangeWayId'],
          exchanger_name: rate['provider'],
          buy_price:      1,
          sell_price:     rate.dig('exchangeRate', 'exchangeRate') || 0,
          reserve:        rate['limits']['max'],
          time:           timestamp
        )
      end.compact
    end
  end
end
