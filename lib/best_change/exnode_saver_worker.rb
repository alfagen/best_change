# frozen_string_literal: true

module BestChange
  class ExnodeSaverWorker
    include ::Sidekiq::Worker

    sidekiq_options queue: :default, retry: 2

    EXNODE_REDIS_KEY = 'exnode'
    BASE_URL = 'https://exnode.ru/-courses-/api/v2/rates'
    SPECIAL_CASES = {
      10 => 'USDTTRC',
      36 => 'USDTERC'
    }.freeze

    def perform(params, timestamp)
      params.split('#').each do |exchange_rate|
        from, to, id_from, id_to = exchange_rate.split('-')
        id_from = id_from.to_i
        id_to = id_to.to_i

        from = SPECIAL_CASES[id_from] || from
        to = SPECIAL_CASES[id_to] || to

        key = BestChange::Repository.generate_key_from_bestchange_ids id_from, id_to, EXNODE_REDIS_KEY
        url = "#{BASE_URL}?from=#{from}&to=#{to}"
        uri = URI(url)
        response = Net::HTTP.get(uri)
        data = JSON.parse(response)['items']

        next unless data.present?

        BestChange::Repository.setRows key, rates(data, timestamp).sort
      end
    end

    private

    def rates(data, timestamp)
      data.map do |rate|
        BestChange::Row.new(
          exchanger_id:   rate['exchanger_id'],
          exchanger_name: rate['exchanger']['name'],
          buy_price:      rate['in'],
          sell_price:     rate['out'],
          reserve:        rate['amount'],
          time:           timestamp
        )
      end
    end
  end
end
