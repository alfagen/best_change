require 'sidekiq'

module BestChange
  class LoadingWorker
    MAX=500

    include ::Sidekiq::Worker
    include ::AutoLogger

    sidekiq_options queue: :critical, retry: false, lock: :until_executed

    def perform
      rates = load_bestchange_rates
      save_bestchange_rates rates
      update_direction_rates

      # TODO callback
      # BestChangeRatesExportWorker.new.perform
    end

    private

    def add_row(directions, row)
      ps1, ps2, exchanger_id, buy_price, sell_price, reserve = row
      exchanger_id = exchanger_id.to_i
      buy_price    = buy_price.to_f
      sell_price   = sell_price.to_f
      key          = BestChange::Repository.generate_key_from_bestchange_ids ps1, ps2, 'bestchange'

      d = directions[key] ||= []
      d << BestChange::Row.new(
        exchanger_id:   exchanger_id,
        #exchanger_name: exchanger_names[exchanger_id] || "Exchanger #{exchanger_id}",
        buy_price:      buy_price,
        sell_price:     sell_price,
        reserve:        reserve,
        time:           time
      )

      BestChange::Repository.setRows key, list.sort
    end

    def save_bestchange_rates(rates)
      directions = {}
      rates.each do |key, row|
        #[
          #{"changer" => 544, "rate" => "0.01334034146", "rankrate" => "0.01334034146", "reserve" => "12628541", "inmin" => "36.82", "inmax" => "5000", "marks" => ["manual"], "extra" => []},
          #{"changer" => 607, "rate" => "0.01271824509", "rankrate" => "0.01271824509", "reserve" => "4690233", "inmin" => "100", "inmax" => "5000", "marks" => ["manual"], "extra" => []},
          #{"changer" => 866, "rate" => "0.01366685282", "rankrate" => "0.01366685282", "reserve" => "8300000.12", "inmin" => "100", "inmax" => "113434.88", "marks" => ["manual", "otherout"], "extra" => []},
          #{"changer" => 948, "rate" => "0.01333415116", "rankrate" => "0.01333415116", "reserve" => "30748294", "inmin" => "61.4", "inmax" => "1000000000000", "marks" => ["manual"], "extra" => []}
        #]

        next if row.empty?
        debugger
        add_row directions, row
        #reserve = entry['reserve'].to_f
        #price = entry['rate'].to_f

        #buy_price, sell_price = if price < 1
                                  #[1.0, 1.0 / price]
                                #else
                                  #[price, 1.0]
                                #end

        #exchanger_name = @changers_map[entry['changer']] || 'unknown'

        #row = BestChangeRow.new
        #row.exchanger_id = entry['changer']
        #row.exchanger_name = exchanger_name
        #row.buy_price = buy_price
        #row.sell_price = sell_price
        #row.reserve = reserve
        #row.time = time_arg

        #rows << row
        Bestchange.repository.set '1','2'
      end

      directions.each do |key, list|
        BestChange::Repository.setRows key, list.sort
      end
    end

    def update_direction_rates
      exchange_rate_ids1 = Gera::TargetAutorateSetting.where('updated_at >= ?', 30.seconds.ago).pluck(:exchange_rate_id)
      exchange_rate_ids2 = Gera::ExchangeRate.where('updated_at >= ?', 30.seconds.ago).pluck(:id)
      exchange_rate_ids = (exchange_rate_ids1 + exchange_rate_ids2)
      Gera::DirectionRateSnapshot.last.direction_rates.where(exchange_rate_id: exchange_rate_ids).each do |dr|
        dr.calculate_rate
        dr.save!
      end
    end

    def load_bestchange_rates
      # https://operator.kassa.cc/bestchange/v2/%s/rates/%s
      api_url = URI("https://www.bestchange.app/v2/#{BestChange.configuration.api_key}/rates/")

      bestchange_keys = Gera::PaymentSystem.pluck(:id, :bestchange_id).to_h
      keys = Gera::ExchangeRate.
        available_for_parser.
        pluck(:income_payment_system_id, :outcome_payment_system_id).
        map { |i,o| [bestchange_keys[i], bestchange_keys[o]].join('-') }

      data = {}
      keys.each_slice MAX do |part|
        url =api_url+part.join('+')
        body = url.read
        rates = JSON.parse body
        data.merge! rates.fetch('rates')
      end
      debugger

      data
    end
  end
end
