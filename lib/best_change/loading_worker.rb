# frozen_string_literal: true

class DumpValutaServiceV2
  MONEY_FORMAT = { with_currency: false, symbol: false, delimiter: nil, separator: '.' }.freeze

  def perform
    data = builder.to_xml
    File.atomic_write(valuta_file_path) { |f| f.write(data) }
  end

  private

  def builder
    exchange_rates = Gera::ExchangeRate
      .available_for_parser
      .preload(
        :exchange_rate_limit,
        payment_system_from: :income_bestchange_cities,
        payment_system_to:   :outcome_bestchange_cities
      )
      .order(:id)
      .to_a

    ids = exchange_rates.map(&:id)

    direction_rates = Gera::DirectionRateSnapshot.last
      .direction_rates
      .where(exchange_rate_id: ids)
      .pluck(:exchange_rate_id, :rate_value)
      .to_h

    index_by_pair = exchange_rates.index_by { |er| [er.income_payment_system_id, er.outcome_payment_system_id] }

    amounts = PayoutSlot.available_rub.pluck(:amount).map(&:to_i).uniq

    global_min_slot = amounts.min
    global_max_slot = amounts.max

    pairs_needed = exchange_rates.flat_map { |er|
      a = er.income_payment_system_id
      b = er.outcome_payment_system_id
      [[a, b], [b, a]]
    }.uniq

    to_ids_needed = exchange_rates.map(&:outcome_payment_system_id).uniq

    @ddr_by_pair = prefetch_ddr_for_pairs(pairs_needed)

    @to_reserve = prefetch_to_reserves(to_ids_needed)

    Nokogiri::XML::Builder.new do |xml|
      xml.rates do
        exchange_rates.each do |er|
          base_rate = direction_rates[er.id]
          next unless base_rate

          mirror_income_id = er.income_payment_system&.mirror_payment_system_id
          mirror_er  = index_by_pair[[mirror_income_id, er.outcome_payment_system_id]]
          mirror_rate = mirror_er ? direction_rates[mirror_er.id] : -1

          min_slot, max_slot = er.income_payment_system&.split_outcome_amount? ? [global_min_slot, global_max_slot] : [nil, nil]

          final_rate = min_slot.nil? ? base_rate : mirror_rate

          min_money, max_money = effective_limits(er)

          codes = {}
          er.payment_system_from.income_bestchange_cities.each { |c| codes[c.code] = true }
          er.payment_system_to.outcome_bestchange_cities.each   { |c| codes[c.code] = true }
          cities_str = codes.empty? ? '' : codes.keys.join(', ')

          xml.item do
            xml.from er.payment_system_from.bestchange_letter_cod
            xml.to   er.payment_system_to.bestchange_letter_cod

            if final_rate && final_rate.positive? && final_rate < 1
              xml.in  (1.0 / final_rate)
              xml.out 1
            else
              xml.in  1
              xml.out (final_rate || 1)
            end

            xml.minamount money_format((min_slot || min_money.to_f), min_money.currency)
            xml.maxamount money_format((max_slot || max_money.to_f), max_money.currency)

            xml.amount total_reserve_for(er.income_payment_system_id, er.outcome_payment_system_id)

            xml.city cities_str unless cities_str.empty?

            params = [:manual]
            params << :verifying if er.payment_system_to.require_verify? || er.payment_system_from.require_verify?
            xml.param params.join(', ')
          end
        end
      end
    end
  end

  def total_reserve_for(from_id, to_id)
    (@to_reserve[to_id.to_i] || 0) + (@ddr_by_pair[[from_id.to_i, to_id.to_i]] || 0)
  end

  def prefetch_ddr_for_pairs(pairs)
    out = {}
    return out if pairs.empty?

    pairs.each_slice(500) do |slice|
      cond = slice.map { |a, b|
        "(income_payment_system_id = #{a.to_i} AND outcome_payment_system_id = #{b.to_i})"
      }.join(' OR ')

      DirectionDeltaReserve.unscoped
        .where(Arel.sql(cond))
        .pluck(:income_payment_system_id, :outcome_payment_system_id, :amount)
        .each do |a, b, amt|
          a = a.to_i; b = b.to_i
          out[[a, b]] = amt
          out[[b, a]] = amt
        end
    end

    out
  end

  def prefetch_to_reserves(to_ids)
    out = {}
    return out if to_ids.empty?

    to_ids.each do |to_id|
      out[to_id.to_i] = reserves_service.get_reserve_by_payment_system_id(to_id)
    end

    out
  end

  def effective_limits(er)
    if (lim = er.exchange_rate_limit)
      [lim.min_amount, lim.max_amount]
    else
      [Money.from_amount(0, Money.default_currency), Money.from_amount(Float::INFINITY, Money.default_currency)]
    end
  end

  def money_format(amount, currency)
    Money.from_amount(amount, currency).format(MONEY_FORMAT)
  end

  def reserves_service
    @reserves_service ||= ReservesByPaymentSystems.new
  end

  def valuta_file_path
    Rails.root.join(Settings.valuta_xml_file_path)
  end
end
