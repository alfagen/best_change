FactoryBot.define do
  factory :gera_payment_system, class: 'Gera::PaymentSystem' do
    sequence(:name) { |n| "Payment System #{n}" }
    sequence(:bestchange_id) { |n| n + 100 }
    currency { OpenStruct.new(iso_code: 'RUB') }
    income_enabled { true }
    outcome_enabled { true }
  end

  factory :gera_exchange_rate, class: 'Gera::ExchangeRate', aliases: [:exchange_rate] do
    sequence(:id) { |n| n }
    payment_system_from { build(:gera_payment_system, currency: OpenStruct.new(iso_code: 'RUB')) }
    payment_system_to { build(:gera_payment_system, currency: OpenStruct.new(iso_code: 'BTC')) }
    value { 1.0 }
    is_enabled { true }
    comission { 5.0 }

    after(:build) do |er|
      er.currency_pair = OpenStruct.new(
        first: er.payment_system_from.currency,
        second: er.payment_system_to.currency
      )
    end
  end

  factory :currency_rate, class: 'Gera::CurrencyRate' do
    cur_from { 'RUB' }
    cur_to { 'BTC' }
    rate_value { 1.7179255491226387e-06 }
    mode { nil }
    snapshot { nil }

    after(:build) do |cr|
      cr.currency_pair = OpenStruct.new(first: cr.cur_from, second: cr.cur_to)
    end
  end

  factory :direction_rate_snapshot, class: 'Gera::DirectionRateSnapshot' do
    direction_rates { [] }
  end
end
