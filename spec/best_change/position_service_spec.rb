require 'spec_helper'
require 'best_change/row'

RSpec.describe BestChange::PositionService, type: :service do
  include Gera::Mathematic

  let(:br_data) { Oj.load(File.read 'spec/fixtures/bestchange.json').each_with_index { |row, index| row.position = index }  }
  let(:base_rate_multiplicator) { 1.7179255491226387e-06 }

  # Обновляет buy_price для "my" row, симулируя что BestChange отразил наш новый курс
  # Используем формулу из реального Gera::Mathematic#calculate_finite_rate:
  # Для base_rate <= 1: finite_rate = base_rate * (1.0 - comission/100)
  def update_my_row_for_commission!(data, new_commission, base_rate_mult)
    my_row = data.find { |d| d.is_my? }
    # Используем calculate_finite_rate для base_rate <= 1
    target_rate = base_rate_mult * (1.0 - new_commission / 100.0)
    my_row.buy_price = 1.0 / target_rate
  end

  let(:br_rate) { br_data.find { |d| d.is_my? }.rate }

  # Рейт из bestchange data
  let!(:current_rate)       { Gera::Rate.new in_amount: 640307.14285714, out_amount: 1.0 }
  let!(:currency_pair)      { Gera::CurrencyPair.new RUB, BTC }
  let(:payment_system_from) { create :gera_payment_system, currency: currency_pair.first } # RUB
  let(:payment_system_to)   { create :gera_payment_system, currency: currency_pair.second } # BTC
  let(:direction)           { Gera::Direction.new payment_system_from: payment_system_from, payment_system_to: payment_system_to }

  # (СберОнлайн->Bitcoin)
  let!(:bestchange_key) { '42-93' }
  let!(:base_rate) { calculate_base_rate br_rate, comission }
  # let!(:base_rate)      { 1.7370546342914202e-06 }
  let(:comission)       { 10 }
  let!(:currency_rate)  { create :currency_rate, rate_value: base_rate, currency_pair: currency_pair }

  let!(:direction_rate_snapshot) { create :direction_rate_snapshot }

  let!(:exchange_rate) {
    # TODO пернести в factory
    Gera::ExchangeRate.find_by(payment_system_from: direction.payment_system_from, payment_system_to: direction.payment_system_to) ||
    create(:exchange_rate, payment_system_to: direction.payment_system_to, payment_system_from: direction.payment_system_from)
  }

  let!(:direction_rate) {
    direction_rate_snapshot.direction_rates.create(
      direction: direction,
      exchange_rate: exchange_rate,
      currency_rate: currency_rate,
      base_rate_value: base_rate
    )
  }

  let(:status) {
    BestChange::Service.
      new(exchange_rate: exchange_rate).
      status
  }

  let(:current_position) { 40 }

  before do
    exchange_rate.update comission: comission
    allow_any_instance_of(ExchangeRate).to receive(:validate_rate_bestchange_comission).and_return true

    # Mock the Universe.currency_rates_repository to avoid UnknownPair errors
    allow(Gera::Universe).to receive(:currency_rates_repository).and_return(
      double('currency_rates_repository', find_currency_rate_by_pair: OpenStruct.new(rate_value: 1.7179255491226387e-06))
    )
  end

  subject do
    BestChange::PositionService.
      new(exchange_rate: exchange_rate)
  end


  context 'проверочка' do
    it { expect(direction_rate.rate_value).to eq br_rate }
    it { expect(direction_rate.base_rate_value).to eq base_rate }
    it { expect(direction_rate.comission).to eq comission }
    it { expect(status.position).to eq current_position }
    it { expect(status.target_position).to eq current_position }
  end

  before do
    allow_any_instance_of(BestChange::Repository).to receive(:getRows).and_return br_data

    # Mock ExchangeRateUpdaterWorker to update both exchange_rate AND br_data
    # Симулирует что после изменения комиссии, BestChange отражает наш новый курс
    allow(Gera::ExchangeRateUpdaterWorker).to receive(:perform_in) do |_delay, id, attributes|
      er = Gera::ExchangeRate.find(id)
      if er && attributes
        er.update(attributes)
        # Обновляем br_data чтобы отразить новую комиссию
        if attributes[:comission]
          update_my_row_for_commission!(br_data, attributes[:comission], base_rate_multiplicator)
        end
      end
      true
    end
  end

  describe 'курс выше 1 (RUB -> BTC)' do
    context 'Поднимаем выше' do
      let(:new_position) { 35 }

      specify do
        subject.change_position! new_position
        expect(status.target_position).to eq new_position
      end
    end

    context 'Поднимаем выше, где между строчками растояние мешьне чем наш шаг' do
      let(:new_position) { 3 }

      specify do
        subject.change_position! new_position
        expect(status.target_position).to eq new_position
      end
    end

    context 'Ставим как было' do
      let(:new_position) { current_position }

      specify do
        subject.change_position! new_position
        expect(status.target_position).to eq new_position
      end
    end

    context 'Опускаем ниже' do
      let(:new_position) { 41 }

      specify do
        subject.change_position! new_position
        expect(status.target_position).to eq new_position
      end
    end

    context 'Ставим первой' do
      let(:new_position) { 0 }

      specify do
        subject.change_position! new_position
        expect(status.target_position).to eq new_position
      end
    end

    context 'Ставим последней' do
      let(:new_position) { br_data.count - 1 }

      specify do
        subject.change_position! new_position
        expect(status.target_position).to eq new_position
      end
    end

    context 'Ставим ниже количества' do
      specify do
        subject.change_position! br_data.count + 5
        expect(status.target_position).to eq br_data.count - 1
      end
    end
  end
end
