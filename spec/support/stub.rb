# Минимальные заглушки для классов из гема gera, нужных для тестов
module Gera
  class CurrencyPair
    attr_accessor :first, :second

    def initialize(first, second)
      @first = first
      @second = second
    end
  end

  class CurrencyRate
    attr_accessor :cur_from, :cur_to, :rate_value, :currency_pair, :mode, :snapshot

    def self.create!(attributes = {})
      new(attributes)
    end

    def self.where(conditions)
      []
    end

    def initialize(attributes = {})
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }
      @currency_pair ||= OpenStruct.new
    end

    def currency_pair
      @currency_pair ||= OpenStruct.new(first: cur_from, second: cur_to)
    end

    def save!
      true
    end
  end

  class PaymentSystem
    attr_accessor :id, :currency, :name, :priority, :income_enabled, :outcome_enabled, :type_cy, :bestchange_id

    @@instances = []

    def self.create!(attributes = {})
      instance = new(attributes)
      @@instances << instance
      instance
    end

    def self.pluck(*columns)
      @@instances.map { |i| columns.map { |c| i.send(c) } }
    end

    def self.clear_instances!
      @@instances = []
    end

    def initialize(attributes = {})
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }
      @currency ||= OpenStruct.new
      @bestchange_id ||= 1
      @id ||= rand(1..10000)
    end

    def save!
      true
    end

    def blank?
      @bestchange_id.nil? || @bestchange_id == 0
    end
  end

  class Direction
    attr_accessor :payment_system_from, :payment_system_to

    def initialize(payment_system_from: nil, payment_system_to: nil)
      @payment_system_from = payment_system_from
      @payment_system_to = payment_system_to
    end
  end

  class ExchangeRate
    attr_accessor :id, :payment_system_from, :payment_system_to, :value, :is_enabled, :comission, :bestchange_key, :currency_pair,
                  :income_payment_system_id, :outcome_payment_system_id

    @@instances = []

    def self.create!(attributes = {})
      instance = new(attributes)
      @@instances << instance
      instance
    end

    def self.find(id)
      @@instances.find { |i| i.id == id }
    end

    def self.find_or_create_by(attributes)
      new(attributes)
    end

    def self.find_by(attributes)
      new(attributes)
    end

    def self.includes(*associations)
      self
    end

    def self.where(*args)
      MockRelation.new([new])
    end

    def self.enabled
      self
    end

    def self.available_for_parser
      self
    end

    def self.pluck(*columns)
      @@instances.map { |i| columns.map { |c| i.send(c) } }
    end

    def self.clear_instances!
      @@instances = []
    end

    def self.find_each(batch_size: 1000, &block)
      yield(new) if block_given?
    end

    def initialize(attributes = {})
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }
      @id ||= rand(1000..9999)  # Generate a random ID if not provided
      @payment_system_from ||= OpenStruct.new(currency: OpenStruct.new)
      @payment_system_to ||= OpenStruct.new(currency: OpenStruct.new)
      @@instances << self unless @@instances.include?(self)
    end

    def save!
      true
    end

    def currency_pair
      @currency_pair ||= OpenStruct.new
    end

    def update(attributes)
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }
    end
  end

  class DirectionRateSnapshot
    def self.create!(attributes = {})
      new(attributes)
    end

    def initialize(attributes = {})
      @attributes = attributes
    end

    def save!
      true
    end
  end

  class CurrencyRateHistoryInterval
    def self.create!(attributes = {})
      new(attributes)
    end

    def initialize(attributes = {})
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }
    end

    def save!
      true
    end
  end

  class CurrencyRateMode
    def self.create!(attributes = {})
      new(attributes)
    end

    def initialize(attributes = {})
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }
    end

    def save!
      true
    end
  end

  class CurrencyRateSnapshot
    attr_accessor :currency_rate_mode_snapshot

    def self.create!(attributes = {})
      new(attributes)
    end

    def self.last
      new
    end

    def initialize(attributes = {})
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }
    end

    def rates
      @rates ||= []
    end

    def save!
      true
    end
  end

  class DirectionRate
    attr_accessor :direction, :exchange_rate, :currency_rate, :base_rate_value, :rate_value, :comission

    def self.create!(attributes = {})
      new(attributes)
    end

    def initialize(attributes = {})
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }

      # If rate_value is not provided but base_rate_value is, derive it using Gera::Mathematic formula
      if !@rate_value && @base_rate_value
        @comission = 10.0 if @comission.nil?
        # Use the same formula as Gera::Mathematic.calculate_finite_rate
        if @base_rate_value <= 1
          @rate_value = @base_rate_value * (1.0 - @comission / 100.0)
        else
          @rate_value = @base_rate_value - (@comission * @base_rate_value / 100.0)
        end
      end

      # Calculate commission if we have rate_value and base_rate_value but no commission
      if @rate_value && @base_rate_value && @comission.nil?
        @comission = calculate_comission
      end
    end

    def save!
      true
    end

    private

    def calculate_comission
      # Simple commission calculation based on the difference between base rate and actual rate
      return 0 unless @base_rate_value && @rate_value && @base_rate_value > 0

      ((@rate_value / @base_rate_value) - 1) * 100
    end
  end

  class DirectionRatesProxy
    def initialize(parent)
      @parent = parent
      @direction_rates = []
    end

    def create(attributes)
      direction_rate = Gera::DirectionRate.new(attributes)
      @direction_rates << direction_rate
      direction_rate
    end

    def <<(item)
      @direction_rates << item
    end

    def each(&block)
      @direction_rates.each(&block)
    end

    def size
      @direction_rates.size
    end

    def empty?
      @direction_rates.empty?
    end

    def first
      @direction_rates.first
    end

    def last
      @direction_rates.last
    end

    def to_a
      @direction_rates
    end

    def to_ary
      @direction_rates
    end

    def where(conditions)
      MockRelation.new(@direction_rates)
    end
  end

  class DirectionRateSnapshot
    attr_accessor :direction_rates

    def self.create!(attributes = {})
      new(attributes)
    end

    def self.last
      new
    end

    def initialize(attributes = {})
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }
      @direction_rates ||= []
    end

    def direction_rates
      @direction_rates_proxy ||= DirectionRatesProxy.new(self)
    end

    def save!
      true
    end
  end

  class MockRelation
    def initialize(items)
      @items = items
    end

    def pluck(*columns)
      @items.map { |i| columns.map { |c| i.send(c) rescue nil } }
    end

    def where(conditions)
      MockRelation.new(@items)
    end

    def each(&block)
      @items.each(&block)
    end
  end

  class TargetAutorateSetting
    def self.where(*args)
      MockRelation.new([])
    end
  end

  class ExternalRate
    def self.create!(attributes = {})
      new(attributes)
    end

    def initialize(attributes = {})
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }
    end

    def save!
      true
    end
  end

  class ExternalRateSnapshot
    def self.create!(attributes = {})
      new(attributes)
    end

    def initialize(attributes = {})
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }
    end

    def save!
      true
    end
  end

  class CurrencyRateModeSnapshot
    attr_accessor :title

    def self.create!(attributes = {})
      new(attributes)
    end

    def initialize(attributes = {})
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }
    end

    def save!
      true
    end
  end

  class RateSource
    attr_accessor :key, :title

    def self.create!(attributes = {})
      new(attributes)
    end

    def initialize(attributes = {})
      attributes.each { |key, value| send("#{key}=", value) if respond_to?("#{key}=") }
    end

    def save!
      true
    end
  end

  class RateSourceManual < RateSource
  end

  class RateSourceCbr < RateSource
  end

  class RateSourceCbrAvg < RateSource
  end

  class RateSourceExmo < RateSource
  end

  class RateSourceBitfinex < RateSource
  end

  class RateSourceBinance < RateSource
  end

  class CurrencyRatesRepository
    class UnknownPair < StandardError
    end
  end

  class Universe
    def self.clear!
      # заглушка
    end

    def self.currency_rates_repository
      @currency_rates_repository ||= OpenStruct.new(
        find_currency_rate_by_pair: ->(pair) {
          # Always return a mock currency rate for testing
          # Handle both CurrencyPair objects and OpenStruct objects
          rate_value = case pair
                       when Gera::CurrencyPair
                         60.0  # Default rate for CurrencyPair objects
                       else
                         # For OpenStruct or other objects, extract rate based on currency names
                         if pair.respond_to?(:first) && pair.respond_to?(:second)
                           case [pair.first&.to_s, pair.second&.to_s]
                           when ['RUB', 'BTC'], ['rub', 'btc']
                             1.7179255491226387e-06  # Correct base_rate for RUB->BTC
                           else
                             60.0
                           end
                         else
                           60.0
                         end
                       end

          OpenStruct.new(rate_value: rate_value)
        }
      )
    end

    private

    def self.base_rate
      640307.14285714  # From the test: in_amount: 640307.14285714, out_amount: 1.0
    end
  end

  class MathematicModule
    def calculate_base_rate(rate, commission)
      # Simple calculation for testing
      rate * (1 + commission / 100.0)
    end
  end

  module Mathematic
    def calculate_comission(rate, base_rate_multiplicator)
      # Simple calculation for testing
      (rate / base_rate_multiplicator - 1) * 100
    end
  end

  class ExchangeRateUpdaterWorker
    def self.perform_in(delay, id, attributes)
      # Find the exchange rate and update its attributes
      exchange_rate = Gera::ExchangeRate.find(id)
      if exchange_rate && attributes
        exchange_rate.update(attributes)
      end
      true
    end
  end

# MoneySupport - это модуль в геме gera, не трогаем его
end

class ExchangeRate
end
