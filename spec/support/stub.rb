# Минимальные заглушки для классов из гема gera, нужных для тестов
module Gera
  class CurrencyRate
    def self.create!(attributes = {})
      new(attributes)
    end

    def self.where(conditions)
      []
    end

    def initialize(attributes = {})
      @attributes = attributes
    end

    def currency_pair
      @currency_pair ||= OpenStruct.new
    end

    def save!
      true
    end
  end

  class PaymentSystem
    def self.create!(attributes = {})
      new(attributes)
    end

    def initialize(attributes = {})
      @attributes = attributes
    end

    def currency
      @currency ||= OpenStruct.new
    end

    def save!
      true
    end
  end

  class ExchangeRate
    def self.create!(attributes = {})
      new(attributes)
    end

    def self.find_or_create_by(attributes)
      new(attributes)
    end

    def self.includes(associations)
      self
    end

    def self.where(conditions)
      [new]
    end

    def self.enabled
      self
    end

    def self.available_for_parser
      [new]
    end

    def self.find_each(&block)
      yield(new) if block_given?
    end

    def initialize(attributes = {})
      @attributes = attributes
    end

    def payment_system_from
      @payment_system_from ||= OpenStruct.new(currency: OpenStruct.new)
    end

    def payment_system_to
      @payment_system_to ||= OpenStruct.new(currency: OpenStruct.new)
    end

    def save!
      true
    end

    def currency_pair
      @currency_pair ||= OpenStruct.new
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

  class Universe
    def self.clear!
      # заглушка
    end
  end

  # MoneySupport - это модуль в геме gera, не трогаем его
end

class ExchangeRate
end
