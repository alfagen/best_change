require 'oj'

# Класс нужно предварительно загрузить, чтобы Oj его использовал
require_relative 'row'

# Репозиторий bestchange-евских рейтингов
#
module BestChange
  class Repository < RedisRepository
    KEY_SEPARATOR = '-'.freeze
    CACHE_KEY = 'bestchange:preloaded_data:v1'.freeze

    class << self
      delegate :getRows, :setRows, :getRowByExchangerId, to: :instance

      # Идентификаторы платежных систем в bestchange
      def generate_key_from_bestchange_ids(id1, id2, source)
        [source, id1, id2].join KEY_SEPARATOR
      end
    end

    # key - ключ вида ID1_ID2
    # где ID1, ID2 - идентификаторы направления обмена из BestExchange (PaymentSystem#bestchange_id)
    # возвращает список BestChange::Row
    def getRows(key)
      preloaded_data[key]
    end

    def setRows(key, data)
      set key, Oj.dump(data)
    end

    # TODO rename to getRowByKey
    def getRowByExchangerId(key, exchanger_id = nil)
      exchanger_id ||= BestChange.configuration.exchanger_id

      getRows(key).find { |row| row.exchanger_id == exchanger_id}
    end

    def preloaded_data
      Rails.cache.fetch('bestchange_preloaded_data', expires_in: 30.seconds) do
        cached = Redis.current.get(CACHE_KEY)
        return Oj.load(cached) if cached.present?
      
        keys = Gera::ExchangeRate.all.map do |er|
          er.bestchange_key
        rescue Gera::CurrencyRatesRepository::UnknownPair, Gera::DirectionRate::UnknownExchangeRate, ActiveRecord::RecordInvalid
          nil
        end.compact

        store = Repository.send(:new).send(:store)
        raw_values = store.mget(*keys)
        rows_cache = {}

        keys.zip(raw_values).each do |key, json|
          next unless json.present?
          rows_cache[key] = Oj.load(json).each_with_index { |row, i| row.position = i }
        end

        Redis.current.set(CACHE_KEY, Oj.dump(rows_cache), ex: 30)

        rows_cache
      end
    end
  end
end
