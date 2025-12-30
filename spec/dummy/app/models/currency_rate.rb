class CurrencyRate < ApplicationRecord
  belongs_to :currency_rate_snapshot
  belongs_to :currency_pair

  validates :rate_value, presence: true, numericality: true

  def currency_pair
    @currency_pair ||= OpenStruct.new(
      first: OpenStruct.new(local_id: cur_from),
      second: OpenStruct.new(local_id: cur_to)
    )
  end
end