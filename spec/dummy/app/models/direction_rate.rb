class DirectionRate < ApplicationRecord
  belongs_to :direction_rate_snapshot
  belongs_to :direction
  belongs_to :exchange_rate
  belongs_to :currency_rate

  validates :rate_value, presence: true, numericality: true
  validates :base_rate_value, presence: true, numericality: true

  def comission
    exchange_rate&.comission || 0
  end
end