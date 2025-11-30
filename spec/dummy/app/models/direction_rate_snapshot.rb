class DirectionRateSnapshot < ApplicationRecord
  has_many :direction_rates, dependent: :destroy
end