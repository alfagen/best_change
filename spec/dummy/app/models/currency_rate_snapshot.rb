class CurrencyRateSnapshot < ApplicationRecord
  has_many :currency_rates, dependent: :destroy

  validates :created_at, presence: true
end