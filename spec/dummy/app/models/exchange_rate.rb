class ExchangeRate < ApplicationRecord
  belongs_to :payment_system_from, class_name: 'PaymentSystem'
  belongs_to :payment_system_to, class_name: 'PaymentSystem'
  has_many :direction_rates, dependent: :destroy

  validates :comission, presence: true, numericality: true

  def validate_rate_bestchange_comission
    true # Заглушка для валидации
  end

  def currency_pair
    OpenStruct.new(
      first: payment_system_from&.currency,
      second: payment_system_to&.currency
    )
  end
end