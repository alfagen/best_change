class PaymentSystem < ApplicationRecord
  validates :name, presence: true
  validates :type_cy, presence: true

  def currency
    OpenStruct.new(local_id: type_cy)
  end
end