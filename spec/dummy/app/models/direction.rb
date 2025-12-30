class Direction < ApplicationRecord
  belongs_to :payment_system_from, class_name: 'PaymentSystem'
  belongs_to :payment_system_to, class_name: 'PaymentSystem'
  has_many :direction_rates, dependent: :destroy

  validates :payment_system_from, presence: true
  validates :payment_system_to, presence: true
end