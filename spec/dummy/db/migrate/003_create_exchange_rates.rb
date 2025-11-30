class CreateExchangeRates < ActiveRecord::Migration[7.0]
  def change
    create_table :exchange_rates do |t|
      t.references :payment_system_from, null: false, foreign_key: { to_table: :payment_systems }
      t.references :payment_system_to, null: false, foreign_key: { to_table: :payment_systems }
      t.decimal :comission, precision: 8, scale: 2, null: false

      t.timestamps
    end
  end
end