class CreateDirectionRates < ActiveRecord::Migration[7.0]
  def change
    create_table :direction_rates do |t|
      t.references :direction_rate_snapshot, null: false, foreign_key: true
      t.references :direction, null: false, foreign_key: true
      t.references :exchange_rate, null: false, foreign_key: true
      t.references :currency_rate, null: false, foreign_key: true
      t.decimal :rate_value, precision: 20, scale: 10, null: false
      t.decimal :base_rate_value, precision: 20, scale: 10, null: false

      t.timestamps
    end
  end
end