class CreateCurrencyRates < ActiveRecord::Migration[7.0]
  def change
    create_table :currency_rates do |t|
      t.references :currency_rate_snapshot, null: false, foreign_key: true
      t.integer :cur_from, null: false
      t.integer :cur_to, null: false
      t.decimal :rate_value, precision: 20, scale: 10, null: false
      t.string :mode

      t.timestamps
    end
  end
end