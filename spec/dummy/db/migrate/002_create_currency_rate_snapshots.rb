class CreateCurrencyRateSnapshots < ActiveRecord::Migration[7.0]
  def change
    create_table :currency_rate_snapshots do |t|
      t.timestamps
    end
  end
end