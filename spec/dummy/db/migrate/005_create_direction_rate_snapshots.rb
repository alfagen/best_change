class CreateDirectionRateSnapshots < ActiveRecord::Migration[7.0]
  def change
    create_table :direction_rate_snapshots do |t|
      t.timestamps
    end
  end
end