class CreatePaymentSystems < ActiveRecord::Migration[7.0]
  def change
    create_table :payment_systems do |t|
      t.string :name, null: false
      t.integer :type_cy, null: false
      t.boolean :income_enabled, default: true
      t.boolean :outcome_enabled, default: true
      t.integer :priority, default: 1

      t.timestamps
    end
  end
end