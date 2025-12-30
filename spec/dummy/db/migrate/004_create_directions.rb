class CreateDirections < ActiveRecord::Migration[7.0]
  def change
    create_table :directions do |t|
      t.references :payment_system_from, null: false, foreign_key: { to_table: :payment_systems }
      t.references :payment_system_to, null: false, foreign_key: { to_table: :payment_systems }

      t.timestamps
    end
  end
end