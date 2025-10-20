class BestChange::RowSerializer
  include FastJsonapi::ObjectSerializer
  set_type :bestchange_row

  attributes :exchanger_id, :exchanger_name, :status

  attribute :buy_price do |row|
    row.buy_price.to_f
  end

  attribute :sell_price do |row|
    row.sell_price.to_f
  end

  attribute :reserve do |row|
    row.reserve.to_f
  end

  attribute :rate do |row|
    row.rate.to_f
  end

  attribute :base_rate_percent do |row|
    row.base_rate_percent.to_f
  end

  attribute :position do |row|
    row.position.to_i
  end
end
