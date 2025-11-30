require 'rails_helper'

RSpec.describe BestChange, type: :model do
  it "has a version number" do
    expect(BestChange::VERSION).not_to be nil
  end

  it "can be configured" do
    expect { BestChange.configure { |config| config.exchanger_id = 123 } }.not_to raise_error
  end

  context 'configuration' do
    it 'stores configuration values' do
      BestChange.configure do |config|
        config.exchanger_id = 999
        config.redis = Redis.new(db: 2)
      end

      expect(BestChange.configuration.exchanger_id).to eq(999)
    end
  end
end
