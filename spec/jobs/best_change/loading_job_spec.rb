# frozen_string_literal: true

require 'spec_helper'

RSpec.describe BestChange::LoadingJob, type: :job do
  it 'uses critical queue' do
    expect(described_class.queue_name).to eq('critical')
  end

  it 'performs without error' do
    VCR.use_cassette :bestchange do
      expect { described_class.new.perform }.not_to raise_error
    end
  end
end
