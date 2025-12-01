# frozen_string_literal: true

require 'spec_helper'

RSpec.describe BestChange::TrusteeLoadingJob, type: :job do
  it 'uses default queue' do
    expect(described_class.queue_name).to eq('default')
  end
end
