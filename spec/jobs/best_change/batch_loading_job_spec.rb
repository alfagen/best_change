# frozen_string_literal: true

require 'spec_helper'

RSpec.describe BestChange::BatchLoadingJob, type: :job do
  it 'uses critical queue' do
    expect(described_class.queue_name).to eq('critical')
  end
end
