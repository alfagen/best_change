# frozen_string_literal: true

require 'spec_helper'

RSpec.describe BestChange::LoadingJob, type: :job do
  it 'performs without error' do
    VCR.use_cassette :bestchange do
      expect { described_class.new.perform }.not_to raise_error
    end
  end
end
