require 'spec_helper'

RSpec.describe BestChange::LoadingWorker, type: :job do
  it 'performs without error' do
    VCR.use_cassette :bestchange do
      expect { BestChange::LoadingWorker.new.perform }.not_to raise_error
    end
  end
end
