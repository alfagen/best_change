module BestChange
  class Configuration
    attr_accessor :redis
    attr_accessor :exchanger_id
    attr_accessor :valuta_access_log
    attr_accessor :fetcher_path # ~/bestchange_fetcher/main

    def fetcher_full_path
      File.expand_path fetcher_path
    end
  end
end
