module BestChange
  class Configuration
    attr_accessor :redis
    attr_accessor :exchanger_id
    attr_accessor :api_key
    attr_accessor :valuta_access_log
    attr_accessor :logger
    attr_accessor :rates_export_worker_class
    attr_accessor :fetcher_path

    def rates_export_worker
      return nil unless rates_export_worker_class

      klass = rates_export_worker_class.is_a?(String) ? rates_export_worker_class.constantize : rates_export_worker_class
      klass.new
    end
  end
end
