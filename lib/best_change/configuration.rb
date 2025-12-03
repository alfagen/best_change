module BestChange
  class Configuration
    attr_accessor :redis
    attr_accessor :exchanger_id
    attr_accessor :api_key
    attr_accessor :valuta_access_log
    attr_accessor :logger
    attr_accessor :rates_export_job_class
    attr_accessor :fetcher_path

    def rates_export_job
      return nil unless rates_export_job_class

      klass = rates_export_job_class.is_a?(String) ? rates_export_job_class.constantize : rates_export_job_class
      klass.new
    end
  end
end
