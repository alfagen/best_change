module BestChange
  class ApplicationController < ActionController::Base
    protect_from_forgery with: :exception

    # Basic health check
    def health
      render json: {
        status: 'ok',
        engine: 'BestChange',
        version: BestChange::VERSION,
        timestamp: Time.current.iso8601
      }
    end

    private

    def render_error(message, status = :unprocessable_entity)
      render json: { error: message }, status: status
    end

    def render_success(data = {})
      render json: { success: true }.merge(data)
    end
  end
end