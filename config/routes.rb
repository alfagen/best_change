BestChange::Engine.routes.draw do
  # API routes for exchange rates and status
  namespace :api do
    namespace :v1 do
      resources :exchange_rates, only: [:index] do
        collection do
          get :status
          get :competitive
        end
      end

      resources :currency_pairs, only: [:index] do
        collection do
          get :available
        end
      end
    end
  end

  # Health check endpoint
  get '/health', to: 'application#health'
end