# Load Rails-specific factories
Rails.application.eager_load! if defined?(Rails)

# Factories are loaded in spec_helper.rb, don't reload here
# to avoid FactoryBot::DuplicateDefinitionError