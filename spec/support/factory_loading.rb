# Load Rails-specific factories
Rails.application.eager_load! if defined?(Rails)

# Load all factories after Rails and Gera are loaded
FactoryBot.definition_file_paths = [
  File.join(__dir__, '..', 'factories')
]
FactoryBot.find_definitions