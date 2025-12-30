# Rails specific dependencies that need Rails to be loaded first
require 'gera'
require 'gera/engine'
require 'gera/railtie'

# Initialize Gera support
Gera::MoneySupport.init