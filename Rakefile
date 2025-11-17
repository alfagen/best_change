require "bundler/gem_tasks"
require "rspec/core/rake_task"
require "yard"

RSpec::Core::RakeTask.new(:spec)

# Load Rails engine tasks from dummy app
load File.expand_path('spec/dummy/Rakefile') if File.exist?('spec/dummy/Rakefile')

# Documentation tasks
YARD::Rake::YardocTask.new do |t|
  t.files = ['lib/**/*.rb', '-', 'README.md', 'CHANGELOG.md']
  t.stats_options = ['--list-undoc']
end

# Security task
desc "Run security audit"
task :security do
  require 'brakeman'
  Brakeman.run quiet: true, exit_on_warn: true
end

# Engine integration test
desc "Test Rails Engine integration"
task :test_engine do
  Dir.chdir('spec/dummy') do
    system('bundle exec rails test') || exit(1)
    system('bundle exec rails best_change:check_config') || exit(1)
  end
end

# Coverage task
desc "Run tests with coverage"
task :coverage do
  ENV['COVERAGE'] = 'true'
  Rake::Task[:spec].invoke
end

# All checks task for CI
desc "Run all checks (tests, security, documentation)"
task :ci => [:spec, :security, :test_engine]

task :default => :spec
