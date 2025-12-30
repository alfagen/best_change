# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Full Rails Engine integration with isolate_namespace
- RESTful API endpoints for exchange rate data
- Background job processing with Sidekiq
- Rails-compatible configuration with initializers
- Middleware for configuration validation
- Rake tasks for managing background jobs
- Generators for installation and custom workers
- Comprehensive documentation and usage examples
- Support for multiple Rails versions (6.0+)
- Support for multiple Ruby versions (2.7+)

### Changed
- Enhanced configuration class with validation
- Improved error handling in background workers
- Better logging integration with Rails logger
- Optimized Redis connection handling

### Fixed
- Backward compatibility issues with existing gem usage
- Redis namespace conflicts in test environments

## [Previous versions]

### Features
- Exchange rate loading from BestChange API
- Commission calculation relative to base rates
- Competitive status determination
- Redis-based data storage
- Background processing with Sidekiq
- Basic service layer implementation