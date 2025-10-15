# Requirements Document

## Introduction

This feature introduces a new factory class for creating CompositeStoreDirectory and RemoteSegmentStoreDirectory instances that use CompositeRemoteDirectory. The factory will follow the existing IndexStorePlugin pattern and integrate with IndexService to centralize directory creation logic while maintaining backward compatibility.

## Requirements

### Requirement 1

**User Story:** As a developer, I want a centralized factory for creating composite directories, so that directory creation logic is consistent and maintainable across the codebase.

#### Acceptance Criteria

1. WHEN IndexService needs to create a CompositeStoreDirectory THEN the factory SHALL create an instance with proper plugin-based format discovery
2. WHEN the factory creates a CompositeStoreDirectory THEN it SHALL use PluginsService to discover available DataFormat plugins
3. WHEN multiple DataFormat plugins are available THEN the factory SHALL create a CompositeStoreDirectory that supports all discovered formats
4. IF no DataFormat plugins are found THEN the factory SHALL create a fallback CompositeStoreDirectory with default formats (Lucene, Text)

### Requirement 2

**User Story:** As a developer, I want the factory to create RemoteSegmentStoreDirectory instances with CompositeRemoteDirectory, so that remote storage supports format-aware operations.

#### Acceptance Criteria

1. WHEN IndexService needs a remote directory THEN the factory SHALL create RemoteSegmentStoreDirectory with CompositeRemoteDirectory
2. WHEN creating CompositeRemoteDirectory THEN the factory SHALL use the same format discovery as local directories
3. WHEN RemoteSegmentStoreDirectory is created THEN it SHALL support all format-aware operations through CompositeRemoteDirectory
4. IF remote storage is not configured THEN the factory SHALL return null or throw appropriate exception

### Requirement 3

**User Story:** As a developer, I want the factory to follow IndexStorePlugin patterns, so that it integrates seamlessly with existing plugin architecture.

#### Acceptance Criteria

1. WHEN implementing the factory THEN it SHALL follow the same interface pattern as DirectoryFactory and CompositeDirectoryFactory
2. WHEN the factory is used THEN it SHALL accept IndexSettings and ShardPath parameters like existing factories
3. WHEN plugins register directory factories THEN the new factory SHALL be discoverable through IndexStorePlugin interface
4. WHEN factory methods are called THEN they SHALL throw IOException on failure like existing factories

### Requirement 4

**User Story:** As a developer, I want backward compatibility with existing directory creation, so that existing code continues to work without modification.

#### Acceptance Criteria

1. WHEN existing Store constructors are used THEN they SHALL continue to work without the factory
2. WHEN IndexService uses existing directoryFactory THEN it SHALL work alongside the new composite factory
3. WHEN legacy directory creation is used THEN it SHALL not interfere with composite directory creation
4. IF both old and new factories are available THEN IndexService SHALL prefer the new composite factory when appropriate

### Requirement 5

**User Story:** As a developer, I want proper error handling and logging, so that directory creation failures are easy to diagnose and debug.

#### Acceptance Criteria

1. WHEN factory creation fails THEN it SHALL log detailed error messages with context
2. WHEN plugin discovery fails THEN the factory SHALL log available plugins and continue with fallback
3. WHEN format registration fails THEN the factory SHALL log the specific format and error details
4. IF directory initialization fails THEN the factory SHALL provide clear error messages indicating the cause

### Requirement 6

**User Story:** As a developer, I want the factory to support future directory types, so that new composite directory implementations can be easily added.

#### Acceptance Criteria

1. WHEN new composite directory types are needed THEN the factory interface SHALL be extensible
2. WHEN UltraWarm composite directories are implemented THEN they SHALL use the same factory pattern
3. WHEN new storage backends are added THEN the factory SHALL support creating appropriate composite directories
4. IF factory extension is needed THEN it SHALL not break existing implementations