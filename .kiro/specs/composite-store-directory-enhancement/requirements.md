# Requirements Document

## Introduction

This feature enhances OpenSearch's storage architecture to support multiple data formats (Lucene, Parquet, and future formats) through composite directories. The enhancement involves migrating to a unified format-aware system that uses FileMetadata for format determination and routing files to appropriate storage containers.

The key architectural change is moving from file name-based format detection to FileMetadata-based format routing, ensuring that FormatStoreDirectory implementations remain agnostic to file extensions while the composite directories handle format routing based on metadata. All formats, including Lucene, will be treated equally through the generic composite flow without special handling.

## Requirements

### Requirement 1: FileMetadata-Based Format Routing

**User Story:** As a storage system, I want to route files to appropriate format directories based on FileMetadata instead of file names, so that format determination is centralized and consistent.

#### Acceptance Criteria

1. WHEN a file upload is initiated THEN the system SHALL use FileMetadata.df (DataFormat) to determine the target format directory
2. WHEN FileMetadata is available THEN the system SHALL NOT rely on file extension-based format detection
3. WHEN routing files THEN CompositeStoreDirectory SHALL extract format information from FileMetadata.df field
4. WHEN FileMetadata contains format information THEN FormatStoreDirectory implementations SHALL NOT need knowledge of supported file extensions

### Requirement 2: Enhanced copyFrom API

**User Story:** As a remote storage component, I want the copyFrom API to accept FileMetadata instead of just file names, so that I can determine the correct format directory and blob container for each file.

#### Acceptance Criteria

1. WHEN copyFrom is called THEN it SHALL accept FileMetadata parameter containing both fileName and DataFormat
2. WHEN FileMetadata is provided THEN the system SHALL extract format type from FileMetadata.df field
3. WHEN format is determined THEN the system SHALL route to the correct blob container based on format
4. WHEN uploading files THEN Lucene format SHALL be treated as any other format without special handling

### Requirement 3: RemoteSegmentStoreDirectory Integration

**User Story:** As a remote segment store, I want to use CompositeRemoteDirectory instead of dataDirectory, so that I can support multiple storage formats with format-specific blob containers.

#### Acceptance Criteria

1. WHEN RemoteSegmentStoreDirectory is initialized THEN it SHALL use CompositeRemoteDirectory for multi-format support
2. WHEN uploading segments THEN the system SHALL route files to format-specific blob containers
3. WHEN accessing remote files THEN the system SHALL maintain format-aware path structures
4. WHEN migrating from dataDirectory THEN Lucene format SHALL use the same generic flow as other formats

### Requirement 4: IndexShard and Store Integration

**User Story:** As an index shard, I want to use CompositeStoreDirectory for local storage operations, so that I can handle multiple data formats consistently.

#### Acceptance Criteria

1. WHEN IndexShard initializes storage THEN it SHALL use CompositeStoreDirectory for local file operations
2. WHEN Store is created THEN it SHALL provide access to CompositeStoreDirectory alongside existing directory
3. WHEN performing file operations THEN the system SHALL route through CompositeStoreDirectory based on format metadata
4. WHEN accessing files THEN Lucene format SHALL be handled through the same generic composite flow as other formats

### Requirement 5: Segment Upload Flow Enhancement

**User Story:** As a segment upload process, I want to use FileMetadata from catalog snapshots for format-aware uploads, so that each file is uploaded to the correct format-specific location.

#### Acceptance Criteria

1. WHEN AfterRefresh triggers upload THEN the system SHALL obtain FileMetadata from catalog snapshots
2. WHEN syncSegments processes files THEN it SHALL pass FileMetadata to upload methods
3. WHEN uploadNewSegments is called THEN it SHALL use FileMetadata for format determination
4. WHEN copyFrom API is invoked THEN it SHALL receive FileMetadata instead of just file names
5. WHEN files are uploaded THEN each format SHALL be routed to its specific blob container path

### Requirement 6: Format-Aware Directory Implementation

**User Story:** As a FormatStoreDirectory implementation, I want to declare what DataFormat I can handle, so that the composite directory can route files to the appropriate format directory.

#### Acceptance Criteria

1. WHEN FormatStoreDirectory is implemented THEN it SHALL declare the specific DataFormat it handles
2. WHEN CompositeStoreDirectory routes files THEN it SHALL match FileMetadata.df with FormatStoreDirectory's supported DataFormat
3. WHEN FileMetadata is available THEN CompositeStoreDirectory SHALL use DataFormat matching for routing instead of file extension logic
4. WHEN FormatStoreDirectory is queried THEN it SHALL provide its supported DataFormat information
5. WHEN files are processed THEN format-specific logic SHALL be contained within the appropriate FormatStoreDirectory implementation

### Requirement 7: Extensible Format Support

**User Story:** As a system architect, I want the design to be extensible for future data formats, so that new formats can be added without modifying existing code.

#### Acceptance Criteria

1. WHEN new data formats are introduced THEN the system SHALL support them without modifying existing format directories
2. WHEN format plugins are added THEN CompositeStoreDirectory SHALL automatically discover and integrate them
3. WHEN routing files THEN the system SHALL use a plugin-based approach for format-specific handling
4. WHEN blob containers are created THEN each format SHALL have its own isolated storage path

### Requirement 8: Backward Compatibility

**User Story:** As an existing OpenSearch deployment, I want Lucene-only installations to continue working without changes, so that upgrades are seamless.

#### Acceptance Criteria

1. WHEN upgrading from Lucene-only systems THEN Lucene format SHALL be treated as any other format in the composite flow
2. WHEN FileMetadata is not available THEN the system SHALL fall back to existing file name-based detection
3. WHEN processing any format files THEN ALL formats including Lucene SHALL use the same generic composite flow
4. WHEN migrating storage THEN existing blob paths and structures SHALL remain accessible

### Requirement 9: Catalog Snapshot Integration

**User Story:** As a catalog snapshot system, I want to provide FileMetadata with format information, so that the upload flow can make format-aware routing decisions.

#### Acceptance Criteria

1. WHEN catalog snapshots are created THEN they SHALL include FileMetadata with DataFormat information
2. WHEN getAllSearchableFiles is called THEN it SHALL return Collection<FileMetadata> with format details
3. WHEN files are processed THEN the system SHALL extract both fileName and DataFormat from FileMetadata
4. WHEN format information is missing THEN the system SHALL handle graceful fallback to extension-based detection

### Requirement 10: Parquet Format Support

**User Story:** As a Parquet data format, I want to work seamlessly with the current composite directory implementation, so that Parquet files are uploaded and managed alongside other formats.

#### Acceptance Criteria

1. WHEN Parquet files are generated THEN they SHALL be included in FileMetadata with DataFormat.PARQUET
2. WHEN Parquet files are uploaded THEN they SHALL use the same generic composite flow as other formats
3. WHEN ParquetFormatDirectory is implemented THEN it SHALL declare DataFormat.PARQUET as its supported format
4. WHEN Parquet files are routed THEN CompositeStoreDirectory SHALL match FileMetadata.df with ParquetFormatDirectory
5. WHEN Parquet files are stored remotely THEN they SHALL be placed in format-specific blob containers (e.g., basePath/parquet/)