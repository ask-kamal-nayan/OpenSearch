/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.DataFormat;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/**
 * Interface for format-specific remote directories that handle remote storage operations
 * for different data formats (Lucene, Parquet, etc.)
 * This interface mirrors the RemoteDirectory API patterns but adds format-specific capabilities.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public interface FormatRemoteDirectory extends Closeable {
    
    /**
     * Returns the data format this remote directory handles
     */
    DataFormat getDataFormat();
    
    /**
     * Returns the remote base path for this format
     */
    String getRemoteBasePath();
    
    /**
     * Determines if this remote directory can handle the given file
     * @param fileName the name of the file to check
     * @return true if this remote directory accepts the file, false otherwise
     */
    boolean acceptsFile(String fileName);
    
    /**
     * Performs format-specific remote initialization
     */
    void initialize() throws IOException;
    
    /**
     * Performs format-specific remote cleanup
     */
    void cleanup() throws IOException;
    
    /**
     * Copies a file from local directory to remote storage (async version with multi-stream support)
     * This mirrors RemoteDirectory.copyFrom() method signature exactly
     * 
     * @param from local directory to copy from
     * @param src source file name in local directory
     * @param remoteFileName destination file name in remote storage
     * @param context IO context for the operation
     * @param postUploadRunner callback to run after successful upload
     * @param listener action listener for async completion
     * @param lowPriorityUpload whether this is a low priority upload
     * @return true if async upload was started, false if caller should use sync copyFrom
     */
    boolean copyFrom(
        Directory from,
        String src,
        String remoteFileName,
        IOContext context,
        Runnable postUploadRunner,
        ActionListener<Void> listener,
        boolean lowPriorityUpload
    );
    
    /**
     * Copies a file from local directory to remote storage (sync version)
     * This mirrors Directory.copyFrom() method pattern
     * 
     * @param from local directory to copy from
     * @param src source file name
     * @param dest destination file name in remote storage
     * @param context IO context for the operation
     * @throws IOException if copy operation fails
     */
    void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException;
    
    /**
     * Creates output for writing to a remote file
     * @param fileName name of the file to create
     * @param context IO context for the operation
     * @return IndexOutput for writing to the remote file
     * @throws IOException if output cannot be created
     */
    IndexOutput createOutput(String fileName, IOContext context) throws IOException;
    
    /**
     * Opens a remote file for reading
     * @param fileName name of the file to open
     * @param context IO context for the operation
     * @return IndexInput for reading the remote file
     * @throws IOException if file cannot be opened
     */
    IndexInput openInput(String fileName, IOContext context) throws IOException;
    
    /**
     * Opens a remote file for reading with specified length
     * @param fileName name of the file to open
     * @param fileLength length of the file
     * @param context IO context for the operation
     * @return IndexInput for reading the remote file
     * @throws IOException if file cannot be opened
     */
    IndexInput openInput(String fileName, long fileLength, IOContext context) throws IOException;
    
    /**
     * Opens a block from a remote file for reading
     * @param fileName name of the file
     * @param position starting position of the block
     * @param length length of the block
     * @param fileLength total file length
     * @param context IO context for the operation
     * @return IndexInput for reading the block
     * @throws IOException if block cannot be opened
     */
    IndexInput openBlockInput(String fileName, long position, long length, long fileLength, IOContext context) throws IOException;
    
    /**
     * Deletes a file from remote storage
     * @param fileName name of the file to delete
     * @throws IOException if deletion fails
     */
    void deleteFile(String fileName) throws IOException;
    
    /**
     * Lists all files in the remote directory
     * @return array of file names
     * @throws IOException if listing fails
     */
    String[] listAll() throws IOException;
    
    /**
     * Lists files with given prefix
     * @param filenamePrefix prefix to filter by
     * @return collection of matching file names
     * @throws IOException if listing fails
     */
    Collection<String> listFilesByPrefix(String filenamePrefix) throws IOException;
    
    /**
     * Returns the length of a file in remote storage
     * @param fileName name of the file
     * @return file length in bytes
     * @throws IOException if file length cannot be determined
     */
    long fileLength(String fileName) throws IOException;
    
    /**
     * Checks if a file exists in remote storage
     * @param fileName name of the file to check
     * @return true if file exists, false otherwise
     * @throws IOException if existence check fails
     */
    boolean fileExists(String fileName) throws IOException;
    
    /**
     * Deletes the entire remote directory
     * @throws IOException if deletion fails
     */
    void delete() throws IOException;
}
