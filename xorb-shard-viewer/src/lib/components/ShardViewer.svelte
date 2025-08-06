<script lang="ts">
  import type { ShardData } from "../types.js";
  import { formatBytes, formatHash, formatTimestamp } from "../parsers.js";

  export let data: ShardData;
  export let filename: string;
  export let fileSize: number;

  $: header = data.header;
  $: footer = data.footer;
  $: totalFiles = data.file_info.length;
  $: totalXorbs = data.cas_info.length;
  $: totalChunks = data.cas_info.reduce(
    (sum, cas) => sum + cas.header.num_entries,
    0
  );

  // Check verification and metadata status across all files
  $: {
    const filesWithVerification = data.file_info.filter(
      (fileInfo) => fileInfo.header.file_flags & 0x80000000
    ).length;
    const filesWithMetadata = data.file_info.filter(
      (fileInfo) => fileInfo.header.file_flags & 0x40000000
    ).length;

    verificationStatus =
      filesWithVerification === totalFiles
        ? "✅"
        : filesWithVerification === 0
          ? "❌"
          : "❓";

    metadataStatus =
      filesWithMetadata === totalFiles
        ? "✅"
        : filesWithMetadata === 0
          ? "❌"
          : "❓";
  }

  let verificationStatus = "";
  let metadataStatus = "";
  $: totalStoredBytes = data.cas_info.reduce(
    (sum, cas) => sum + cas.header.num_bytes_in_cas,
    0
  );

  // Track which files are expanded
  let expandedFiles = new Set<number>();

  function toggleFileExpansion(fileIndex: number) {
    if (expandedFiles.has(fileIndex)) {
      expandedFiles.delete(fileIndex);
    } else {
      expandedFiles.add(fileIndex);
    }
    expandedFiles = expandedFiles; // Trigger reactivity
  }
</script>

<div class="shard-viewer">
  <div class="header">
    <h2>🗂️ Shard File Analysis</h2>
    <div class="file-info">
      <span class="filename">{filename}</span>
      <span class="file-size">{formatBytes(fileSize)}</span>
    </div>
  </div>

  <div class="metadata-grid">
    <div class="metadata-section">
      <h3>Header Information</h3>
      <div class="metadata-item">
        <span class="label">Version:</span>
        <span class="value">{header.version}</span>
      </div>
      <div class="metadata-item">
        <span class="label">Footer Size:</span>
        <span class="value">{formatBytes(header.footer_size)}</span>
      </div>
    </div>

    <div class="metadata-section">
      <h3>Content Statistics</h3>
      <div class="metadata-item">
        <span class="label">Files:</span>
        <span class="value">{totalFiles.toLocaleString()}</span>
      </div>
      <div class="metadata-item">
        <span class="label">XORBs:</span>
        <span class="value">{totalXorbs.toLocaleString()}</span>
      </div>
      <div class="metadata-item">
        <span class="label">Total Chunks:</span>
        <span class="value">{totalChunks.toLocaleString()}</span>
      </div>
      <div class="metadata-item">
        <span class="label">Stored Bytes:</span>
        <span class="value">{formatBytes(totalStoredBytes)}</span>
      </div>
    </div>

    <div class="metadata-section">
      <h3>Timestamps & Security</h3>
      <div class="metadata-item">
        <span class="label">Created:</span>
        <span class="value"
          >{formatTimestamp(footer.shard_creation_timestamp)}</span
        >
      </div>
      <div class="metadata-item">
        <span class="label">Key Expiry:</span>
        <span class="value">{formatTimestamp(footer.shard_key_expiry)}</span>
      </div>
      <div class="metadata-item">
        <span class="label">HMAC Key:</span>
        <span class="value hash" title={formatHash(footer.chunk_hash_hmac_key)}>
          {formatHash(footer.chunk_hash_hmac_key)}
        </span>
      </div>
    </div>
  </div>

  {#if data.file_info.length > 0}
    <div class="file-details">
      <h3>
        File Information ({data.file_info.length} files) - Verification: {verificationStatus}
        Metadata: {metadataStatus}
      </h3>
      <div class="table-container">
        <table class="data-table">
          <thead>
            <tr>
              <th>File Hash</th>
              <th>Entries</th>
              <th>Total Length</th>
              <th>SHA256</th>
            </tr>
          </thead>
          <tbody>
            {#each data.file_info as fileInfo, fileIndex}
              <tr
                class="file-row"
                class:expanded={expandedFiles.has(fileIndex)}
                on:click={() => toggleFileExpansion(fileIndex)}
                role="button"
                tabindex="0"
                on:keydown={(e) =>
                  e.key === "Enter" && toggleFileExpansion(fileIndex)}
              >
                <td class="hash" title={formatHash(fileInfo.header.file_hash)}>
                  <span class="expand-icon">
                    {expandedFiles.has(fileIndex) ? "▼" : "▶"}
                  </span>
                  {formatHash(fileInfo.header.file_hash)}
                </td>
                <td>{fileInfo.header.num_entries}</td>
                <td>
                  {formatBytes(
                    fileInfo.entries.reduce(
                      (sum, entry) => sum + entry.unpacked_segment_bytes,
                      0
                    )
                  )}
                </td>
                <td>
                  {#if fileInfo.metadata_ext}
                    <span
                      class="hash"
                      title={formatHash(fileInfo.metadata_ext.sha256)}
                    >
                      {formatHash(fileInfo.metadata_ext.sha256)}
                    </span>
                  {:else}
                    <span class="no-data">—</span>
                  {/if}
                </td>
              </tr>
              {#if expandedFiles.has(fileIndex)}
                <tr class="file-details-row">
                  <td colspan="4">
                    <div class="file-entries-container">
                      <div class="entries-table-container">
                        <h4>Data Entries for File</h4>
                        <table class="entries-table">
                          <thead>
                            <tr>
                              <th>Entry #</th>
                              <th>CAS Hash</th>
                              <th>Chunk Range</th>
                              <th>Unpacked Size</th>
                            </tr>
                          </thead>
                          <tbody>
                            {#each fileInfo.entries as entry, entryIndex}
                              <tr>
                                <td>{entryIndex + 1}</td>
                                <td
                                  class="hash"
                                  title={formatHash(entry.cas_hash)}
                                >
                                  {formatHash(entry.cas_hash)}
                                </td>
                                <td
                                  >{entry.chunk_index_start} - {entry.chunk_index_end}</td
                                >
                                <td
                                  >{formatBytes(
                                    entry.unpacked_segment_bytes
                                  )}</td
                                >
                              </tr>
                            {/each}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </td>
                </tr>
              {/if}
            {/each}
          </tbody>
        </table>
      </div>
    </div>
  {/if}

  {#if data.cas_info.length > 0}
    <div class="cas-details">
      <h3>CAS Information ({data.cas_info.length} XORBs)</h3>
      <div class="table-container">
        <table class="data-table">
          <thead>
            <tr>
              <th>CAS Hash</th>
              <th>Chunks</th>
              <th>Bytes in CAS</th>
              <th>Bytes on Disk</th>
              <th>Compression</th>
            </tr>
          </thead>
          <tbody>
            {#each data.cas_info as casInfo}
              <tr>
                <td class="hash" title={formatHash(casInfo.header.cas_hash)}>
                  {formatHash(casInfo.header.cas_hash)}
                </td>
                <td>{casInfo.header.num_entries}</td>
                <td>{formatBytes(casInfo.header.num_bytes_in_cas)}</td>
                <td>{formatBytes(casInfo.header.num_bytes_on_disk)}</td>
                <td>
                  {casInfo.header.num_bytes_in_cas > 0
                    ? (
                        (casInfo.header.num_bytes_on_disk /
                          casInfo.header.num_bytes_in_cas) *
                        100
                      ).toFixed(1) + "%"
                    : "N/A"}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </div>
  {/if}
</div>

<style>
  .shard-viewer {
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px;
  }

  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 30px;
    padding-bottom: 20px;
    border-bottom: 2px solid #eee;
  }

  .header h2 {
    margin: 0;
    color: #333;
    font-size: 24px;
  }

  .file-info {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 4px;
  }

  .filename {
    font-weight: 600;
    color: #555;
  }

  .file-size {
    font-size: 14px;
    color: #888;
  }

  .metadata-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 20px;
    margin-bottom: 30px;
  }

  .metadata-section {
    background: #f8f9fa;
    padding: 20px;
    border-radius: 8px;
    border: 1px solid #e9ecef;
  }

  .metadata-section h3 {
    margin: 0 0 15px 0;
    color: #495057;
    font-size: 18px;
    font-weight: 600;
  }

  .metadata-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
    padding: 8px 0;
    border-bottom: 1px solid #e9ecef;
  }

  .metadata-item:last-child {
    border-bottom: none;
    margin-bottom: 0;
  }

  .metadata-item .label {
    font-weight: 500;
    color: #6c757d;
    margin-right: 10px;
  }

  .metadata-item .value {
    font-family: "Monaco", "Consolas", monospace;
    font-size: 14px;
    color: #212529;
    text-align: right;
  }

  .hash {
    font-family: "Monaco", "Consolas", monospace;
    background: #e9ecef;
    padding: 2px 6px;
    border-radius: 4px;
    cursor: help;
    font-size: 12px;
  }

  .file-details,
  .cas-details {
    margin-top: 30px;
  }

  .file-details h3,
  .cas-details h3 {
    margin: 0 0 20px 0;
    color: #495057;
    font-size: 18px;
    font-weight: 600;
  }

  .table-container {
    overflow-x: auto;
    border-radius: 8px;
    border: 1px solid #e9ecef;
  }

  .data-table {
    width: 100%;
    border-collapse: collapse;
    background: white;
  }

  .data-table th,
  .data-table td {
    padding: 12px;
    text-align: left;
    border-bottom: 1px solid #e9ecef;
  }

  .data-table th {
    background: #f8f9fa;
    font-weight: 600;
    color: #495057;
  }

  .data-table tr:hover {
    background: #f8f9fa;
  }

  .no-data {
    color: #999;
    font-style: italic;
  }

  /* Expandable file rows */
  .file-row {
    cursor: pointer;
    transition: background-color 0.2s ease;
  }

  .file-row:hover {
    background-color: #f0f7ff !important;
  }

  .file-row.expanded {
    background-color: #e3f2fd;
  }

  .expand-icon {
    display: inline-block;
    margin-right: 8px;
    font-size: 12px;
    width: 12px;
    transition: transform 0.2s ease;
  }

  .file-details-row {
    background-color: #f8f9fa;
  }

  .file-details-row td {
    padding: 0;
  }

  .file-entries-container {
    padding: 16px;
    border-left: 3px solid #007bff;
    margin-left: 12px;
    max-height: 90vh;
    overflow-y: auto;
    overflow-x: hidden;
    border-radius: 6px;
    background-color: #f8f9fa;
  }

  /* Scrollbar styling for webkit browsers */
  .file-entries-container::-webkit-scrollbar {
    width: 6px;
  }

  .file-entries-container::-webkit-scrollbar-track {
    background: #f1f1f1;
    border-radius: 3px;
  }

  .file-entries-container::-webkit-scrollbar-thumb {
    background: #c1c1c1;
    border-radius: 3px;
  }

  .file-entries-container::-webkit-scrollbar-thumb:hover {
    background: #a8a8a8;
  }

  .file-entries-container h4 {
    margin: 0 0 12px 0;
    color: #495057;
    font-size: 14px;
    font-weight: 600;
    position: sticky;
    top: 0;
    background-color: #f8f9fa;
    padding: 8px 0;
    z-index: 1;
  }

  .entries-table-container {
    overflow-x: auto;
    border-radius: 6px;
    border: 1px solid #dee2e6;
  }

  .entries-table {
    width: 100%;
    border-collapse: collapse;
    background: white;
    font-size: 13px;
  }

  .entries-table th,
  .entries-table td {
    padding: 8px 12px;
    text-align: left;
    border-bottom: 1px solid #dee2e6;
  }

  .entries-table th {
    background: #f1f3f4;
    font-weight: 600;
    color: #495057;
    font-size: 12px;
  }

  .entries-table tr:hover {
    background: #f8f9fa;
  }

  .entries-table .hash {
    font-family: "Monaco", "Menlo", "Ubuntu Mono", monospace;
    font-size: 11px;
    color: #6c757d;
  }

  @media (max-width: 768px) {
    .header {
      flex-direction: column;
      align-items: flex-start;
      gap: 10px;
    }

    .metadata-grid {
      grid-template-columns: 1fr;
    }

    .metadata-item {
      flex-direction: column;
      align-items: flex-start;
      gap: 4px;
    }

    .metadata-item .value {
      text-align: left;
    }
  }
</style>
